/**
 * Copyright 2017-2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.hive.hive2rel.parsetree;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.linkedin.coral.common.calcite.CalciteUtil;
import com.linkedin.coral.common.calcite.sql.*;
import com.linkedin.coral.common.calcite.sql.ddl.*;
import com.linkedin.coral.common.calcite.sql.ddl.SqlAlterTable;
import com.linkedin.coral.common.calcite.sql.ddl.SqlAlterTable.AlterTableOperation;
import com.linkedin.coral.common.calcite.sql.ddl.alter.*;
import com.linkedin.coral.common.calcite.sql.ddl.constraint.SqlColumnConstraint;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.metastore.api.Table;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.linkedin.coral.common.functions.CoralSqlUnnestOperator;
import com.linkedin.coral.common.functions.Function;
import com.linkedin.coral.common.functions.FunctionFieldReferenceOperator;
import com.linkedin.coral.hive.hive2rel.functions.HiveFunctionResolver;
import com.linkedin.coral.hive.hive2rel.functions.HiveJsonTupleOperator;
import com.linkedin.coral.hive.hive2rel.functions.HiveRLikeOperator;
import com.linkedin.coral.hive.hive2rel.functions.StaticHiveFunctionRegistry;
import com.linkedin.coral.hive.hive2rel.functions.VersionedSqlUserDefinedFunction;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.ASTNode;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.CoralParseDriver;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.HiveParser;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.Node;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.ParseDriver;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.ParseException;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.*;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;


//这个类主要的作用就是把hive 的ast 转换为 calcite 的sqlnode
/**
 * Class to convert Hive Abstract Syntax Tree(AST) represented by {@link ASTNode} to
 * Calcite based AST represented using {@link SqlNode}.
 *
 * Hive AST nodes do not support polymorphic behavior for processing AST using, for example, visitors
 * ASTNode carries all the information as type and text fields and children nodes are of base class Node.
 * This requires constant casting of nodes and string processing to get the type and value out of a node.
 * This complicates analysis of the tree.
 *
 * This class converts the AST to Calcite based AST using {@link SqlNode}.This is more structured
 * allowing for simpler analysis code.
 *
 * NOTE:
 * There are certain difficulties in correct translation.
 * For example, for identifier named {@code s.field1} it's hard to ascertain if {@code s} is a
 * table name or column name of type struct. This is typically resolved by validators using scope but
 * we haven't specialized that part yet.
 */

public class ParseTreeBuilder extends AbstractASTVisitor<SqlNode, ParseTreeBuilder.ParseContext> {
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final HiveFunctionResolver functionResolver;

  /**
   * Constructs a parse tree builder
   */
  public ParseTreeBuilder(HiveFunctionResolver functionResolver) {
    this.functionResolver = functionResolver;
  }

  /**
   * Creates a parse tree for input sql. The input SQL must NOT contain any Dali function names.
   * It is okay for the sql to refer to dali views that use dali functions.
   * @param sql sql statement to convert to parse tree
   * @return Calcite SqlNode representing parse tree that calcite framework can understand
   */
  public SqlNode processSql(String sql) {
    return process(sql, null);
  }

  /**
   * Returns true if the view is created using spark sql. This relies on the presence of the
   * spark.sql.create.version property in the views when created using spark sql.
   *
   * @param hiveView
   * @return true if the view is created using spark sql
   */
  private static boolean isCreatedUsingSpark(Table hiveView) {
    Map<String, String> tableParams = hiveView.getParameters();
    return tableParams != null && tableParams.containsKey("spark.sql.create.version");
  }

  public SqlNode process(String sql, @Nullable Table hiveView) {
    ParseDriver pd = new CoralParseDriver(hiveView != null && isCreatedUsingSpark(hiveView));
    try {
      ASTNode root = pd.parse(sql);
      return processAST(root, hiveView);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private SqlNode processAST(ASTNode node, @Nullable Table hiveView) {
    ParseContext ctx = new ParseContext(hiveView);
    return visit(node, ctx);
  }


  /*
   * visit 方法实现了 ASTNode 到 SqlNode 的转换逻辑，
   * visitChildren 不会转换，具体落到叶子接点上，然后非叶子节点包含children即可
   *
   *
   * */

  @Override
  protected SqlNode visitTabAlias(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);
    checkState(sqlNodes.size() == 1);
    return sqlNodes.get(0);
  }

  @Override
  protected SqlNode visitLateralView(ASTNode node, ParseContext ctx) {
    return visitLateralViewInternal(node, ctx, false);
  }

  @Override
  protected SqlNode visitLateralViewOuter(ASTNode node, ParseContext ctx) {
    return visitLateralViewInternal(node, ctx, true);
  }

  // lateral views are turned to:
  // "...FROM table, lateral (select col FROM UNNEST(table.col) as t(col))" query
  // For lateral outer, we need to replace UNNEST with
  //    UNNEST(if(table.col is null or cadinality(table.col) == 0, [null], table.col))
  private SqlNode visitLateralViewInternal(ASTNode node, ParseContext ctx, boolean isOuter) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);
    checkState(sqlNodes.size() == 2 && sqlNodes.get(0) instanceof SqlNodeList);

    // rightNode is AS.createCall(unnest, t, col)
    SqlNode rightNode = Iterables.getOnlyElement((SqlNodeList) sqlNodes.get(0));
    if (!(rightNode instanceof SqlCall) || !(((SqlCall) rightNode).getOperator() instanceof SqlAsOperator)) {
      throw new UnsupportedOperationException(format("Unsupported LATERAL VIEW without AS: %s", rightNode));
    }
    SqlCall aliasCall = (SqlCall) rightNode;
    List<SqlNode> aliasOperands = aliasCall.getOperandList();
    checkState(aliasOperands.get(0) instanceof SqlCall);
    SqlCall tableFunctionCall = (SqlCall) aliasOperands.get(0);

    if (tableFunctionCall.getOperator() instanceof CoralSqlUnnestOperator) {
      return visitLateralViewExplode(sqlNodes, aliasOperands, tableFunctionCall, isOuter);
    }

    if (tableFunctionCall.getOperator() instanceof HiveJsonTupleOperator) {
      return visitLateralViewJsonTuple(sqlNodes, aliasOperands, tableFunctionCall);
    }

    if (tableFunctionCall.getOperator() instanceof VersionedSqlUserDefinedFunction) {
      return visitLateralViewUDTF(sqlNodes, aliasOperands, tableFunctionCall);
    }

    throw new UnsupportedOperationException(format("Unsupported LATERAL VIEW operator: %s", tableFunctionCall));
  }

  /**
   * For generic UDTFs, we treat them as LinkedIn UDFs and make the following conversion:
   *
   * SELECT a, t.col1
   * FROM test.tableOne
   * LATERAL VIEW `com.linkedin.coral.hive.hive2rel.CoralTestUDTF`(`tableone`.`a`) `t`
   * ->
   * SELECT a, t.col1
   * FROM test.tableOne
   * LATERAL COLLECTION_TABLE(`com.linkedin.coral.hive.hive2rel.CoralTestUDTF`(`tableone`.`a`)) AS `t` (`col1`)
   *
   * therefore, we need to get the return field names (`col1` in the above example) of the UDTF from
   * `StaticHiveFunctionRegistry.UDTF_RETURN_FIELD_NAME_MAP`.
   */
  private SqlNode visitLateralViewUDTF(List<SqlNode> sqlNodes, List<SqlNode> aliasOperands, SqlCall tableFunctionCall) {
    SqlNode lateralCall = SqlStdOperatorTable.LATERAL.createCall(ZERO,
        new SqlLateralOperator(SqlKind.COLLECTION_TABLE).createCall(ZERO, tableFunctionCall));
    final String functionName = tableFunctionCall.getOperator().getName();
    ImmutableList<String> fieldNames =
        StaticHiveFunctionRegistry.UDTF_RETURN_FIELD_NAME_MAP.getOrDefault(functionName, null);
    if (fieldNames == null) {
      throw new RuntimeException("User defined table function " + functionName + " is not registered.");
    }
    List<SqlNode> asOperands = new ArrayList<>();
    asOperands.add(lateralCall);
    asOperands.add(aliasOperands.get(1));
    fieldNames.forEach(name -> asOperands.add(new SqlIdentifier(name, ZERO)));
    SqlCall aliasCall = SqlStdOperatorTable.AS.createCall(ZERO, asOperands);
    return new SqlJoin(ZERO, sqlNodes.get(1), SqlLiteral.createBoolean(false, ZERO), JoinType.COMMA.symbol(ZERO),
        aliasCall/*lateralCall*/, JoinConditionType.NONE.symbol(ZERO), null);
  }

  private SqlNode visitLateralViewExplode(List<SqlNode> sqlNodes, List<SqlNode> aliasOperands,
      SqlCall tableFunctionCall, boolean isOuter) {
    final int operandCount = aliasOperands.size();
    // explode array if operandCount == 3: LATERAL VIEW EXPLODE(op0) op1 AS op2
    // explode map if operandCount == 4: LATERAL VIEW EXPLODE(op0) op1 AS op2, op3
    // posexplode array if operandCount == 4: LATERAL VIEW POSEXPLODE(op0) op1 AS op2, op3
    // if operandCount == 2: `LATERAL VIEW EXPLODE(op0) op1` or `LATERAL VIEW POSEXPLODE(op0) op1`
    //   if op0 is a map, it implies `AS key, value` where key, value are auto-named by Hive
    //   if op0 is an array, it implies `AS col` for `explode` or `AS ORDINALITY, col` for `posexplode`, which are auto-generated by Hive
    //   The logic above will be implemented as part of Calcite SqlNode validation
    //   Note that `operandCount == 2 && isOuter` is not supported yet due to the lack of type information needed
    //   to derive the correct IF function parameters.
    checkState(operandCount == 2 || operandCount == 3 || operandCount == 4,
        format("Unsupported LATERAL VIEW EXPLODE operand number: %d", operandCount));
    // TODO The code below assumes LATERAL VIEW is used with UNNEST EXPLODE/POSEXPLODE only. It should be made more generic.
    SqlCall unnestCall = tableFunctionCall;
    SqlNode unnestOperand = unnestCall.operand(0);
    final SqlOperator operator = unnestCall.getOperator();

    if (isOuter) {
      checkState(operandCount > 2,
          "LATERAL VIEW OUTER EXPLODE without column aliases is not supported. Add 'AS col' or 'AS key, value' to fix it");
      // transforms unnest(b) to unnest( if(b is null or cardinality(b) = 0, ARRAY(null)/MAP(null, null), b))
      SqlNode operandIsNull = SqlStdOperatorTable.IS_NOT_NULL.createCall(ZERO, unnestOperand);
      SqlNode emptyArray = SqlStdOperatorTable.GREATER_THAN.createCall(ZERO,
          SqlStdOperatorTable.CARDINALITY.createCall(ZERO, unnestOperand), SqlLiteral.createExactNumeric("0", ZERO));
      SqlNode ifCondition = SqlStdOperatorTable.AND.createCall(ZERO, operandIsNull, emptyArray);
      // array of [null] or map of (null, null) should be 3rd param to if function. With our type inference, calcite acts
      // smart and for unnest(array[null]) or unnest(map(null, null)) determines return type to be null
      SqlNode arrayOrMapOfNull;
      if (operandCount == 3
          || (operator instanceof CoralSqlUnnestOperator && ((CoralSqlUnnestOperator) operator).withOrdinality)) {
        arrayOrMapOfNull = SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(ZERO, SqlLiteral.createNull(ZERO));
      } else {
        arrayOrMapOfNull = SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR.createCall(ZERO, SqlLiteral.createNull(ZERO),
            SqlLiteral.createNull(ZERO));
      }
      Function hiveIfFunction = functionResolver.tryResolve("if", null, 1);
      unnestOperand = hiveIfFunction.createCall(SqlLiteral.createCharString("if", ZERO),
          ImmutableList.of(ifCondition, unnestOperand, arrayOrMapOfNull), null);
    }
    unnestCall = operator.createCall(ZERO, unnestOperand);

    SqlNode lateralCall = SqlStdOperatorTable.LATERAL.createCall(ZERO, unnestCall);

    // The following code can work in both of the two cases:
    // A. Table alias only, no column aliases.
    // B. Both table and column aliases.  Note that in this case, the number of column aliases need to match the
    //    actual number of columns generated from the EXPLODE function, which is calculated by HiveUncollect.deriveRowType
    /** See also {@link HiveUncollect#deriveRowType()} */
    List<SqlNode> asOperands = new ArrayList<>();
    asOperands.add(lateralCall);

    // For POSEXPLODE case, we need to change the order of 2 alias. i.e. `pos, val` -> `val, pos` to be aligned with calcite validation
    if (operator instanceof CoralSqlUnnestOperator && ((CoralSqlUnnestOperator) operator).withOrdinality
        && operandCount == 4) {
      asOperands.add(aliasOperands.get(1));
      asOperands.add(aliasOperands.get(3));
      asOperands.add(aliasOperands.get(2));
    } else {
      asOperands.addAll(aliasOperands.subList(1, aliasOperands.size()));
    }
    SqlNode as = SqlStdOperatorTable.AS.createCall(ZERO, asOperands);

    return new SqlJoin(ZERO, sqlNodes.get(1), SqlLiteral.createBoolean(false, ZERO), JoinType.COMMA.symbol(ZERO), as,
        JoinConditionType.NONE.symbol(ZERO), null);
  }

  private SqlNode visitLateralViewJsonTuple(List<SqlNode> sqlNodes, List<SqlNode> aliasOperands, SqlCall sqlCall) {
    /*
     Represent
        LATERAL VIEW json_tuple(json, p1, p2) jt AS a, b
     as
        LATERAL (
          SELECT
            IF(p1 is supported JSON key, get_json_object(json, '$["${p1}"]'), NULL) a,
            IF(p2 is supported JSON key, get_json_object(json, '$["${p1}"]'), NULL) b
        ) AS jt(a, b)
     TODO the relation alias `jt` is being lost by downstream transformations
     */

    Function getJsonObjectFunction = functionResolver.tryResolve("get_json_object", null, 2);
    Function ifFunction = functionResolver.tryResolve("if", null, 3);

    List<SqlNode> jsonTupleOperands = sqlCall.getOperandList();
    SqlNode jsonInput = jsonTupleOperands.get(0);

    List<SqlNode> projections = new ArrayList<>();
    for (int jsonKeyPosition = 0; jsonKeyPosition < jsonTupleOperands.size() - 1; jsonKeyPosition++) {
      SqlNode jsonKey = jsonTupleOperands.get(1 + jsonKeyPosition);
      SqlNode keyAlias = aliasOperands.get(2 + jsonKeyPosition);

      // '$["jsonKey"]'
      SqlCall jsonPath = SqlStdOperatorTable.CONCAT.createCall(ZERO,
          SqlStdOperatorTable.CONCAT.createCall(ZERO, SqlLiteral.createCharString("$[\"", ZERO), jsonKey),
          SqlLiteral.createCharString("\"]", ZERO));

      SqlCall getJsonObjectCall =
          getJsonObjectFunction.createCall(SqlLiteral.createCharString(getJsonObjectFunction.getFunctionName(), ZERO),
              ImmutableList.of(jsonInput, jsonPath), null);
      // TODO Hive get_json_object returns a string, but currently is mapped in Trino to json_extract which returns a json. Once fixed, remove the CAST
      SqlCall castToString = SqlStdOperatorTable.CAST.createCall(ZERO, getJsonObjectCall,
          // TODO This results in CAST to VARCHAR(65535), which may be too short, but there seems to be no way to avoid that.
          //  even `new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, Integer.MAX_VALUE - 1, ZERO), ZERO)` results in a limited VARCHAR precision.
          createBasicTypeSpec(SqlTypeName.VARCHAR));
      // TODO support jsonKey containing a quotation mark (") or backslash (\)
      SqlCall ifCondition =
          HiveRLikeOperator.RLIKE.createCall(ZERO, jsonKey, SqlLiteral.createCharString("^[^\\\"]*$", ZERO));
      SqlCall ifFunctionCall = ifFunction.createCall(SqlLiteral.createCharString(ifFunction.getFunctionName(), ZERO),
          ImmutableList.of(ifCondition, castToString, SqlLiteral.createNull(ZERO)), null);
      SqlNode projection = ifFunctionCall;
      // Currently only explicit aliasing is supported. Implicit alias would be c0, c1, etc.
      projections.add(SqlStdOperatorTable.AS.createCall(ZERO, projection, keyAlias));
    }

    SqlNode select =
        new SqlSelect(ZERO, null, new SqlNodeList(projections, ZERO), null, null, null, null, null, null, null, null, null);
    SqlNode lateral = SqlStdOperatorTable.LATERAL.createCall(ZERO, select);
    SqlCall lateralAlias = SqlStdOperatorTable.AS.createCall(ZERO,
        ImmutableList.<SqlNode> builder().add(lateral).addAll(aliasOperands.subList(1, aliasOperands.size())).build());
    SqlNode joinNode = new SqlJoin(ZERO, sqlNodes.get(1), SqlLiteral.createBoolean(false, ZERO),
        JoinType.COMMA.symbol(ZERO), lateralAlias, JoinConditionType.NONE.symbol(ZERO), null);
    return joinNode;
  }

  @Override
  protected SqlNode visitLeftSemiJoin(ASTNode node, ParseContext ctx) {
    throw new RuntimeException(String.format("%s, %s, %s", node.getType(), node.getText(), node.dump()));
  }

  @Override
  protected SqlNode visitCrossJoin(ASTNode node, ParseContext ctx) {
    throw new RuntimeException(String.format("%s, %s, %s", node.getType(), node.getText(), node.dump()));
  }

  @Override
  protected SqlNode visitFullOuterJoin(ASTNode node, ParseContext ctx) {
    return processJoin(node, ctx, JoinType.FULL);
  }

  @Override
  protected SqlNode visitRightOuterJoin(ASTNode node, ParseContext ctx) {
    return processJoin(node, ctx, JoinType.RIGHT);
  }

  @Override
  protected SqlNode visitJoin(ASTNode node, ParseContext ctx) {
    return processJoin(node, ctx, JoinType.INNER);
  }

  @Override
  protected SqlNode visitLeftOuterJoin(ASTNode node, ParseContext ctx) {
    return processJoin(node, ctx, JoinType.LEFT);
  }

  private SqlNode processJoin(ASTNode node, ParseContext ctx, JoinType joinType) {
    List<SqlNode> children = visitChildren(node, ctx);
    checkState(children.size() == 2 || children.size() == 3);
    JoinConditionType conditionType;
    SqlNode condition = null;
    if (children.size() == 2) {
      conditionType = JoinConditionType.NONE;
    } else {
      conditionType = JoinConditionType.ON;
      condition = children.get(2);
    }

    return new SqlJoin(ZERO, children.get(0), SqlLiteral.createBoolean(false, ZERO), joinType.symbol(ZERO),
        children.get(1), conditionType.symbol(ZERO), condition);
  }

  @Override
  protected SqlNode visitFalse(ASTNode node, ParseContext ctx) {
    return SqlLiteral.createBoolean(false, ZERO);
  }

  @Override
  protected SqlNode visitTrue(ASTNode node, ParseContext ctx) {
    return SqlLiteral.createBoolean(true, ZERO);
  }

  @Override
  protected SqlNode visitNullToken(ASTNode node, ParseContext ctx) {
    return SqlLiteral.createNull(ZERO);
  }

  @Override
  protected SqlNode visitLimit(ASTNode node, ParseContext ctx) {
    ctx.fetch = visitChildren(node, ctx).get(0);
    return ctx.fetch;
  }

  @Override
  protected SqlNode visitUnion(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);

    // We use Hive 1.1 in which UNION defaults to UNION_ALL
    return new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, sqlNodes.toArray(new SqlNode[0]), ZERO);
  }

  @Override
  protected SqlNode visitNumber(ASTNode node, ParseContext ctx) {
    String strval = node.getText();
    return SqlLiteral.createExactNumeric(strval, ZERO);
  }

  @Override
  protected SqlNode visitAllColRef(ASTNode node, ParseContext ctx) {
    List<SqlNode> children = visitChildren(node, ctx);
    // This is to allow t.*
    // In Hive ASTNode Tree, "t.*" has the following shape
    // TOK_ALLCOLREF
    // - TOK_TABLE_OR_COL
    //   - "t"
    List<String> names = new ArrayList<>();
    if (children != null) {
      for (SqlNode child : children) {
        names.addAll(((SqlIdentifier) child).names);
      }
    }
    names.add("*");
    List<SqlParserPos> sqlParserPos = Collections.nCopies(names.size(), ZERO);
    SqlNode star = SqlIdentifier.star(names, ZERO, sqlParserPos);
    return star;
  }

  @Override
  protected SqlNode visitHaving(ASTNode node, ParseContext ctx) {
    checkState(node.getChildren().size() == 1);
    ctx.having = visit((ASTNode) node.getChildren().get(0), ctx);
    return ctx.having;
  }

  @Override
  protected SqlNode visitWhere(ASTNode node, ParseContext ctx) {
    checkState(node.getChildren().size() == 1);
    ctx.where = visit((ASTNode) node.getChildren().get(0), ctx);
    return ctx.where;
  }

  @Override
  protected SqlNode visitSortColNameDesc(ASTNode node, ParseContext ctx) {
    return visitSortColName(node, ctx, true);
  }

  @Override
  protected SqlNode visitSortColNameAsc(ASTNode node, ParseContext ctx) {
    return visitSortColName(node, ctx, false);
  }

  private SqlNode visitSortColName(ASTNode node, ParseContext ctx, boolean descending) {
    List<SqlNode> children = visitChildren(node, ctx);
    checkState(children.size() == 1);
    if (!descending) {
      return children.get(0);
    }
    return new SqlBasicCall(SqlStdOperatorTable.DESC, new SqlNode[] { children.get(0) }, ZERO);
  }

  @Override
  protected SqlNode visitOrderBy(ASTNode node, ParseContext ctx) {
    List<SqlNode> orderByCols = visitChildren(node, ctx);
    ctx.orderBy = new SqlNodeList(orderByCols, ZERO);
    return ctx.orderBy;
  }


  @Override
  protected SqlNode visitNullsFirst(ASTNode node, ParseContext ctx) {
    List<SqlNode> orderByCols = visitChildren(node, ctx);
    return SqlStdOperatorTable.NULLS_FIRST.createCall(ZERO, orderByCols);
  }

  @Override
  protected SqlNode visitNullsLast(ASTNode node, ParseContext ctx) {
    List<SqlNode> orderByCols = visitChildren(node, ctx);
    return NULLS_LAST.createCall(ZERO, orderByCols);
  }


  //singe group by only one col
  @Override
  protected SqlNode visitGroupBy(ASTNode node, ParseContext ctx) {
    List<SqlNode> grpCols = visitChildren(node, ctx);
    ctx.grpBy = new SqlNodeList(grpCols, ZERO);
    return ctx.grpBy;
  }


  // todo hive 识别到grouping set 后，构造的 ast 中会略去 group by 节点， group by 会被认为是 grouping set的特殊情况，
  // group by  a,b 会构造多个group by ast 节点
  // 在visitGroupingSets 中很难在sqlnode 中去构造一个 groupby 类型sqlnode
  protected  SqlNode visitGroupingSets(ASTNode node,  ParseContext ctx){

    if(ctx.grpBy == null){
      ctx.grpBy = new SqlNodeList(new ArrayList<SqlNode>(), ZERO);
    }

    List<SqlNode> groupingSets = visitChildren(node, ctx);
    List<SqlNode> operands = new ArrayList<>();
    if (groupingSets.isEmpty()) {
      operands.add(CalciteUtil.createSqlNodeList(Collections.emptyList()));
    } else  {
      List<SqlNode> operand = groupingSets.stream()
              .filter(f -> !(f instanceof SqlIdentifier)) // 过滤掉 SqlIdentifier 类型的节点
              .collect(Collectors.toList());
      operands.add(new SqlNodeList(operand, ZERO));
    }
    SqlNode groupingSetsNode = GROUPING_SETS.createCall(ZERO, operands);
    ctx.grpBy.add(groupingSetsNode);

    return groupingSetsNode;
  }

  protected  SqlNode visitGroupingSetsExpression(ASTNode node, ParseContext ctx){
    List<SqlNode> identifiers = visitChildren(node, ctx);
    if (identifiers == null || identifiers.isEmpty()){
      return  CalciteUtil.createSqlNodeList(Collections.emptyList());
    }else{
      return ROW.createCall(ZERO, new SqlNodeList(identifiers, ZERO));
    }
  }

  //  SqlNodeList 是sqlnode 的子类
  protected  SqlNode visitCubeGroupBy(ASTNode node, ParseContext ctx){
    if(ctx.grpBy == null){
      ctx.grpBy = new SqlNodeList(new ArrayList<SqlNode>(), ZERO);
    }

    List<SqlNode> cubeGroupby = visitChildren(node, ctx);
    List<SqlNode> operands = new ArrayList<>();
    if (cubeGroupby.isEmpty()) {
      operands.add(CalciteUtil.createSqlNodeList(Collections.emptyList()));
    } else  {
      operands.add(new SqlNodeList(cubeGroupby, ZERO));
    }
    SqlNode cubeCall = CUBE.createCall(ZERO, operands);
    ctx.grpBy.add(cubeCall);

    return cubeCall;
  }

  protected  SqlNode visitRollUpGroupBy(ASTNode node, ParseContext ctx){
    if(ctx.grpBy == null){
      ctx.grpBy = new SqlNodeList(new ArrayList<SqlNode>(), ZERO);
    }

    List<SqlNode> rollupGroupby = visitChildren(node, ctx);
    List<SqlNode> operands = new ArrayList<>();
    if (rollupGroupby.isEmpty()) {
      operands.add(CalciteUtil.createSqlNodeList(Collections.emptyList()));
    } else  {
      operands.add(new SqlNodeList(rollupGroupby, ZERO));
    }
    SqlNode rollupCall = ROLLUP.createCall(ZERO, operands);
    ctx.grpBy.add(rollupCall);

    return rollupCall;
  }

  @Override
  protected SqlNode visitOperator(ASTNode node, ParseContext ctx) {
    ArrayList<Node> children = node.getChildren();
    if (children.size() == 1) {
      return visitUnaryOperator(node, ctx);
    } else if (children.size() == 2) {
      return visitBinaryOperator(node, ctx);
    } else {
      throw new RuntimeException(
          String.format("Unhandled AST operator: %s with > 2 children, tree: %s", node.getText(), node.dump()));
    }
  }

  private SqlNode visitUnaryOperator(ASTNode node, ParseContext ctx) {
    SqlNode operand = visit((ASTNode) node.getChildren().get(0), ctx);
    SqlOperator operator = functionResolver.resolveUnaryOperator(node.getText());
    return operator.createCall(ZERO, operand);
  }

  private SqlNode visitBinaryOperator(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);
    checkState(sqlNodes.size() == 2);
    return functionResolver.resolveBinaryOperator(node.getText()).createCall(ZERO, sqlNodes);
  }

  @Override
  protected SqlNode visitDotOperator(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);
    checkState(sqlNodes != null && sqlNodes.size() == 2);
    // return SqlStdOperatorTable.DOT.createCall(ZERO, sqlNodes);
    if (sqlNodes.get(0) instanceof SqlIdentifier) {
      SqlIdentifier left = (SqlIdentifier) sqlNodes.get(0);
      SqlIdentifier right = (SqlIdentifier) sqlNodes.get(1);
      Iterable<String> names = Iterables.concat(left.names, right.names);
      return new SqlIdentifier(ImmutableList.copyOf(names), ZERO);
    } else {
      return FunctionFieldReferenceOperator.DOT.createCall(ZERO, sqlNodes);
    }
  }

  @Override
  protected SqlNode visitLParen(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);
    checkState(sqlNodes.size() == 2);
    return SqlStdOperatorTable.ITEM.createCall(ZERO, sqlNodes);
  }

  @Override
  protected SqlNode visitFunctionStar(ASTNode node, ParseContext ctx) {
    ASTNode functionNode = (ASTNode) node.getChildren().get(0);
    List<SqlOperator> functions = SqlStdOperatorTable.instance().getOperatorList().stream()
        .filter(f -> functionNode.getText().equalsIgnoreCase(f.getName())).collect(Collectors.toList());
    checkState(functions.size() == 1);
    return new SqlBasicCall(functions.get(0), new SqlNode[] { new SqlIdentifier("", ZERO) }, ZERO);
  }

  @Override
  protected SqlNode visitFunctionDistinct(ASTNode node, ParseContext ctx) {
    return visitFunctionInternal(node, ctx, SqlSelectKeyword.DISTINCT.symbol(ZERO));
  }

  //todo 暂时改为 visitHiveFunctionInternal
  @Override
  protected SqlNode visitFunction(ASTNode node, ParseContext ctx) {
    return visitFunctionInternal(node, ctx, null);
//    return visitHiveFunctionInternal(node, ctx, null);
  }





  //todo 这里其实只是为了
  private SqlNode visitHiveFunctionInternal(ASTNode node, ParseContext ctx, SqlLiteral quantifier) {
    ArrayList<Node> children = node.getChildren();
    checkState(!children.isEmpty());
    ASTNode functionNode = (ASTNode) children.get(0);
    String functionName = functionNode.getText();
    List<SqlNode> sqlOperands = visitChildren(children, ctx);
    SqlIdentifier identifier = new SqlIdentifier(functionName, SqlParserPos.ZERO);
    return new HiveFunctionSqlNode(SqlParserPos.ZERO, identifier,  new SqlNodeList(sqlOperands, ZERO));
  }

  private SqlNode visitFunctionInternal(ASTNode node, ParseContext ctx, SqlLiteral quantifier) {
    ArrayList<Node> children = node.getChildren();
    checkState(children.size() > 0);
    ASTNode functionNode = (ASTNode) children.get(0);
    String functionName = functionNode.getText();
    List<SqlNode> sqlOperands = visitChildren(children, ctx);


    if (functionName.equalsIgnoreCase("internal_interval")) {
      SqlNode originalNode = sqlOperands.get(0);
      SqlNode intervalType = sqlOperands.get(1);
      SqlNode value = sqlOperands.get(2);

      // Create a new SqlCall using SqlHiveInternalIntervalOperator
      return SqlHiveInternalIntervalOperator.INSTANCE.createCall(
              SqlParserPos.ZERO,
              new SqlIdentifier("internal_interval", originalNode.getParserPosition()),
              intervalType,
              value
      );
    }

    Function hiveFunction = functionResolver.tryResolve(functionName, ctx.hiveTable.orElse(null),
        // The first element of sqlOperands is the operator itself. The actual # of operands is sqlOperands.size() - 1
        sqlOperands.size() - 1);

    // Special treatment for Window Function
    SqlNode lastSqlOperand = sqlOperands.get(sqlOperands.size() - 1);
    if (lastSqlOperand instanceof SqlWindow) {
      // For a SQL example of "func() OVER (PARTITIONED BY ...)":
      // In Hive, TOK_WINDOWSPEC (the window spec) is the last operand of the function "func":
      //    TOK_FUNCTION will have 1+N+1 children, where the first is the function name, the last is TOK_WINDOWSPEC
      //    and everything in between are the operands of the function
      // In Calcite, SqlWindow (the window spec) is a sibling of the function "func":
      //    SqlBasicCall("OVER") will have 2 children: "func" and SqlWindow
      /** See {@link #visitWindowSpec(ASTNode, ParseContext)} for SQL, AST Tree and SqlNode Tree examples */
      SqlNode func =
          hiveFunction.createCall(sqlOperands.get(0), sqlOperands.subList(1, sqlOperands.size() - 1), quantifier);
      SqlNode window = lastSqlOperand;
      return new SqlBasicCall(SqlStdOperatorTable.OVER, new SqlNode[] { func, window }, ZERO);
    }

    if (functionName.equalsIgnoreCase("SUBSTRING")) {
      // Calcite overrides instance of SUBSTRING with its default SUBSTRING function as defined in SqlStdOperatorTable,
      // so we rewrite instances of SUBSTRING as SUBSTR
      SqlNode originalNode = sqlOperands.get(0);
      SqlNode substrNode = new SqlIdentifier(ImmutableList.of("SUBSTR"), null, originalNode.getParserPosition(), null);
      hiveFunction = functionResolver.tryResolve("SUBSTR", ctx.hiveTable.orElse(null), sqlOperands.size() - 1);
      return hiveFunction.createCall(substrNode, sqlOperands.subList(1, sqlOperands.size()), quantifier);
    }

    return hiveFunction.createCall(sqlOperands.get(0), sqlOperands.subList(1, sqlOperands.size()), quantifier);
  }

  @Override
  protected SqlNode visitSelectExpr(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);
    if (sqlNodes.size() == 1) {
      return sqlNodes.get(0);
    } else if (sqlNodes.size() == 2) {
      return new SqlBasicCall(SqlStdOperatorTable.AS, sqlNodes.toArray(new SqlNode[0]), ZERO);
    } else if (sqlNodes.size() >= 3) {
      // lateral view alias have 3+ args
      List<SqlNode> nodes = new ArrayList<>();
      nodes.add(sqlNodes.get(0));
      nodes.add(sqlNodes.get(sqlNodes.size() - 1)); // last
      nodes.addAll(sqlNodes.subList(1, sqlNodes.size() - 1));
      return new SqlBasicCall(SqlStdOperatorTable.AS, nodes.toArray(new SqlNode[0]), ZERO);
    } else {
      throw new UnhandledASTTokenException(node);
    }
  }

  @Override
  protected SqlNode visitSelectDistinct(ASTNode node, ParseContext ctx) {
    ctx.keywords = new SqlNodeList(ImmutableList.of(SqlSelectKeyword.DISTINCT.symbol(ZERO)), ZERO);
    return visitSelect(node, ctx);
  }

  @Override
  protected SqlNode visitSelect(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);
    ctx.selects = new SqlNodeList(sqlNodes, ZERO);
    return ctx.selects;
  }

  @Override
  protected SqlNode visitTabRefNode(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);
    checkState(sqlNodes != null && !sqlNodes.isEmpty());
    if (sqlNodes.size() == 1) {
      return sqlNodes.get(0);
    }
    if (sqlNodes.size() == 2) {
      return new SqlBasicCall(SqlStdOperatorTable.AS, sqlNodes.toArray(new SqlNode[0]), ZERO);
    }

    throw new UnhandledASTTokenException(node);
  }


  //todo 这里为啥的names hive 原生的ast 并没有 names 语法
  @Override
  protected SqlNode visitTabnameNode(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodes = visitChildren(node, ctx);
    List<String> names =
        sqlNodes.stream().map(s -> ((SqlIdentifier) s).names).flatMap(List::stream).collect(Collectors.toList());

    return new SqlIdentifier(names, ZERO);
  }

  protected SqlNode visitSubqueryOp(ASTNode node, ParseContext ctx) {
    throw new UnhandledASTTokenException(node);
  }

  private SqlOperator getSubQueryOp(ASTNode node, ParseContext ctx) {
    checkState(node.getChildren().size() == 1, node.dump());
    String opName = ((ASTNode) node.getChildren().get(0)).getText();
    if (opName.equalsIgnoreCase("in")) {
      return SqlStdOperatorTable.IN;
    } else if (opName.equalsIgnoreCase("exists")) {
      return SqlStdOperatorTable.EXISTS;
    } else {
      throw new UnhandledASTTokenException(node);
    }
  }

  @Override
  protected SqlNode visitSubqueryExpr(ASTNode node, ParseContext ctx) {
    ArrayList<Node> children = node.getChildren();
    checkState(children.size() >= 2);
    SqlOperator op = getSubQueryOp((ASTNode) node.getChildren().get(0), ctx);
    SqlNode subQuery = visit((ASTNode) children.get(1), ctx);
    List<SqlNode> operands = new ArrayList<>();
    operands.add(subQuery);
    if (children.size() == 3) {
      SqlNode lhs = visit(((ASTNode) children.get(2)), ctx);
      operands.add(0, lhs);
    }
    return new SqlBasicCall(op, operands.toArray(new SqlNode[0]), ZERO);
  }

  @Override
  protected SqlNode visitSubquery(ASTNode node, ParseContext ctx) {
    ParseContext subQueryContext = new ParseContext(ctx.getHiveTable().orElse(null));
    List<SqlNode> sqlNodes = visitChildren(node, subQueryContext);
    if (sqlNodes.size() == 1) {
      return sqlNodes.get(0);
    } else if (sqlNodes.size() == 2) {
      return new SqlBasicCall(SqlStdOperatorTable.AS, sqlNodes.toArray(new SqlNode[0]), ZERO);
    }
    throw new UnhandledASTTokenException(node);
  }

  @Override
  protected SqlNode visitFrom(ASTNode node, ParseContext ctx) {
    List<SqlNode> children = visitChildren(node, ctx);
    if (children.size() == 1) {
      ctx.from = children.get(0);
      return children.get(0);
    }
    // TODO: handle join
    throw new UnsupportedASTException(node.dump());
  }

  @Override
  protected SqlNode visitIdentifier(ASTNode node, ParseContext ctx) {
    return new SqlIdentifier(node.getText(), ZERO);
  }

  /** See {@link #removeBackslashBeforeQuotes}
   * We use removeBackslashBeforeQuotes to remove the backslash before quotes,
   * so that we maintain patterns like {@code I'm} or {@code abc"xyz} as is in the java object in memory,
   * the escaped literal string representation will be generated when the SqlNode is written to string
   * by the SqlWriter, which can be controlled by the SqlDialect to decide the choice of escaping mechanism.
   * */
  @Override
  protected SqlNode visitStringLiteral(ASTNode node, ParseContext ctx) {
    // TODO: Add charset here. UTF-8 is not supported by calcite
    String text = node.getText();
    checkState(text.length() >= 2);
    return SqlLiteral.createCharString(removeBackslashBeforeQuotes(text.substring(1, text.length() - 1)), ZERO);
  }

  private String removeBackslashBeforeQuotes(String input) {
    // matches a \' or \" literal pattern
    Pattern pattern = Pattern.compile("\\\\['\"]");
    Matcher matcher = pattern.matcher(input);

    StringBuffer res = new StringBuffer();
    while (matcher.find()) {
      String replacement = matcher.group().substring(1);
      matcher.appendReplacement(res, replacement);
    }
    matcher.appendTail(res);

    return res.toString();
  }


//  todo number literal 如何构建sqlnode
//  @Override
//  protected SqlNode visitNumberLiteral(ASTNode node, ParseContext ctx) {
//    String text = node.getText();
//    checkState(text.length() >= 2);
//    return SqlLiteral.createExactNumeric(text.substring(0, text.length() - 1), ZERO);
//  }

  protected SqlNode visitNumberLiteral(ASTNode node, ParseContext ctx) {
    String text = node.getText();
    checkState(text.length() > 0);

    // 检查是否有类型后缀
    char lastChar = text.charAt(text.length() - 1);
    if (Character.isLetter(lastChar)) {
      // 如果有后缀，去掉它
      text = text.substring(0, text.length() - 1);
    }

    // 根据数字的格式创建适当的 SqlLiteral
    if (text.contains(".") || text.toLowerCase().contains("e")) {
      // 浮点数
      return SqlLiteral.createApproxNumeric(text, ZERO);
    } else {
      // 整数
      return SqlLiteral.createExactNumeric(text, ZERO);
    }
  }

  @Override
  protected SqlNode visitDateLiteral(ASTNode node, ParseContext ctx) {
    String text = node.getText();
    checkState(text.length() >= 2);
    return SqlLiteral.createCharString(text.substring(1, text.length() - 1), ZERO);
  }

  @Override
  protected SqlNode visitQueryNode(ASTNode node, ParseContext ctx) {
    ArrayList<Node> children = node.getChildren();
    checkState(children != null && !children.isEmpty());
    SqlNode cte = null;
    ParseContext qc = new ParseContext(ctx.getHiveTable().orElse(null));
    for (Node child : node.getChildren()) {
      ASTNode ast = (ASTNode) child;
      if (ast.getType() == HiveParser.TOK_CTE) {
        // Child of type TOK_CTE represents the "WITH" list
        /** See {@link #visitCTE(ASTNode, ParseContext) visitCTE} for the return value */
        cte = visit(ast, new ParseContext(null));
      } else {
        // The return values are ignored since all other children of SELECT query will be captures via ParseConext qc.
        visit(ast, qc);
      }
    }

    //todo add sql hint to here, temporarily set to null
    SqlSelect select = new SqlSelect(ZERO, qc.keywords, qc.selects, qc.from, qc.where, qc.grpBy, qc.having, null,
        qc.orderBy, null, qc.fetch, null);
    if (cte != null) {
      // Calcite uses "SqlWith(SqlNodeList of SqlWithItem, SqlSelect)" to represent queries with WITH
      /** See {@link #visitCTE(ASTNode, ParseContext) visitCTE} for details */
      return new SqlWith(ZERO, (SqlNodeList) cte, select);
    } else {
      return select;
    }
  }

  protected SqlNode visitNil(ASTNode node, ParseContext ctx) {
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitBoolean(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.BOOLEAN);
  }

  @Override
  protected SqlNode visitInt(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.INTEGER);
  }

  @Override
  protected SqlNode visitSmallInt(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.SMALLINT);
  }

  @Override
  protected SqlNode visitBigInt(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.BIGINT);
  }

  @Override
  protected SqlNode visitTinyInt(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.TINYINT);
  }

  @Override
  protected SqlNode visitFloat(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.FLOAT);
  }

  @Override
  protected SqlNode visitDouble(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.DOUBLE);
  }

  @Override
  protected SqlNode visitVarchar(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.VARCHAR);
  }

  @Override
  protected SqlNode visitChar(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.CHAR);
  }

  @Override
  protected SqlNode visitString(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.VARCHAR);
  }

  @Override
  protected SqlDataTypeSpec visitMap(ASTNode node, ParseContext ctx) {
    List<SqlNode> children = visitChildren(node, ctx);
    SqlDataTypeSpec keySqlTypeNameSpec = (SqlDataTypeSpec) children.get(0);
    SqlDataTypeSpec valSqlTypeNameSpec = (SqlDataTypeSpec) children.get(1);
    SqlMapTypeNameSpec mapTypeSpec = new SqlMapTypeNameSpec(keySqlTypeNameSpec, valSqlTypeNameSpec, ZERO);
    return new SqlDataTypeSpec(mapTypeSpec, ZERO);
  }

  @Override
  protected SqlNode visitList(ASTNode node, ParseContext ctx) {
    List<SqlNode> children = visitChildren(node, ctx);
    SqlDataTypeSpec typeSpec = (SqlDataTypeSpec) children.get(0);
      return new SqlArrayTypeSpec(typeSpec, true, ZERO);
  }

  protected SqlNode visitStruct(ASTNode node, ParseContext ctx) {
    SqlParserPos pos = SqlParserPos.ZERO;
    if (((ASTNode) node.getChildren().get(0)).getType()  == HiveParser.TOK_TABCOLLIST){
      ASTNode colListsNode = (ASTNode) node.getChildren().get(0);
      List<SqlNode> sqlNodeList =visitChildren (colListsNode, ctx);
      return new SqlStructType(new SqlNodeList(sqlNodeList, ZERO), pos);
    }else {
      throw new UnhandledASTTokenException(node);
    }

  }

  @Override
  protected SqlNode visitBinary(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.BINARY);
  }

  @Override
  protected SqlNode visitDecimal(ASTNode node, ParseContext ctx) {
    if (node.getChildCount() == 2) {
      try {
        final SqlTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(SqlTypeName.DECIMAL,
            Integer.parseInt(((ASTNode) node.getChildren().get(0)).getText()),
            Integer.parseInt(((ASTNode) node.getChildren().get(1)).getText()), ZERO);
        return new SqlDataTypeSpec(typeNameSpec, ZERO);
      } catch (NumberFormatException e) {
        return createBasicTypeSpec(SqlTypeName.DECIMAL);
      }
    }
    return createBasicTypeSpec(SqlTypeName.DECIMAL);
  }

  @Override
  protected SqlNode visitDate(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.DATE);
  }

  @Override
  protected SqlNode visitTimestamp(ASTNode node, ParseContext ctx) {
    return createBasicTypeSpec(SqlTypeName.TIMESTAMP);
  }

  @Override
  protected SqlNode visitIsNull(ASTNode node, ParseContext ctx) {
    return SqlLiteral.createCharString("is null", ZERO);
  }

  @Override
  protected SqlNode visitIsNotNull(ASTNode node, ParseContext ctx) {
    return SqlLiteral.createCharString("is not null", ZERO);
  }

  @Override
  protected SqlNode visitKeywordLiteral(ASTNode node, ParseContext ctx) {
    return SqlLiteral.createCharString(node.getText(), ZERO);
  }

  @Override
  protected SqlNode visitCTE(ASTNode node, ParseContext ctx) {
    // ASTNode tree from Hive Antlr
    // TOK_QUERY
    // - TOK_FROM
    // - TOK_INSERT
    // -- TOK_DESTINATION
    // -- TOK_SELECT
    // - TOK_CTE       <-- processed by this method visitCTE
    // -- TOK_SUBQUERY
    // --- TOK_QUERY
    // --- LITERAL (alias of the subquery)
    // -- TOK_SUBQUERY
    // --- TOK_QUERY
    // --- LITERAL (alias of the subquery)

    // SqlNode tree expected by Calcite
    // - SqlWith
    // -- withList: SqlNodeList  <-- returned by this method visitCTE
    // --- element: SqlWithItem
    // ---- id: SimpleIdentifier
    // ---- columnList: SqlNodeList (column aliases - not supported by Hive)
    // ---- definition: SqlSelect
    // -- node: SqlSelect

    ArrayList<Node> children = node.getChildren();
    checkState(children != null && !children.isEmpty());
    // First, visit the children to capture all their translation result (in List<SqlNode>)
    // All children are expected to be translated into SqlBasicCall(definition, alias) by visitSubquery
    /** See {@link #visitSubquery(ASTNode, ParseContext) visitSubquery} for details */
    List<SqlNode> sqlNodeList =visitChildren (node, ctx);
    // Second, translate the list of SqlBasicCall to list of SqlWithItem
    List<SqlWithItem> withItemList = new ArrayList<>();
    for (SqlNode sqlNode : sqlNodeList) {
      SqlBasicCall call = (SqlBasicCall) sqlNode;
      SqlNode definition = call.getOperandList().get(0);
      SqlNode alias = call.getOperandList().get(1);
      SqlWithItem withItem = new SqlWithItem(ZERO, (SqlIdentifier) alias, null, definition);
      withItemList.add(withItem);
    }
    // Return a SqlNodeList with the contents of the withItemList
    SqlNodeList result = new SqlNodeList(withItemList, ZERO);
    return result;
  }

  @Override
  protected SqlNode visitWindowSpec(ASTNode node, ParseContext ctx) {
    // See Apache Hive source code: https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/parse/CalcitePlanner.java#L4339
    // "private RelNode genSelectForWindowing(QB qb, RelNode srcRel, HashSet<ColumnInfo> newColumns)"

    // Example SQL:
    //   ROW_NUMBER() OVER (PARTITION BY x ORDER BY y ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
    // Hive Antlr ASTNode Tree:
    //TOK_FUNCTION
    //   ROW_NUMBER
    //   TOK_WINDOWSPEC  <-- processed by this node
    //      TOK_PARTITIONINGSPEC
    //         TOK_DISTRIBUTEBY
    //         TOK_ORDERBY
    //      TOK_WINDOWRANGE
    //         CURRENT
    //         FOLLOWING
    //            1

    // Calcite SqlNode Tree:
    // SqlBasicCall(Over):
    // - SqlBasicCall(ROW_NUMBER)
    // - SqlWindow  <-- returned by this node
    // -- partitionList
    // -- orderList
    // -- isRows
    // -- lowerBound
    // -- upperBound
    // -- allowPartial

    // Use a separate context to avoid inadvertently having the "ORDER BY" clause from the window added to the SELECT query.
    ctx = new ParseContext(ctx.hiveTable.orElse(null));

    SqlWindow partitionSpec = (SqlWindow) visitOptionalChildByType(node, ctx, HiveParser.TOK_PARTITIONINGSPEC);
    SqlWindow windowRange = (SqlWindow) visitOptionalChildByType(node, ctx, HiveParser.TOK_WINDOWRANGE);
    SqlWindow windowValues = (SqlWindow) visitOptionalChildByType(node, ctx, HiveParser.TOK_WINDOWVALUES);
    SqlWindow window = windowRange != null ? windowRange : windowValues;

    return new SqlWindow(ZERO, null, null, partitionSpec == null ? SqlNodeList.EMPTY : partitionSpec.getPartitionList(),
        partitionSpec == null ? SqlNodeList.EMPTY : partitionSpec.getOrderList(),
        SqlLiteral.createBoolean(windowRange != null, ZERO), window == null ? null : window.getLowerBound(),
        window == null ? null : window.getUpperBound(), null);
  }

  @Override
  protected SqlNode visitPartitioningSpec(ASTNode node, ParseContext ctx) {
    SqlNode partitionList = visitOptionalChildByType(node, ctx, HiveParser.TOK_DISTRIBUTEBY);
    SqlNode orderList = visitOptionalChildByType(node, ctx, HiveParser.TOK_ORDERBY);
    return new SqlWindow(ZERO, null, null, partitionList != null ? (SqlNodeList) partitionList : SqlNodeList.EMPTY,
        orderList != null ? (SqlNodeList) orderList : SqlNodeList.EMPTY, null, null, null, null);
  }

  @Override
  protected SqlNode visitDistributeBy(ASTNode node, ParseContext ctx) {
    return new SqlNodeList(visitChildren(node, ctx), ZERO);
  }

  @Override
  protected SqlNode visitClusterBy(ASTNode node, ParseContext ctx) {
    return new SqlNodeList(visitChildren(node, ctx), ZERO);
  }

  //叶子节点
  @Override
  protected SqlNode visitWindowRange(ASTNode node, ParseContext ctx) {
    // Hive AST:
    //      TOK_WINDOWRANGE  (ROWS ...)
    //         CURRENT
    //         FOLLOWING
    //            1
    List<SqlNode> sqlNodeList = visitChildren(node, ctx);
    SqlNode preceding = sqlNodeList.get(0);
    SqlNode following = (sqlNodeList.size() < 2 ? null : sqlNodeList.get(1));

    return new SqlWindow(ZERO, null, null, SqlNodeList.EMPTY, SqlNodeList.EMPTY, null, preceding, following, null);
  }


  //叶子节点
  @Override
  protected SqlNode visitWindowValues(ASTNode node, ParseContext ctx) {
    // Hive AST:
    //      TOK_WINDOWVALUES  (VALUES ...)
    //         CURRENT
    //         FOLLOWING
    //            1

    // Reuse the same code of visitWindowRange since the AST structure is exactly the same
    return visitWindowRange(node, ctx);
  }

  //叶子节点
  @Override
  protected SqlNode visitPreceding(ASTNode node, ParseContext ctx) {
    SqlNode sqlNode = visitChildren(node, ctx).get(0);
    if (sqlNode.getKind() == SqlKind.LITERAL && sqlNode.toString().equalsIgnoreCase("'UNBOUNDED'")) {
      return SqlWindow.createUnboundedPreceding(ZERO);
    } else {
      return SqlWindow.createPreceding(sqlNode, ZERO);
    }
  }

  //叶子节点
  @Override
  protected SqlNode visitFollowing(ASTNode node, ParseContext ctx) {
    SqlNode sqlNode = visitChildren(node, ctx).get(0);
    if (sqlNode.getKind() == SqlKind.LITERAL && sqlNode.toString().equalsIgnoreCase("'UNBOUNDED'")) {
      return SqlWindow.createUnboundedFollowing(ZERO);
    } else {
      return SqlWindow.createFollowing(sqlNode, ZERO);
    }
  }

  //叶子节点
  @Override
  protected SqlNode visitCurrentRow(ASTNode node, ParseContext ctx) {
    return SqlWindow.createCurrentRow(ZERO);
  }

  private SqlDataTypeSpec createBasicTypeSpec(SqlTypeName type) {
    final SqlTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(type, ZERO);
    return new SqlDataTypeSpec(typeNameSpec, ZERO);
  }

 //非叶子节点
  @Override
  protected SqlNode visitTableTokOrCol(ASTNode node, ParseContext ctx) {
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitCreateDatabase(ASTNode node, ParseContext ctx) {
    SqlIdentifier databaseName = null;
    boolean ifNotExists = false;
    SqlNode comment = null;
    SqlNode location = null;
    SqlNode managedLocation = null;
    SqlNodeList properties = null;
    boolean isSchema = false;
    boolean isRemote = false;
    SqlIdentifier connector = null;


    for (Node child : node.getChildren()) {
      ASTNode ast = (ASTNode) child;
      switch (ast.getType()) {
        case HiveParser.Identifier:
          databaseName = new SqlIdentifier(ast.getText(), ZERO);
          break;
        case HiveParser.TOK_IFNOTEXISTS:
          ifNotExists = true;
          break;
        case HiveParser.TOK_DATABASECOMMENT:
          comment = visit((ASTNode) ast.getChild(0), ctx);
          break;
        case HiveParser.TOK_DATABASEPROPERTIES:
          properties = (SqlNodeList) visit(ast, ctx);
          break;
        case HiveParser.TOK_DATABASELOCATION:
          location = visit((ASTNode) ast.getChild(0), ctx);
          break;
        case HiveParser.TOK_DATABASE_MANAGEDLOCATION:
          managedLocation = visit((ASTNode) ast.getChild(0), ctx);
          break;
        case HiveParser.TOK_DATACONNECTOR:
          connector = new SqlIdentifier(ast.getChild(0).getText(), ZERO);
          isRemote = true;
          break;
      }
    }
//    if (node.getToken().getText().equalsIgnoreCase("CREATE SCHEMA")) {
//      isSchema = true;
//    }
    return new SqlCreateDatabase(
            ZERO,
            databaseName,
            ifNotExists,
            comment,
            location,
            managedLocation,
            properties,
            isSchema,
            isRemote,
            connector
    );
  }

  @Override
  protected SqlNode visitDatabaseProperties(ASTNode node, ParseContext ctx) {
    List<SqlNode> propertyList = new ArrayList<>();
    for (Node child : node.getChildren()) {
      if (((ASTNode) child).getType() == HiveParser.TOK_DBPROPLIST) {
        propertyList.addAll(visitChildren((ASTNode) child, ctx));
      }
    }
    return new SqlNodeList(propertyList, ZERO);
  }


  @Override
  protected SqlNode visitSwitchDatabase(ASTNode node, ParseContext ctx) {
    SqlIdentifier databaseName = null;

    for (Node child : node.getChildren()) {
      ASTNode ast = (ASTNode) child;
      if (ast.getType() == HiveParser.Identifier) {
        databaseName = new SqlIdentifier(ast.getText(), ZERO);
        break;
      }
    }
    // Handle the special case of "USE DEFAULT"
    if (databaseName != null && databaseName.getSimple().equalsIgnoreCase("DEFAULT")) {
      databaseName = new SqlIdentifier("DEFAULT", ZERO);
    }

    return new SqlUseDatabase(ZERO, databaseName);
  }

  @Override
  protected SqlNode visitCreateTable(ASTNode node, ParseContext ctx) {
    CreateTableOptions ctOptions = new CreateTableOptions();
    ctOptions.ifNotExists = false;
    ctOptions.isExternal = false;
    ctOptions.isTemporary = false;

    for (Node child : node.getChildren()) {
      ASTNode ast = (ASTNode) child;
      switch (ast.getType()) {
        case HiveParser.TOK_TABNAME:
          ctOptions.name = (SqlIdentifier) visitTabnameNode(ast, ctx);
          break;
        case HiveParser.TOK_IFNOTEXISTS:
          ctOptions.ifNotExists = true;
          break;
        case HiveParser.KW_EXTERNAL:
          ctOptions.isExternal = true;
          break;
        case HiveParser.KW_TEMPORARY:
          ctOptions.isTemporary = true;
          break;
        case HiveParser.TOK_TABCOLLIST:
          ctOptions.columnList = (SqlNodeList) visitColumnList(ast, ctx);
          break;
        case HiveParser.TOK_TABLECOMMENT:
          ctOptions.comment = (SqlCharStringLiteral) visit((ASTNode)ast.getChild(0), ctx);
          break;
        case HiveParser.TOK_TABLEPARTCOLS:
          ctOptions.partitionByList = (SqlNodeList) visitPartitionColumns(ast, ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_BUCKETS:
          visitClusterAndSort(ast, ctOptions, ctx);
          break;
        case HiveParser.TOK_TABLEROWFORMAT:
          ctOptions.rowFormat = visitRowFormat(ast, ctx);
          break;
        case HiveParser.TOK_TABLESERIALIZER:
          ctOptions.rowFormat = visitTableSerializer(ast,ctx);
          break;
        case HiveParser.TOK_FILEFORMAT_GENERIC:
          ctOptions.storedAs = (SqlIdentifier) visit((ASTNode)ast.getChild(0), ctx);
          break;
        case HiveParser.TOK_TABLELOCATION:
          ctOptions.location = (SqlCharStringLiteral) visit((ASTNode)ast.getChild(0), ctx);
          break;
        case HiveParser.TOK_TABLEPROPERTIES:
          ctOptions.propertyList = (SqlNodeList) visit(ast, ctx);
          break;
        default:
          // Handle other cases or throw an exception
          break;
      }
    }

    return new SqlCreateTable(
            ZERO,
            ctOptions.name,
            ctOptions.columnList,
            new ArrayList<>(), // tableConstraints
            ctOptions.propertyList,
            ctOptions.partitionByList,
            ctOptions.clusterByList,
            ctOptions.bucketNum,
            ctOptions.sortedByList,
            null, // skewedByList //todo
            ctOptions.storedAs,
            null, // storedBy
            ctOptions.location,
            ctOptions.rowFormat,
            ctOptions.comment,
            ctOptions.isTemporary,
            ctOptions.isExternal,
            false, // isTransactional
            ctOptions.ifNotExists,
            null // query
    );
  }


  private SqlNode visitPartitionColumns(ASTNode node, ParseContext ctx) {
    List<SqlNode> partitionCols = new ArrayList<>();
    for (Node child : node.getChildren()) {
      ASTNode ast = (ASTNode) child;
      SqlIdentifier colName = (SqlIdentifier) visit((ASTNode)ast.getChild(0), ctx);
      SqlDataTypeSpec colType = (SqlDataTypeSpec) visit((ASTNode)ast.getChild(1), ctx);
      partitionCols.add(new SqlTableColumn.SqlRegularColumn(ZERO, colName, null, colType, null));
    }
    return new SqlNodeList(partitionCols, ZERO);
  }


  /*├TOK_ALTERTABLE_BUCKETS (852)
    │   │   ├TOK_TABCOLNAME (1232)
    │   │   │   └id (24)
    │   │   ├TOK_TABCOLNAME (1232)
    │   │   │   └TOK_TABSORTCOLNAMEASC (1260)
    │   │   │       └TOK_NULLS_FIRST (1086)
    │   │   │           └name (24)
    │   │   └16 (420)
    */
  //test done ALTER TABLE table_name PARTITION (year=2024, month=8) CLUSTERED BY (id) SORTED BY (name) INTO 5 BUCKETS
  private void visitClusterAndSort(ASTNode node, CreateTableOptions ctOptions, ParseContext ctx) {
    for (Node child : node.getChildren()) {
      ASTNode ast = (ASTNode) child;
      if (ast.getType() == HiveParser.TOK_TABCOLNAME &&  ((ASTNode) ast.getChildren().get(0)).getType() == HiveParser.Identifier) {
        ctOptions.clusterByList = new SqlNodeList(visitChildren(ast, ctx), ZERO);
      } else if (ast.getType() == HiveParser.TOK_TABCOLNAME &&  ((ASTNode) ast.getChildren().get(0)).getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
        ctOptions.sortedByList = new SqlNodeList(visitChildren(ast, ctx), ZERO);
      }else if(ast.getType() == HiveParser.Number){
        ctOptions.bucketNum = visit(ast, ctx);
      }
    }
  }


  private SqlNode visitRowFormat(ASTNode node, ParseContext ctx) {
    ASTNode child = (ASTNode) node.getChild(0);
    if (child.getType() == HiveParser.TOK_SERDEPROPS) {
      return visitRowFormatDelimited(child, ctx);
    } else {
      // Handle SERDE case
      return null;
    }
  }

  private SqlNode visitRowFormatDelimited(ASTNode node, ParseContext ctx) {
    SqlCharStringLiteral fieldsTerminatedBy = null;
    SqlCharStringLiteral escapedBy = null;
    SqlCharStringLiteral collectionItemsTerminatedBy = null;
    SqlCharStringLiteral mapKeysTerminatedBy = null;
    SqlCharStringLiteral linesTerminatedBy = null;

    for (Node child : node.getChildren()) {
      ASTNode ast = (ASTNode) child;
      switch (ast.getType()) {
        case HiveParser.TOK_TABLEROWFORMATFIELD:
          fieldsTerminatedBy = (SqlCharStringLiteral) visit((ASTNode)ast.getChild(0), ctx);
          break;
        case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
          collectionItemsTerminatedBy = (SqlCharStringLiteral) visit((ASTNode)ast.getChild(0), ctx);
          break;
        case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
          mapKeysTerminatedBy = (SqlCharStringLiteral) visit((ASTNode)ast.getChild(0), ctx);
          break;
        case HiveParser.TOK_TABLEROWFORMATLINES:
          linesTerminatedBy = (SqlCharStringLiteral) visit((ASTNode)ast.getChild(0), ctx);
          break;
        // Handle other cases
      }
    }

    return new SqlRowFormatDelimited(ZERO, fieldsTerminatedBy, escapedBy, collectionItemsTerminatedBy,
            mapKeysTerminatedBy, linesTerminatedBy, null);
  }

  @Override
  protected SqlNodeList visitTableProperties(ASTNode node, ParseContext ctx) {
      return (SqlNodeList) visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNodeList visitTablePropList(ASTNode node, ParseContext ctx) {
    List<SqlNode> properties = visitChildren(node, ctx);
    return new SqlNodeList(properties, ZERO);
  }

  @Override
  protected SqlNode visitTableProperty(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodeList = visitChildren(node, ctx);
    SqlNode key = sqlNodeList.get(0);
    SqlNode value = sqlNodeList.get(1);
    return new SqlProperty(key, value,ZERO);
  }



  @Override
  protected SqlNode visitColumnList(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodeList = visitChildren(node, ctx);
    return new SqlNodeList(sqlNodeList, ZERO);
  }

  @Override
  protected SqlNode visitColumn(ASTNode node, ParseContext ctx) {
    SqlIdentifier columnName = (SqlIdentifier) visit((ASTNode) node.getChild(0), ctx);
    SqlDataTypeSpec dataType = (SqlDataTypeSpec) visit((ASTNode) node.getChild(1), ctx);
    SqlNode comment = null;
    SqlColumnConstraint constraint = null;

    for (int i = 2; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      switch (child.getType()) {
        case HiveParser.StringLiteral:
          comment = visit(child, ctx);
          break;
        case HiveParser.TOK_PRIMARY_KEY:
          constraint = (SqlColumnConstraint) visitPrimaryKey(child, ctx);
          break;
        case HiveParser.TOK_UNIQUE:
          constraint = (SqlColumnConstraint) visitUnique(child, ctx);
          break;
        case HiveParser.TOK_NOT_NULL:
          constraint = (SqlColumnConstraint) visitNotNull(child, ctx);
          break;
        case HiveParser.TOK_DEFAULT_VALUE:
          constraint = (SqlColumnConstraint) visitDefaultValue(child, ctx);
          break;
        case HiveParser.TOK_CHECK_CONSTRAINT:
          constraint = (SqlColumnConstraint) visitCheckConstraint(child, ctx);
          break;
      }
    }

    return new SqlTableColumn.SqlRegularColumn(ZERO, columnName, comment, dataType, constraint);
  }

  @Override
  protected SqlNode visitTableName(ASTNode node, ParseContext ctx) {
    return visitChildren(node, ctx).get(0);
  }


  //test done
  //ALTER TABLE old_table_name RENAME TO new_table_name
  @Override
  protected SqlNode visitAlterTableRename(ASTNode node, ParseContext ctx) {
    SqlNode  new_name = visitChildren(node, ctx).get(0);
     return new SqlAlterTableRename(ZERO, new_name);
  }


  //test done ALTER TABLE table_name SET TBLPROPERTIES ('comment' = 'New table comment','b'='c')
  @Override
  protected SqlNode visitAlterTableProperties(ASTNode node, ParseContext ctx) {
    SqlNodeList  props = (SqlNodeList) visitChildren(node, ctx).get(0);
    return  new SqlAlterTableSetProperties(ZERO, props);
  }


  //test done ALTER TABLE table_name SET SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' WITH SERDEPROPERTIES ('serialization.format' = ',', 'field.delim' = ',')
  @Override
  protected SqlNode visitAlterTableSerializer(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands = visitChildren(node, ctx);

    SqlNodeList  props = new SqlNodeList(operands, ZERO);
    SqlNode serdeName = props.get(0);
    SqlNodeList serde_properties =  SqlNodeList.EMPTY;
    if (props.size() > 1) {
      serde_properties  = (SqlNodeList) props.get(1);
    }
    return new SqlAlterTableSetSerde(ZERO,serdeName,serde_properties);
  }

  //test done ALTER TABLE table_name UNSET SERDEPROPERTIES ('field.delim')
  @Override
  protected SqlNode visitAlterTableUnsetSerdeProperties(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);
    SqlNodeList  props = new SqlNodeList(operands, ZERO);
    if (props.size() >1 ){
      throw new UnsupportedOperationException("TOK_ALTERTABLE_UNSETSERDEPROPERTIES only supported one child in hive");
    }
    SqlNodeList serde_properties  = (SqlNodeList) props.get(0);
    return  new SqlAlterTableUnsetSerdeProperties(ZERO, serde_properties);
  }

  @Override
  protected SqlNode visitAlterTableClusterSort(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);
    SqlNodeList  props = new SqlNodeList(operands, ZERO);
    return props;
  }



  //test done ALTER TABLE my_table NOT CLUSTERED
  protected SqlNode visitNotClustered(ASTNode node, ParseContext ctx) {
    return new SqlIdentifier("NOT CLUSTERED",ZERO);
  }
  //test done  ALTER TABLE my_table NOT SORTED
  protected SqlNode visitNotSort(ASTNode node, ParseContext ctx) {
    return new SqlIdentifier("NOT SORTED",ZERO);
  }



  /**
   *
   * └TOK_TABLESKEWED (1253)
   *     ├TOK_TABCOLNAME (1232)
   *     │   ├col1 (24)
   *     │   └col2 (24)
   *     ├TOK_TABCOLVALUE_PAIR (1235)
   *     │   ├TOK_TABCOLVALUES (1234)
   *     │   │   └TOK_TABCOLVALUE (1233)
   *     │   │           ├'s1' (432)
   *     │   │           └1 (420)
   *     │   ├TOK_TABCOLVALUES (1234)
   *     │   │   └TOK_TABCOLVALUE (1233)
   *     │   │           ├'s3' (432)
   *     │   │           └3 (420)
   *     │   ├TOK_TABCOLVALUES (1234)
   *     │   │   └TOK_TABCOLVALUE (1233)
   *     │   │           ├'s13' (432)
   *     │   │           └13 (420)
   *     │   └TOK_TABCOLVALUES (1234)
   *     │           └TOK_TABCOLVALUE (1233)
   *     │               ├'s78' (432)
   *     │               └78 (420)
   *     └TOK_STOREDASDIRS (1217)
   *
   *
   * TOK_TABLESKEWED (1253)
   *   ├TOK_TABCOLNAME (1232)
   *   │   └key (24)
   *   ├TOK_TABCOLVALUE (1233)
   *   │   ├1 (420)
   *   │   ├5 (420)
   *   │   └6 (420)
   *   └TOK_STOREDASDIRS (1217)
   * */
  //test done ALTER TABLE list_bucket_multiple SKEWED BY (col1, col2) ON (('s1',1), ('s3',3), ('s13',13), ('s78',78)) STORED AS DIRECTORIES
  //test done ALTER TABLE list_bucket_single  SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES
  //test done ALTER TABLE table_name SKEWED BY (col1, col2)    ON ((1,1), (2,2)) STORED AS DIRECTORIES
  @Override
  protected SqlNode visitAlterTableSkewed(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);

    SqlNodeList  alter_table_buckets_childs = new SqlNodeList(operands, ZERO);

    SqlNodeList  skewedCols = SqlNodeList.EMPTY;
    SqlNode  skewedValues= null;
    SqlNode  skewedValuePair = null;
    SqlNode  storedAsDirs = null;
    boolean  notSkewed = false;

    if (!alter_table_buckets_childs.isEmpty()){
      for (SqlNode  skwed_child :alter_table_buckets_childs) {
        if (skwed_child instanceof SqlStoredAsDirs) {
          storedAsDirs = skwed_child;
        } else if (skwed_child instanceof SqlNodeList) {
          for(SqlNode  table_skwed_child : (SqlNodeList) skwed_child){
            if (table_skwed_child instanceof SqlStoredAsDirs) {
              storedAsDirs = table_skwed_child;
            } else if (table_skwed_child instanceof SqlTableColValuePair) {
              skewedValuePair =  table_skwed_child;
            } else if (table_skwed_child instanceof SqlValueTuple) {
              skewedValues=table_skwed_child;
            } else if (table_skwed_child instanceof SqlNodeList && ((SqlNodeList) table_skwed_child).get(0) instanceof SqlIdentifier) {
              skewedCols = (SqlNodeList)table_skwed_child;
            }
          }
        }
      }
    } else{
      notSkewed = true;
    }

    return  new SqlAlterTableSetSkewed(ZERO, skewedCols, skewedValues, skewedValuePair, storedAsDirs,notSkewed);
//    SqlNodeList  props = (SqlNodeList) visitChildren(node, ctx);
//    ASTNode table_skwed_child = (ASTNode) node.getChild(0);
//
//    if (child.getChildCount() == 1 && child.getChild(0).getType() ==HiveParser.TOK_STOREDASDIRS) {
//      atOptions.storedAsDirs = true;
//    } else if (child.getChildCount() == 0) {
//      atOptions.operation = AlterTableOperation.NOT_SKEWED;
//    } else if (child.getChild(0).getType() == HiveParser.TOK_TABLESKEWED) {
//      ASTNode skewedNode = (ASTNode) child.getChild(0);
//
//      // Process skewed columns
//      ASTNode colNameNode = (ASTNode) skewedNode.getChild(0);
//      List<SqlNode> skewedCols = new ArrayList<>();
//      for (int j = 0; j < colNameNode.getChildCount(); j++) {
//        skewedCols.add(new SqlIdentifier(colNameNode.getChild(j).getText(), SqlParserPos.ZERO));
//      }
//      atOptions.skewedCols = new SqlNodeList(skewedCols, SqlParserPos.ZERO);
//
//      // Process skewed values
//      ASTNode colValueNode = (ASTNode) skewedNode.getChild(1);
//      List<SqlNode> skewedValues = new ArrayList<>();
//
//      if (colValueNode.getType() == HiveParser.TOK_TABCOLVALUE) {
//        atOptions.skewedColsValuePair=false;
//        // Single column skewed values
//        for (int j = 0; j < colValueNode.getChildCount(); j++) {
//          ASTNode valueNode = (ASTNode) colValueNode.getChild(j);
//          SqlNode value = createSqlLiteral(valueNode);
//          skewedValues.add(value);
//        }
//      } else if (colValueNode.getType() == HiveParser.TOK_TABCOLVALUE_PAIR) {
//        atOptions.skewedColsValuePair=true;
//        // Multi-column skewed values
//        for (int j = 0; j < colValueNode.getChildCount(); j++) {
//          ASTNode tupleNode = (ASTNode) colValueNode.getChild(j).getChild(0);
//          List<SqlNode> tupleValues = new ArrayList<>();
//          for (int k = 0; k < tupleNode.getChildCount(); k++) {
//            ASTNode valueNode = (ASTNode) tupleNode.getChild(k);
//            SqlNode value = createSqlLiteral(valueNode);
//            tupleValues.add(value);
//          }
//          skewedValues.add(new SqlValueTuple(SqlParserPos.ZERO, tupleValues.toArray(new SqlNode[0])));
//        }
//      }
//      atOptions.skewedValues = new SqlNodeList(skewedValues, SqlParserPos.ZERO);
//
//      // Check if STORED AS DIRECTORIES is present
//      atOptions.storedAsDirs = skewedNode.getChildCount() > 2;
//    }
  }

  protected SqlNodeList visitTableSkewed(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);
    return new SqlNodeList(operands, ZERO);
  }


  @Override
  protected SqlNode visitStoredAsDirs(ASTNode node, ParseContext ctx) {
    return new SqlStoredAsDirs("Stored As Dirs",ZERO);
  }

  @Override
  protected SqlNode visitTabColValues(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);
    return new SqlNodeList(operands, ZERO);
  }

  @Override
  protected SqlNode visitTabColValue(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);
    SqlNodeList  values = new SqlNodeList(operands, ZERO);
    return new SqlValueTuple(ZERO, values);
  }

  //test done ALTER TABLE list_bucket_multiple SKEWED BY (col1, col2) ON (('s1',1), ('s3',3), ('s13',13), ('s78',78)) STORED AS DIRECTORIES
  @Override
  protected SqlNode visitTabColValuePair(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);
    SqlNodeList  value_pairs = new SqlNodeList(operands, ZERO);

    return new SqlTableColValuePair(ZERO, value_pairs);
  }

  /*├TOK_ALTERTABLE_BUCKETS (852)
    │   │   ├TOK_TABCOLNAME (1232)
    │   │   │   └id (24)
    │   │   ├TOK_TABCOLNAME (1232)
    │   │   │   └TOK_TABSORTCOLNAMEASC (1260)
    │   │   │       └TOK_NULLS_FIRST (1086)
    │   │   │           └name (24)
    │   │   └16 (420)
    */
  @Override
  protected SqlNode visitAlterTableBuckets(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);
    SqlNodeList  alter_table_buckets_childs = new SqlNodeList(operands, ZERO);
    SqlNodeList  clusterByList = SqlNodeList.EMPTY;
    SqlNodeList  sortedByList= SqlNodeList.EMPTY;;
    SqlNode  bucketNum= null;


    for (SqlNode  bucket_child :alter_table_buckets_childs) {
      if (bucket_child instanceof SqlNodeList && ((SqlNodeList) bucket_child).get(0) instanceof SqlIdentifier) {
        clusterByList= (SqlNodeList) bucket_child;
      } else if (bucket_child instanceof SqlNumericLiteral) {
        bucketNum =  bucket_child;
      } else if (bucket_child instanceof SqlNodeList && ((SqlNodeList) bucket_child).get(0) instanceof SqlBasicCall) {
        sortedByList = (SqlNodeList) bucket_child;
      }
    }

    return  new SqlAlterTableBuckets(ZERO, clusterByList,sortedByList,bucketNum);
  }

  @Override
  protected SqlNode visitAlterTableAddParts(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableDropParts(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitPartSpec(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);
    SqlNodeList  props = new SqlNodeList(operands, ZERO);
    return new SqlAlterPartition(ZERO,props);
  }

  @Override
  protected SqlNode visitIfExists(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitPurge(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableRenamePart(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableExchangePartition(ASTNode node, ParseContext ctx) {
    // Method content
      return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitTableColName(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands =  visitChildren(node, ctx);
    return new SqlNodeList(operands, ZERO);
  }

  //todo
  @Override
  protected SqlNode visitAlterTableArchive(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableUnarchive(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableFileFormat(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterPartitionFileFormat(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterPartitionLocation(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableLocation(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableTouch(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableCompact(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterPartitionMergeFiles(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableMergeFiles(ASTNode node, ParseContext ctx) {
    return visitChildren(node, ctx).get(0);
    // Method content
  }

  @Override
  protected SqlNode visitAlterTableUpdateColumns(ASTNode node, ParseContext ctx) {
    return visitChildren(node, ctx).get(0);
    // Method content
  }

  @Override
  protected SqlNode visitAlterTableRenameCol(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableChangeColAfterPosition(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableAddCols(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableReplaceCols(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableAddConstraint(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableDropConstraint(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableUpdateColStats(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableUpdateStats(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableDropProperties(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableSkewedLocation(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }



  @Override
  protected SqlNode visitAlterTableOwner(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableSetPartSpec(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableConvert(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableExecute(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableDropBranch(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableCreateBranch(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableDropTag(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableCreateTag(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterPartitionUpdateStats(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterPartitionSerializer(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterPartitionSetSerdeProperties(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTableSetSerdeProperties(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterPartitionUnsetSerdeProperties(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitSkewedLocations(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitSkewedLocationList(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitSkewedLocationMap(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterPartitionBuckets(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitBlocking(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitCompactPool(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitRollback(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterPartitionUpdateColStats(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterTablePartColType(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterDatabaseOwner(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterDatabaseLocation(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitAlterDatabaseManagedLocation(ASTNode node, ParseContext ctx) {
    // Method content
    return visitChildren(node, ctx).get(0);
  }



  @Override
  protected SqlNode visitPrimaryKey(ASTNode node, ParseContext ctx) {
    return createColumnConstraint(SqlColumnConstraint.SqlColumnConstraintType.PRIMARY_KEY, node, ctx);
  }

  @Override
  protected SqlNode visitUnique(ASTNode node, ParseContext ctx) {
    return createColumnConstraint(SqlColumnConstraint.SqlColumnConstraintType.UNIQUE, node, ctx);
  }

  @Override
  protected SqlNode visitNotNull(ASTNode node, ParseContext ctx) {
    return createColumnConstraint(SqlColumnConstraint.SqlColumnConstraintType.NOT_NULL, node, ctx);
  }

  @Override
  protected SqlNode visitDefaultValue(ASTNode node, ParseContext ctx) {
    SqlNode defaultValue = visit((ASTNode) node.getChild(0), ctx);

//    // 处理特殊的默认值
//    if (defaultValue instanceof SqlIdentifier) {
//      String defaultValueStr = ((SqlIdentifier) defaultValue).getSimple();
//      switch (defaultValueStr.toUpperCase()) {
//        case "CURRENT_USER":
//          defaultValue = SqlStdOperatorTable.CURRENT_USER.createCall(ZERO);
//          break;
//        case "CURRENT_DATE":
//          defaultValue = SqlStdOperatorTable.CURRENT_DATE.createCall(ZERO);
//          break;
//        case "CURRENT_TIMESTAMP":
//          defaultValue = SqlStdOperatorTable.CURRENT_TIMESTAMP.createCall(ZERO);
//          break;
//        case "NULL":
//          defaultValue = SqlLiteral.createNull(ZERO);
//          break;
//      }
//    }

    return SqlColumnConstraint.createDefault(defaultValue, ZERO);

//    SqlNode defaultValue = visit((ASTNode) node.getChild(0), ctx);
//    return createColumnConstraint(SqlColumnConstraint.SqlColumnConstraintType.DEFAULT, node, ctx, defaultValue);
  }

  @Override
  protected SqlNode visitCheckConstraint(ASTNode node, ParseContext ctx) {
    SqlNode checkExpression = visit((ASTNode) node.getChild(0), ctx);
    return createColumnConstraint(SqlColumnConstraint.SqlColumnConstraintType.CHECK, node, ctx, null, checkExpression);
  }

  private SqlColumnConstraint createColumnConstraint(SqlColumnConstraint.SqlColumnConstraintType type, ASTNode node, ParseContext ctx, SqlNode defaultValue, SqlNode checkExpression) {
    boolean isEnabled = true;
    boolean isValidated = true;
    boolean isRely = true;

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      switch (child.getType()) {
        case HiveParser.TOK_ENABLE:
          isEnabled = true;
          break;
        case HiveParser.TOK_DISABLE:
          isEnabled = false;
          break;
        case HiveParser.TOK_VALIDATE:
          isValidated = true;
          break;
        case HiveParser.TOK_NOVALIDATE:
          isValidated = false;
          break;
        case HiveParser.TOK_RELY:
          isRely = true;
          break;
        case HiveParser.TOK_NORELY:
          isRely = false;
          break;
      }
    }

    return new SqlColumnConstraint(
            SqlLiteral.createSymbol(type, ZERO),
            defaultValue,
            checkExpression,
            SqlLiteral.createBoolean(isEnabled, ZERO),
            SqlLiteral.createBoolean(isValidated, ZERO),
            SqlLiteral.createBoolean(isRely, ZERO),
            ZERO
    );
  }

  // Overloads for convenience
  private SqlColumnConstraint createColumnConstraint(SqlColumnConstraint.SqlColumnConstraintType type, ASTNode node, ParseContext ctx) {
    return createColumnConstraint(type, node, ctx, null, null);
  }

  private SqlColumnConstraint createColumnConstraint(SqlColumnConstraint.SqlColumnConstraintType type, ASTNode node, ParseContext ctx, SqlNode defaultValue) {
    return createColumnConstraint(type, node, ctx, defaultValue, null);
  }


  @Override
  protected SqlNode visitDisable(ASTNode node, ParseContext ctx) {
    return SqlLiteral.createCharString("DISABLE", ZERO);
  }

  @Override
  protected SqlNode visitNoValidate(ASTNode node, ParseContext ctx) {
    return SqlLiteral.createCharString("NOVALIDATE", ZERO);
  }


  @Override
  protected SqlNode visitIfNotExists(ASTNode node, ParseContext ctx) {
    return SqlLiteral.createBoolean(true, ZERO);
  }

  @Override
  protected SqlNode visitTableRowFormat(ASTNode node, ParseContext ctx) {
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitSerdeName(ASTNode node, ParseContext ctx) {
    SqlCharStringLiteral serdeName = null;
    SqlNodeList serdeProperties = null;
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      switch (child.getType()) {
        case HiveParser.StringLiteral:
          serdeName = (SqlCharStringLiteral) visit(child, ctx);
          break;
        case HiveParser.TOK_TABLEPROPERTIES:
          serdeProperties = (SqlNodeList) visit(child, ctx);
          break;
        default:
          System.out.println("parse error unsupported token type"+child.getName());
          break;
      }
    }
    return new SqlRowFormatSerde(ZERO, serdeName, serdeProperties);
  }


  @Override
  protected SqlNode visitTableSerializer(ASTNode node, ParseContext ctx) {
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitTableFileFormat(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodeList = visitChildren(node, ctx);
    return new SqlNodeList(sqlNodeList, ZERO);
  }

  @Override
  protected SqlNode visitFileFormatGeneric(ASTNode node, ParseContext ctx) {
    return new SqlNodeList(Arrays.asList(visitChildren(node, ctx).get(0)), ZERO);
  }

  @Override
  protected SqlNode visitSerdeProps(ASTNode node, ParseContext ctx) {
    return visitChildren(node, ctx).get(0);
  }

  @Override
  protected SqlNode visitTableRowFormatField(ASTNode node, ParseContext ctx) {
    return visitChildren(node, ctx).get(0);
  }


  private SqlIntervalQualifier fromASTIntervalTypeToSqlIntervalQualifier(ASTNode node) {
    switch (node.getType()) {
      case HiveParser.TOK_INTERVAL_DAY_LITERAL:
        return new SqlIntervalQualifier(TimeUnit.DAY, null, ZERO);
      case HiveParser.TOK_INTERVAL_DAY_TIME_LITERAL:
        return new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, ZERO);
      case HiveParser.TOK_INTERVAL_HOUR_LITERAL:
        return new SqlIntervalQualifier(TimeUnit.HOUR, null, ZERO);
      case HiveParser.TOK_INTERVAL_MINUTE_LITERAL:
        return new SqlIntervalQualifier(TimeUnit.MINUTE, null, ZERO);
      case HiveParser.TOK_INTERVAL_MONTH_LITERAL:
        return new SqlIntervalQualifier(TimeUnit.MONTH, null, ZERO);
      case HiveParser.TOK_INTERVAL_SECOND_LITERAL:
        return new SqlIntervalQualifier(TimeUnit.SECOND, null, ZERO);
      case HiveParser.TOK_INTERVAL_YEAR_LITERAL:
        return new SqlIntervalQualifier(TimeUnit.YEAR, null, ZERO);
      case HiveParser.TOK_INTERVAL_YEAR_MONTH_LITERAL:
        return new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, ZERO);
    }
    throw new UnhandledASTTokenException(node);
  }


  private SqlNode createSqlLiteral(ASTNode node) {
    if (node.getType() == HiveParser.StringLiteral) {
      return SqlLiteral.createCharString(node.getText().replaceAll("^'|'$", ""), SqlParserPos.ZERO);
    } else {
      return SqlLiteral.createExactNumeric(node.getText(), SqlParserPos.ZERO);
    }
  }

  @Override
  protected SqlNode visitPartVal(ASTNode node, ParseContext ctx) {
    List<SqlNode> sqlNodeList = visitChildren(node, ctx);
    SqlNode key = sqlNodeList.get(0);
    SqlNode value = sqlNodeList.get(1);
    return new SqlProperty(key, value,ZERO);
  }
/*
*
* Alter DDL
*
* */


  @Override
  protected SqlNode visitAlterTable(ASTNode node, ParseContext ctx) {
    List<SqlNode> operands = visitChildren(node, ctx);
//    SqlNodeList operands = (SqlNodeList) visitChildren(node, ctx);
    return new SqlAlterTable(ZERO, new SqlNodeList(operands, ZERO));


//    /ADDDl/
   /* for (int i = 1; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      switch (child.getType()) {
        case HiveParser.TOK_ALTERTABLE_RENAME:
//          atOptions.operation = AlterTableOperation.RENAME;
//          atOptions.newTableName = (SqlIdentifier) visit((ASTNode) child.getChild(0), ctx);
//          break;
        case HiveParser.TOK_ALTERTABLE_PROPERTIES:
//          atOptions.operation = AlterTableOperation.SET_PROPERTIES;
//          atOptions.properties = (SqlNodeList) visit(child, ctx);
//          break;
        case HiveParser.TOK_ALTERTABLE_SERIALIZER:
//          atOptions.operation = AlterTableOperation.SET_SERDE;
//          atOptions.serdeName = visit((ASTNode) child.getChild(0), ctx);
//          if (child.getChildCount() > 1) {
//            atOptions.serdeProperties = (SqlNodeList) visit((ASTNode) child.getChild(1), ctx);
//          }
          break;
        case HiveParser.TOK_ALTERTABLE_UNSETSERDEPROPERTIES:
//          atOptions.operation = AlterTableOperation.UNSET_SERDE_PROPERTIES;
//          atOptions.serdeProperties = (SqlNodeList) visit((ASTNode) child.getChild(0), ctx);
//          break;
        case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
//
//          break;

        case HiveParser.TOK_ALTERTABLE_SKEWED:
          atOptions.operation = AlterTableOperation.SKEWED;

          break;
        case HiveParser.TOK_ALTERTABLE_ADDPARTS:
          atOptions.ifNotExists=false;
          atOptions.operation = AlterTableOperation.ADD_PARTITION;
          for (Node bucket_child : child.getChildren()) {
            ASTNode ast = (ASTNode) bucket_child;
            if (ast.getType() == HiveParser.TOK_PARTSPEC) {
              atOptions.partitionSpec = new SqlNodeList(visitChildren(ast, ctx), ZERO);
            } else if (ast.getType() == HiveParser.TOK_IFNOTEXISTS) {
              atOptions.ifNotExists=true;
            } else if (ast.getType() == HiveParser.TOK_PARTITIONLOCATION) {
              atOptions.part_location=(SqlCharStringLiteral) visitChildren(ast, ctx).get(0);
            }
          }
          atOptions.ifNotExists = child.getChild(0).getType() == HiveParser.TOK_IFNOTEXISTS;
          break;

        case HiveParser.TOK_ALTERTABLE_DROPPARTS:
          atOptions.operation = SqlAlterTable.AlterTableOperation.DROP_PARTITION;
          atOptions.ifExists = false;
          atOptions.purge = false;
          SqlNodeList partitionSpecs = new SqlNodeList(SqlParserPos.ZERO);

          for (Node child_node : child.getChildren()) {
            ASTNode ast = (ASTNode) child_node;
            switch (ast.getType()) {
              case HiveParser.TOK_PARTSPEC:
                SqlNode partSpec = visit(ast, ctx);
                partitionSpecs.add(partSpec);
                break;
              case HiveParser.TOK_IFEXISTS:
                atOptions.ifExists = true;
                break;
              case HiveParser.KW_PURGE:
                atOptions.purge = true;
                break;
            }
          }
          atOptions.partitionSpecs = partitionSpecs;
          break;
        case HiveParser.TOK_ALTERTABLE_RENAMEPART:
          atOptions.operation = AlterTableOperation.RENAME_PARTITION;
          // 处理子节点
          for (int j = 0; j < child.getChildCount(); j++) {
            ASTNode ast = (ASTNode) child.getChild(j);
            if (ast.getType() == HiveParser.TOK_PARTSPEC) {
              SqlNode partSpec = visit(ast, ctx);
              if (j == 0) {
                // only one partspec allowed in hive part rename
                atOptions.newPartitionSpec = partSpec;
              } else {
                throw new UnsupportedOperationException("Unsupported ALTER TABLE operation: " + child.getText());
              }
            }
          }

          break;

        case HiveParser.TOK_ALTERTABLE_EXCHANGEPARTITION:
          atOptions.operation = AlterTableOperation.EXCHANGE_PARTITION;
          for (Node exchange_node : child.getChildren()) {
            ASTNode ast = (ASTNode) exchange_node;
            switch (ast.getType()) {
              case HiveParser.TOK_PARTSPEC:
                atOptions.partitionSpec = visit(ast, ctx);
                break;
              case HiveParser.TOK_TABNAME:
                atOptions.sourceTable = (SqlIdentifier) visit(ast, ctx);
                break;
            }
          }
          break;
        case HiveParser.TOK_ALTERTABLE_ARCHIVE:
          atOptions.operation = AlterTableOperation.ARCHIVE_PARTITION;
          SqlNodeList archive_partitionSpecs = new SqlNodeList(SqlParserPos.ZERO);

          for (Node child_node : child.getChildren()) {
            ASTNode ast = (ASTNode) child_node;
            if (ast.getType() == HiveParser.TOK_PARTSPEC) {
              SqlNode partSpec = visit(ast, ctx);
              archive_partitionSpecs.add(partSpec);
            }
          }

          atOptions.partitionSpec = archive_partitionSpecs;
          break;
        case HiveParser.TOK_ALTERTABLE_UNARCHIVE:
          atOptions.operation = AlterTableOperation.UNARCHIVE_PARTITION;
          SqlNodeList unarchive_partitionSpecs = new SqlNodeList(SqlParserPos.ZERO);

          for (Node child_node : child.getChildren()) {
            ASTNode ast = (ASTNode) child_node;
            if (ast.getType() == HiveParser.TOK_PARTSPEC) {
              SqlNode partSpec = visit(ast, ctx);
              unarchive_partitionSpecs.add(partSpec);
            }
          }

          atOptions.partitionSpec = unarchive_partitionSpecs;
          break;
        case HiveParser.TOK_ALTERTABLE_FILEFORMAT:
          atOptions.operation = AlterTableOperation.SET_FILE_FORMAT;
          for (Node child_node : child.getChildren()) {
            ASTNode ast = (ASTNode) child_node;
            if (ast.getType() == HiveParser.TOK_FILEFORMAT_GENERIC) {
              atOptions.fileFormat = visit(child, ctx);
            }else if(ast.getType() == HiveParser.TOK_TABLEFILEFORMAT){
              List<SqlNode> formats =  visitChildren(child, ctx);
              atOptions.inputFmt=(SqlCharStringLiteral) formats.get(0);
              atOptions.outFmt=(SqlCharStringLiteral) formats.get(1);
              atOptions.serdeCls=(SqlCharStringLiteral) formats.get(2);
              if (formats.size()==5){
                atOptions.inDriver=(SqlCharStringLiteral) formats.get(3);
                atOptions.outDriver=(SqlCharStringLiteral) formats.get(4);
              }else {
                throw new UnsupportedOperationException("Unsupported ALTER TABLE operation: " + child.getText());
              }
            }
          }
          break;
        case HiveParser.TOK_ALTERPARTITION_FILEFORMAT:
          atOptions.operation = AlterTableOperation.SET_PARTITION_FILE_FORMAT;
          for (Node child_node : child.getChildren()) {
            ASTNode ast = (ASTNode) child_node;
            if (ast.getType() == HiveParser.TOK_FILEFORMAT_GENERIC) {
              atOptions.fileFormat = visit(child, ctx);
            }else if(ast.getType() == HiveParser.TOK_TABLEFILEFORMAT){
              List<SqlNode> formats =  visitChildren(child, ctx);
              atOptions.inputFmt=(SqlCharStringLiteral) formats.get(0);
              atOptions.outFmt=(SqlCharStringLiteral) formats.get(1);
              atOptions.serdeCls=(SqlCharStringLiteral) formats.get(2);
              if (formats.size()==5){
                atOptions.inDriver=(SqlCharStringLiteral) formats.get(3);
                atOptions.outDriver=(SqlCharStringLiteral) formats.get(4);
              }else {
                throw new UnsupportedOperationException("Unsupported ALTER TABLE operation: " + child.getText());
              }
            }
          }
          break;

        case HiveParser.TOK_PARTSPEC:
          atOptions.partitionSpec = visit(child, ctx);
        case HiveParser.TOK_ALTERPARTITION_LOCATION:
          atOptions.operation = AlterTableOperation.SET_PARTITION_LOCATION;
          atOptions.location = visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_LOCATION:
          atOptions.operation = AlterTableOperation.SET_LOCATION;
          atOptions.location = visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_TOUCH:
          atOptions.operation = AlterTableOperation.TOUCH;

          if (child.getChildCount() > 0) {
            SqlNodeList touch_partitionSpecs = new SqlNodeList(SqlParserPos.ZERO);
            for (Node child_node : child.getChildren()) {
              ASTNode ast = (ASTNode) child_node;
              if (ast.getType() == HiveParser.TOK_PARTSPEC) {
                SqlNode partSpec = visit(ast, ctx);
                touch_partitionSpecs.add(partSpec);
              }
            }
            atOptions.partitionSpecs = touch_partitionSpecs;
          }
          break;
        //todo
        case HiveParser.TOK_ALTERTABLE_COMPACT:
          atOptions.operation = AlterTableOperation.COMPACT;
          atOptions.compactionType = visit((ASTNode) child.getChild(0), ctx);
          if (child.getChildCount() > 1) {
            atOptions.compactionProps = (SqlNodeList) visit((ASTNode) child.getChild(1), ctx);
          }
          break;
        case HiveParser.TOK_ALTERPARTITION_MERGEFILES:

        case HiveParser.TOK_ALTERTABLE_MERGEFILES:
          atOptions.operation = AlterTableOperation.MERGE_FILES;
          if (child.getChildCount() > 0) {
            atOptions.partitionSpec = visit((ASTNode) child.getChild(0), ctx);
          }
          break;
        case HiveParser.TOK_ALTERTABLE_UPDATECOLUMNS:
          atOptions.operation = AlterTableOperation.UPDATE_COLUMNS;
          if (child.getChildCount() > 0) {
            atOptions.partitionSpec = visit((ASTNode) child.getChild(0), ctx);
          }
          break;
        //->^(TOK_ALTERTABLE_RENAMECOL $oldName $newName colType $comment? alterColumnConstraint? alterStatementChangeColPosition? restrictOrCascade?)
        case HiveParser.TOK_ALTERTABLE_RENAMECOL:
          atOptions.operation = AlterTableOperation.RENAME_COLUMN;
          atOptions.oldColName = (SqlIdentifier) visit((ASTNode) child.getChild(0), ctx);
          atOptions.newColName = (SqlIdentifier) visit((ASTNode) child.getChild(1), ctx);
          atOptions.newColType = (SqlDataTypeSpec) visit((ASTNode) child.getChild(2), ctx);
          if (child.getChildCount() > 3) {
            atOptions.colComment = visit((ASTNode) child.getChild(3), ctx);
          }
          break;
        case HiveParser.TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION:
          atOptions.afterCol = (SqlIdentifier) visit((ASTNode) child.getChild(0), ctx);
        case HiveParser.TOK_ALTERTABLE_ADDCOLS:
          atOptions.operation = AlterTableOperation.ADD_COLUMNS;
          atOptions.newColumns = (SqlNodeList) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
          atOptions.operation = AlterTableOperation.REPLACE_COLUMNS;
          atOptions.newColumns = (SqlNodeList) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_ADDCONSTRAINT:
          // This case is not directly supported in the given AlterTableOperation enum
          // You might need to add a new operation type for this
          break;
        case HiveParser.TOK_ALTERTABLE_DROPCONSTRAINT:
          atOptions.operation = AlterTableOperation.DROP_CONSTRAINT;
          atOptions.constraintName = (SqlIdentifier) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_UPDATECOLSTATS:
          atOptions.operation = AlterTableOperation.UPDATE_COLUMN_STATS;
          atOptions.columnStats = (SqlNodeList) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_UPDATESTATS:
          atOptions.operation = AlterTableOperation.UPDATE_TABLE_STATS;
          atOptions.tableStats = (SqlNodeList) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_DROPPROPERTIES:
          atOptions.operation = AlterTableOperation.DROP_PROPERTIES;
          atOptions.dropProperties = (SqlNodeList) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_SKEWED_LOCATION:
          atOptions.operation = AlterTableOperation.SET_SKEWED_LOCATION;
          atOptions.skewedLocations = (SqlNodeList) visit((ASTNode) child.getChild(0), ctx);
          break;
        // KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)? KW_INTO num=Number KW_BUCKETS -> ^(TOK_ALTERTABLE_BUCKETS $bucketCols $sortCols? $num)
        case HiveParser.TOK_ALTERTABLE_BUCKETS:
          atOptions.operation = SqlAlterTable.AlterTableOperation.SET_BUCKETS;
          atOptions.clusterCols = (SqlNodeList) visit((ASTNode) child.getChild(0), ctx);
          atOptions.buckets = visit((ASTNode) child.getChild(1), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_OWNER:
          atOptions.operation = AlterTableOperation.SET_OWNER;
          atOptions.newOwner = (SqlIdentifier) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_SETPARTSPEC:
          atOptions.operation = AlterTableOperation.SET_PARTITION_SPEC;
          atOptions.partitionSpec = visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_CONVERT:
          atOptions.operation = AlterTableOperation.CONVERT_TABLE;
          atOptions.convertType = visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_EXECUTE:
          atOptions.operation = AlterTableOperation.EXECUTE;
          atOptions.executeStatement = visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_DROP_BRANCH:
          atOptions.operation = AlterTableOperation.DROP_BRANCH;
          atOptions.branchName = (SqlIdentifier) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_CREATE_BRANCH:
          atOptions.operation = AlterTableOperation.CREATE_BRANCH;
          atOptions.branchName = (SqlIdentifier) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_DROP_TAG:
          atOptions.operation = AlterTableOperation.DROP_TAG;
          atOptions.tagName = (SqlIdentifier) visit((ASTNode) child.getChild(0), ctx);
          break;
        case HiveParser.TOK_ALTERTABLE_CREATE_TAG:
          atOptions.operation = AlterTableOperation.CREATE_TAG;
          atOptions.tagName = (SqlIdentifier) visit((ASTNode) child.getChild(0), ctx);
          break;


        case HiveParser.TOK_ALTERPARTITION_UPDATESTATS:
        case HiveParser.TOK_ALTERPARTITION_SERIALIZER:
        case HiveParser.TOK_ALTERPARTITION_SETSERDEPROPERTIES:
        case HiveParser.TOK_ALTERTABLE_SETSERDEPROPERTIES:
        case HiveParser.TOK_ALTERPARTITION_UNSETSERDEPROPERTIES:
        case HiveParser.TOK_SKEWED_LOCATIONS:
        case HiveParser.TOK_SKEWED_LOCATION_LIST:
        case HiveParser.TOK_SKEWED_LOCATION_MAP:
        case HiveParser.TOK_ALTERPARTITION_BUCKETS:
        case HiveParser.TOK_BLOCKING:
        case HiveParser.TOK_COMPACT_POOL:
        case HiveParser.KW_ROLLBACK:
        case HiveParser.TOK_ALTERPARTITION_UPDATECOLSTATS:
        case HiveParser.TOK_ALTERTABLE_PARTCOLTYPE:
        case HiveParser.TOK_ALTERDATABASE_OWNER:
        case HiveParser.TOK_ALTERDATABASE_LOCATION:
        case HiveParser.TOK_ALTERDATABASE_MANAGEDLOCATION:

        default:
          throw new UnsupportedOperationException("Unsupported ALTER TABLE operation: " + child.getText());
      }
    }*/
  }

  private List<SqlNode> buildOperandList(AlterTableOptions options) {
    List<SqlNode> operands = new ArrayList<>();
    switch (options.operation) {
      case RENAME:
        operands.add(options.newTableName);
        break;
      case SET_PROPERTIES:
        operands.add(options.properties);
        break;
      case SET_SERDE:
        operands.add(options.serdeName);
        if (options.serdeProperties != null) {
          operands.add(options.serdeProperties);
        }
        break;
      case UNSET_SERDE_PROPERTIES:
        operands.add(options.serdeProperties);
        break;
      case CLUSTER_SORT:
        operands.add(options.clusterCols);
        if (options.sortCols != null) {
          operands.add(options.sortCols);
        }
        operands.add(options.buckets);
        break;
      case SKEWED:
        if (options.skewedCols != null) {
          operands.add(options.skewedCols);
          operands.add(options.skewedValues);
          operands.add(SqlLiteral.createBoolean(options.storedAsDirs, ZERO));
        }
        break;
      case ADD_PARTITION:
        operands.add(SqlLiteral.createBoolean(options.ifNotExists, ZERO));
        operands.add(options.partitionSpec);
        if (options.location != null) {
          operands.add(options.location);
        }
        break;
      case DROP_PARTITION:
        operands.add(SqlLiteral.createBoolean(options.ifExists, ZERO));
        operands.add(options.partitionSpec);
        operands.add(SqlLiteral.createBoolean(options.purge, ZERO));
        break;
      case RENAME_PARTITION:
        operands.add(options.oldPartitionSpec);
        operands.add(options.newPartitionSpec);
        break;
      case EXCHANGE_PARTITION:
        operands.add(options.partitionSpec);
        operands.add(options.sourceTable);
        break;
      case ARCHIVE_PARTITION:
      case UNARCHIVE_PARTITION:
        operands.add(options.partitionSpec);
        break;
      case SET_FILE_FORMAT:
        if (options.partitionSpec != null) {
          operands.add(options.partitionSpec);
        }
        operands.add(options.fileFormat);
        break;
      case SET_LOCATION:
        if (options.partitionSpec != null) {
          operands.add(options.partitionSpec);
        }
        operands.add(options.location);
        break;
      case TOUCH:
        if (options.partitionSpec != null) {
          operands.add(options.partitionSpec);
        }
        break;
      case COMPACT:
        operands.add(options.compactionType);
        if (options.compactionProps != null) {
          operands.add(options.compactionProps);
        }
        break;
      case CONCATENATE:
        if (options.partitionSpec != null) {
          operands.add(options.partitionSpec);
        }
        break;
      case UPDATE_COLUMNS:
        if (options.partitionSpec != null) {
          operands.add(options.partitionSpec);
        }
        break;
      case CHANGE_COLUMN:
        if (options.partitionSpec != null) {
          operands.add(options.partitionSpec);
        }
        operands.add(options.oldColName);
        operands.add(options.newColName);
        operands.add(options.newColType);
        operands.add(options.colComment);
        operands.add(options.afterCol);
        break;
      case ADD_COLUMNS:
      case REPLACE_COLUMNS:
        if (options.partitionSpec != null) {
          operands.add(options.partitionSpec);
        }
        operands.add(options.newColumns);
        break;
      case CHANGE_PARTITION_COLUMN_TYPE:
        operands.add(options.partitionSpec);
        operands.add(options.newColType);
        break;
      case DROP_CONSTRAINT:
        operands.add(options.constraintName);
        break;
      case UPDATE_COLUMN_STATS:
        operands.add(options.columnStats);
        break;
      case UPDATE_TABLE_STATS:
        operands.add(options.tableStats);
        break;
      case DROP_PROPERTIES:
        operands.add(options.dropProperties);
        break;
      case SET_SKEWED_LOCATION:
        operands.add(options.skewedLocations);
        break;
      case MERGE_FILES:
        if (options.partitionSpec != null) {
          operands.add(options.partitionSpec);
        }
        break;
      case SET_BUCKETS:
        operands.add(options.clusterCols);
        operands.add(options.buckets);
        break;
      case SET_OWNER:
        operands.add(options.newOwner);
        break;
      case SET_PARTITION_SPEC:
        operands.add(options.partitionSpec);
        break;
      case CONVERT_TABLE:
        operands.add(options.convertType);
        break;
      case EXECUTE:
        operands.add(options.executeStatement);
        break;
      case DROP_BRANCH:
      case CREATE_BRANCH:
        operands.add(options.branchName);
        break;
      case DROP_TAG:
      case CREATE_TAG:
        operands.add(options.tagName);
        break;
      case RENAME_COLUMN:
        operands.add(options.oldColName);
        operands.add(options.newColName);
        operands.add(options.newColType);
        if (options.colComment != null) {
          operands.add(options.colComment);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported ALTER TABLE operation: " + options.operation);
    }
    return operands;
  }



  //终结符，叶子节点，不可再分
  @Override
  protected SqlNode visitIntervalLiteral(ASTNode node, ParseContext ctx) {
    // Hive Antlr Tree looks like the following:
    //   ASTNode(token.type = TOK_INTERVAL_DAY_LITERAL, token.text="'28'")
    // Calcite SqlNode Tree looks like the following:
    //   SqlIntervalLiteral(1 /* sign */, "28" /* unquotedText */, SqlIntervalQualifier, SqlParserPos)

    SqlIntervalQualifier intervalQualifier = fromASTIntervalTypeToSqlIntervalQualifier(node);

    String text = node.getToken().getText();
    String unquotedText = text.replaceAll("['\"]", "");

    return SqlLiteral.createInterval(1, unquotedText, intervalQualifier, ZERO);
  }

  static class ParseContext {
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<Table> hiveTable;
    SqlNodeList keywords;
    SqlNode from;
    SqlNodeList selects;
    SqlNode where;
    SqlNodeList grpBy;
    SqlNode having;
    SqlNode fetch;
    SqlNodeList orderBy;

    ParseContext(@Nullable Table hiveTable) {
      this.hiveTable = Optional.ofNullable(hiveTable);
    }

    Optional<Table> getHiveTable() {
      return hiveTable;
    }
  }


  static class CreateTableOptions {
    SqlIdentifier name;
    SqlNodeList columnList;
    SqlNode query;
    boolean ifNotExists;
    boolean isExternal;
    boolean isTemporary;
    SqlCharStringLiteral comment;
    SqlNodeList partitionByList;
    SqlNodeList clusterByList;
    SqlNode bucketNum;
    SqlNodeList sortedByList;
    SqlNode rowFormat;
    SqlIdentifier storedAs;
    SqlCharStringLiteral location;
    SqlNodeList propertyList;
  }

  static class AlterTableOptions {
    SqlIdentifier tableName;
    SqlAlterTable.AlterTableOperation operation;
    SqlIdentifier newTableName;
    SqlNodeList properties;
    SqlNode serdeName;
    SqlNodeList serdeProperties;
    SqlNodeList clusterCols;
    SqlNodeList sortCols;
    SqlNode buckets;
    SqlNodeList skewedCols;
    SqlNodeList skewedValues;
    boolean storedAsDirs;
    SqlNode partitionSpec;
    SqlNodeList partitionSpecs;
    SqlNode oldPartitionSpec;
    SqlNode newPartitionSpec;
    SqlNode location;
    boolean ifExists;
    boolean ifNotExists;
    boolean purge;
    SqlIdentifier sourceTable;
    SqlNode fileFormat;
    SqlCharStringLiteral inputFmt;
    SqlCharStringLiteral outFmt;
    SqlCharStringLiteral serdeCls;
    SqlCharStringLiteral inDriver;
    SqlCharStringLiteral outDriver;
    SqlNode compactionType;
    SqlNodeList compactionProps;
    SqlIdentifier oldColName;
    SqlIdentifier newColName;
    SqlDataTypeSpec newColType;
    SqlNode colComment;
    SqlIdentifier afterCol;
    SqlNodeList newColumns;
    SqlIdentifier constraintName;
    SqlNodeList columnStats;
    SqlNodeList tableStats;
    SqlNodeList dropProperties;
    SqlNodeList skewedLocations;
    SqlIdentifier newOwner;
    SqlNode convertType;
    SqlNode executeStatement;
    SqlIdentifier branchName;
    SqlIdentifier tagName;
    boolean not_clusterd;
    boolean not_sorted;
    boolean skewedColsValuePair;
    SqlCharStringLiteral part_location;
  }

//  static class CreateTableOptions {
//    SqlIdentifier name;
//    SqlNodeList columnList;
//    SqlNode query;
//    boolean ifNotExists;
//    SqlNode tableSerializer;
//    SqlNodeList tableFileFormat;
//    SqlCharStringLiteral tableRowFormat;
//  }
}
