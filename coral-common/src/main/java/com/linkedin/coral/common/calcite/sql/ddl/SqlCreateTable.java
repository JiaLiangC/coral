/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.ddl;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableNullableList;

import com.linkedin.coral.common.calcite.sql.ExtendedSqlNode;
import com.linkedin.coral.common.calcite.sql.SqlConstraintValidator;
import com.linkedin.coral.common.calcite.sql.SqlUnparseUtils;
import com.linkedin.coral.common.calcite.sql.ddl.SqlTableColumn.SqlComputedColumn;
import com.linkedin.coral.common.calcite.sql.ddl.SqlTableColumn.SqlRegularColumn;
import com.linkedin.coral.common.calcite.sql.ddl.constraint.SqlTableConstraint;
import com.linkedin.coral.common.calcite.sql.error.SqlValidateException;
import org.apache.derby.iapi.types.SQLBoolean;


/** CREATE TABLE DDL sql call. */
public class SqlCreateTable extends SqlCreate implements ExtendedSqlNode {

//  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);
public static final SqlSpecialOperator OPERATOR =
        new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE, 32, false, ReturnTypes.BOOLEAN, null, null) {
          @Override
          public SqlCall createCall(@Nullable final SqlLiteral functionQualifier,
                                    final SqlParserPos pos,
                                    final @Nullable SqlNode... operands) {
            if (operands.length < 19) {
              throw new IllegalArgumentException("Invalid number of operands for CREATE TABLE");
            }

            SqlIdentifier tableName = (SqlIdentifier) operands[0];
            SqlNodeList columnList = (SqlNodeList) operands[1];
            List<SqlNode> nodeList = ((SqlNodeList) operands[2]).getList();
            List<SqlTableConstraint> tableConstraints = new ArrayList<>();

            for (SqlNode node : nodeList) {
              if (node instanceof SqlTableConstraint) {
                tableConstraints.add((SqlTableConstraint) node);
              }
            }

            SqlNodeList propertyList = (SqlNodeList) operands[3];
            SqlNodeList partitionByList = (SqlNodeList) operands[4];
            SqlNodeList clusterByList = (SqlNodeList) operands[5];
            SqlNode bucketNum = operands[6];

            SqlNodeList sortedByList = (SqlNodeList) operands[7];
            SqlNodeList skewedByList = (SqlNodeList) operands[8];
            SqlNode storedAs = operands[9];
            SqlNode storedBy = operands[10];
            SqlCharStringLiteral location = (SqlCharStringLiteral) operands[11];
            SqlNode rowFormat = operands[12];
            SqlCharStringLiteral comment = (SqlCharStringLiteral) operands[13];
            final boolean isTemporary = operands[14] != null && ((SqlLiteral) operands[14]).booleanValue();
            boolean isExternal = operands[15] != null && ((SqlLiteral) operands[15]).booleanValue();
            boolean isTransactional = operands[16] != null && ((SqlLiteral) operands[16]).booleanValue();
            boolean ifNotExists = operands[17] != null && ((SqlLiteral) operands[17]).booleanValue();
            SqlNode query = operands[18];

            return new SqlCreateTable(pos, tableName, columnList, tableConstraints, propertyList,
                    partitionByList, clusterByList,bucketNum, sortedByList, skewedByList, storedAs, storedBy,
                    location, rowFormat, comment, isTemporary, isExternal, isTransactional, ifNotExists, query);
          }
        };


  private final SqlIdentifier tableName;

  public final @Nullable SqlNode query;
  private final @Nullable SqlNodeList columnList;

  private final SqlNodeList propertyList;

  private final List<SqlTableConstraint> tableConstraints;

  private final SqlNodeList partitionByList;
  private final SqlNodeList clusterByList;

  private final SqlNodeList sortedByList;

  private final SqlNodeList skewedByList;
  private final SqlNode storedAs;
  private final SqlNode storedBy;
  private final SqlNode rowFormat;

  private final SqlCharStringLiteral comment;

  private final SqlCharStringLiteral location;

  private final boolean isExternal ;
  private final boolean isTemporary ;
  private final boolean isTransactional ;
  private final SqlNode bucketNum;


  public SqlCreateTable(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList columnList,
      List<SqlTableConstraint> tableConstraints, SqlNodeList propertyList, SqlNodeList partitionKeyList,
      SqlNodeList clusterByList,SqlNode bucketNum, SqlNodeList sortedByList, SqlNodeList skewedByList, SqlNode storedAs, SqlNode storedBy,
      SqlCharStringLiteral location, SqlNode rowFormat, @Nullable SqlCharStringLiteral comment, boolean isTemporary,
      boolean isExternal, boolean isTransactional, boolean ifNotExists, @Nullable SqlNode query) {
    this(OPERATOR, pos, tableName, columnList, tableConstraints, propertyList, partitionKeyList, clusterByList,bucketNum,
        sortedByList, skewedByList, storedAs, storedBy, location, rowFormat, comment, isTemporary, isExternal,
        isTransactional, ifNotExists, query);
  }

  protected SqlCreateTable(SqlSpecialOperator operator, SqlParserPos pos, SqlIdentifier tableName,
      SqlNodeList columnList, List<SqlTableConstraint> tableConstraints, SqlNodeList propertyList,
      SqlNodeList partitionKeyList, SqlNodeList clusterByList,SqlNode bucketNum, SqlNodeList sortedByList, SqlNodeList skewedByList,
      SqlNode storedAs, SqlNode storedBy, SqlCharStringLiteral location, SqlNode rowFormat,
      @Nullable SqlCharStringLiteral comment, boolean isTemporary, boolean isExternal, boolean isTransactional,
      boolean ifNotExists, @Nullable SqlNode query) {
    super(operator, pos, false, ifNotExists);
    this.tableName = requireNonNull(tableName, "tableName should not be null");
    this.columnList = requireNonNull(columnList, "columnList should not be null");
    this.tableConstraints = tableConstraints;
    this.propertyList = propertyList;
    this.partitionByList = partitionKeyList;
    this.clusterByList = clusterByList;
    this.bucketNum = bucketNum;
    this.sortedByList = sortedByList;
    this.skewedByList = skewedByList;
    this.storedAs = storedAs;
    this.storedBy = storedBy;
    this.location = location;
    this.rowFormat = rowFormat;
    this.comment = comment;
    this.isTemporary = isTemporary;
    this.isExternal = isExternal;
    this.isTransactional = isTransactional;
    this.query = query;
  }

  @Override
  public @Nonnull SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public @Nonnull List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableName, columnList, new SqlNodeList(tableConstraints, ZERO),
        propertyList, partitionByList, clusterByList,bucketNum, sortedByList, skewedByList, storedAs, storedBy, location,
        rowFormat, comment,SqlLiteral.createBoolean(isTemporary,ZERO),SqlLiteral.createBoolean(isExternal,ZERO),SqlLiteral.createBoolean(isTransactional,ZERO),SqlLiteral.createBoolean(ifNotExists,ZERO), query);
  }

  public SqlIdentifier getTableName() {
    return tableName;
  }

  public SqlNode getBucketNum() {
    return bucketNum;
  }

  public SqlNodeList getColumnList() {
    return columnList;
  }

  public SqlNodeList getPropertyList() {
    return propertyList;
  }

  public SqlNodeList getPartitionByList() {
    return partitionByList;
  }

  public List<SqlTableConstraint> getTableConstraints() {
    return tableConstraints;
  }

  @Nullable
  public SqlNode getQuery() {
    return query;
  }

  public SqlNodeList getClusterByList() {
    return clusterByList;
  }

  public SqlNodeList getSortedByList() {
    return sortedByList;
  }

  public SqlNodeList getSkewedByList() {
    return skewedByList;
  }

  public SqlNode getStoredAs() {
    return storedAs;
  }

  public SqlNode getStoredBy() {
    return storedBy;
  }

  public SqlNode getRowFormat() {
    return rowFormat;
  }

  public SqlCharStringLiteral getLocation() {
    return location;
  }

  public Optional<SqlCharStringLiteral> getComment() {
    return Optional.ofNullable(comment);
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public boolean isTemporary() {
    return isTemporary;
  }

  public boolean isTransactional() {
    return isTransactional;
  }

  public boolean isExternal() {
    return isExternal;
  }

  @Override
  public void validate() throws SqlValidateException {
    //SqlConstraintValidator.validateAndChangeColumnNullability(tableConstraints, columnList);
  }

  public boolean hasRegularColumnsOnly() {
    for (SqlNode column : columnList) {
      final SqlTableColumn tableColumn = (SqlTableColumn) column;
      if (!(tableColumn instanceof SqlRegularColumn)) {
        return false;
      }
    }
    return true;
  }

  /** Returns the column constraints plus the table constraints. */
  public List<SqlTableConstraint> getFullConstraints() {
    return SqlConstraintValidator.getFullConstraints(tableConstraints, columnList);
  }

  /**
   * Returns the projection format of the DDL columns(including computed columns). i.e. the
   * following DDL:
   *
   * <pre>
   *   create table tbl1(
   *     col1 int,
   *     col2 varchar,
   *     col3 as to_timestamp(col2)
   *   ) with (
   *     'connector' = 'csv'
   *   )
   * </pre>
   *
   * <p>is equivalent with query "col1, col2, to_timestamp(col2) as col3", caution that the
   * "computed column" operands have been reversed.
   */
  public String getColumnSqlString() {
    SqlPrettyWriter writer = new SqlPrettyWriter(
        //todo upgrade calcite version
        SqlPrettyWriter.config().withDialect(AnsiSqlDialect.DEFAULT).withAlwaysUseParentheses(true)
            .withSelectListItemsOnSeparateLines(false).withIndentation(0));
    writer.startList("", "");
    for (SqlNode column : columnList) {
      writer.sep(",");
      SqlTableColumn tableColumn = (SqlTableColumn) column;
      if (tableColumn instanceof SqlComputedColumn) {
        SqlComputedColumn computedColumn = (SqlComputedColumn) tableColumn;
        computedColumn.getExpr().unparse(writer, 0, 0);
        writer.keyword("AS");
      }
      tableColumn.getName().unparse(writer, 0, 0);
    }

    return writer.toString();
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (isTemporary()) {
      writer.keyword("TEMPORARY");
    }
    if (isExternal()) {
      writer.keyword("EXTERNAL");
    }

    writer.keyword("TABLE");
    if (isIfNotExists()) {
      writer.keyword("IF NOT EXISTS");
    }
    tableName.unparse(writer, leftPrec, rightPrec);
    if (columnList.size() > 0 || tableConstraints.size() > 0) {
      SqlUnparseUtils.unparseTableSchema(writer, leftPrec, rightPrec, columnList, tableConstraints);
    }

    if (comment != null) {
      writer.newlineAndIndent();
      writer.keyword("COMMENT");
      comment.unparse(writer, leftPrec, rightPrec);
    }

    if (this.partitionByList !=null && this.partitionByList.size() > 0) {
      writer.newlineAndIndent();
      writer.keyword("PARTITIONED BY");
      SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
      this.partitionByList.unparse(writer, leftPrec, rightPrec);
      writer.endList(partitionedByFrame);
      writer.newlineAndIndent();
    }

    if (clusterByList != null && clusterByList.size() > 0) {
      writer.newlineAndIndent();
      writer.keyword("CLUSTERED BY");
      SqlWriter.Frame clusteredByFrame = writer.startList("(", ")");
      clusterByList.unparse(writer, leftPrec, rightPrec);
      writer.endList(clusteredByFrame);

      if (sortedByList != null && sortedByList.size() > 0) {
        writer.keyword("SORTED BY");
        SqlWriter.Frame sortedByFrame = writer.startList("(", ")");
        sortedByList.unparse(writer, leftPrec, rightPrec);
        writer.endList(sortedByFrame);
      }

      if (bucketNum != null) {
        writer.keyword("INTO");
        bucketNum.unparse(writer, leftPrec, rightPrec);
        writer.keyword("BUCKETS");
      }

      writer.newlineAndIndent();
    }

    if (skewedByList != null && skewedByList.size() > 0) {
      writer.keyword("SKEWED BY");
      SqlWriter.Frame skewedByFrame = writer.startList("(", ")");
      skewedByList.unparse(writer, leftPrec, rightPrec);
      writer.endList(skewedByFrame);
      writer.newlineAndIndent();
    }

    if (rowFormat !=null ){
      rowFormat.unparse(writer, leftPrec, rightPrec);
    }

    if (storedAs !=null){
      writer.keyword("STORED AS");
      storedAs.unparse(writer, leftPrec, rightPrec);
    }

    if (storedBy !=null){
      storedBy.unparse(writer, leftPrec, rightPrec);
    }

    if (location !=null){
      writer.keyword("LOCATION");
      location.unparse(writer, leftPrec, rightPrec);
    }

    if (this.propertyList !=null && this.propertyList.size() > 0) {
      writer.keyword("TBLPROPERTIES");
      SqlWriter.Frame withFrame = writer.startList("(", ")");
      for (SqlNode property : propertyList) {
        SqlUnparseUtils.printIndent(writer);
        property.unparse(writer, leftPrec, rightPrec);
      }
      writer.newlineAndIndent();
      writer.endList(withFrame);
    }
    if (query != null) {
      writer.newlineAndIndent();
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, leftPrec, rightPrec);
    }
  }

  /** Table creation context. */
  public static class TableCreationContext {
    public List<SqlNode> columnList = new ArrayList<>();
    public List<SqlTableConstraint> constraints = new ArrayList<>();
  }

  public String[] fullTableName() {
    return tableName.names.toArray(new String[0]);
  }
}