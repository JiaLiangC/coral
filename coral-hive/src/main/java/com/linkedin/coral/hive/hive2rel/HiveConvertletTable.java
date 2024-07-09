/**
 * Copyright 2018-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.hive.hive2rel;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import com.linkedin.coral.common.functions.FunctionFieldReferenceOperator;



/**
 *  ReflectiveConvertletTable 是一个实现 SqlRexConvertletTable 接口的类，用于通过反射机制调用特定类型的方法来进行 SQL 到 Rex（Relational Expression）节点的转换。
 * 作用ReflectiveConvertletTable 的主要作用是在 SQL 解析和优化的过程中，根据给定的 SqlNode（SQL 语句节点）或者 SqlCall（SQL 调用）生成相应的 RexNode（关系表达式节点）。
 * 具体来说，它通过反射机制调用某些特定格式的方法来实现这一转换过程。ReflectiveConvertletTable 通过反射查找并调用符合以下签名的方法
 * public RexNode convertXxx(ConvertletContext context, SqlNode node)
 * public RexNode convertXxx(ConvertletContext context, SqlOperator operator, SqlCall call)
 * 通过利用反射机制，ReflectiveConvertletTable 可以动态地调用不同名称和签名的方法。这种灵活性意味着可以很容易地扩展新的 SQL 转换方法，而不需要修改现有的转换表逻辑。
 * 假设我们有一个 ReflectiveConvertletTable 子类 MyConvertletTable，并且我们希望将 SQL 加法转换为 Rex 节点：public class MyConvertletTable extends ReflectiveConvertletTable {
 *     public RexNode convertAdd(ConvertletContext context, SqlOperator operator, SqlCall call) {
 *         // 实现具体的转换逻辑
 *     }
 * }在上述示例中，当 MyConvertletTable 遇到一个 SQL 加法操作时，会通过反射查找并调用 convertAdd 方法，从而完成 SQL 到 Rex 的转换.
*/



/**
 * ConvertletTable for Hive Operators
 * @see ReflectiveConvertletTable documentation for method naming and visibility rules
 */
public class HiveConvertletTable extends ReflectiveConvertletTable {


  //把自定义的 FunctionFieldReferenceOperator 转为rexnode
  @SuppressWarnings("unused")
  public RexNode convertFunctionFieldReferenceOperator(SqlRexContext cx, FunctionFieldReferenceOperator op,
      SqlCall call) {
    RexNode funcExpr = cx.convertExpression(call.operand(0));
    String fieldName = FunctionFieldReferenceOperator.fieldNameStripQuotes(call.operand(1));
    return cx.getRexBuilder().makeFieldAccess(funcExpr, fieldName, false);
  }

  @SuppressWarnings("unused")
  public RexNode convertCast(SqlRexContext cx, SqlCastFunction cast, SqlCall call) {
    final SqlNode left = call.operand(0);
    RexNode leftRex = cx.convertExpression(left);
    SqlDataTypeSpec dataType = call.operand(1);
    RelDataType castType = dataType.deriveType(cx.getValidator(), true);
    // can not call RexBuilder.makeCast() since that optimizes to remove the cast
    // we don't want to remove the cast
    return cx.getRexBuilder().makeAbstractCast(castType, leftRex);
  }

  @Override
  public SqlRexConvertlet get(SqlCall call) {
    SqlRexConvertlet convertlet = super.get(call);
    return convertlet != null ? convertlet : StandardConvertletTable.INSTANCE.get(call);
  }
}
