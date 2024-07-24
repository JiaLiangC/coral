/**
 * Copyright 2022-2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.coralservice.apps.transformer;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;


/**
 * Transformer to convert SqlCall from array[i] to array[i+1] to ensure array indexes start at 1.
 */
public class ShiftArrayIndexTransformer extends SqlCallTransformer {

  private static final String ITEM_OPERATOR = "ITEM";

  public ShiftArrayIndexTransformer() {
    super(HiveSqlDialect.DEFAULT, SparkSqlDialect.DEFAULT);
  }

  @Override
  public boolean condition(SqlCall sqlCall) {
//    if (ITEM_OPERATOR.equalsIgnoreCase(sqlCall.getOperator().getName())) {
//      final SqlNode columnNode = sqlCall.getOperandList().get(0);
//      return deriveRelDatatype(columnNode) instanceof ArraySqlType;
//    }
    return false;
  }

  @Override
  public SqlCall transform(SqlCall sqlCall) {
    final SqlNode itemNode = sqlCall.getOperandList().get(1);
    SqlNode newIndex;
    newIndex = SqlStdOperatorTable.PLUS.createCall(itemNode.getParserPosition(), itemNode,
        SqlNumericLiteral.createExactNumeric("1", SqlParserPos.ZERO));

    // Create new object instead of modifying the old SqlCall to avoid transforming the same object
    // multiple times if it appears multiple times in SqlNode
    // TODO: Add unit test to verify the necessity of creating a new object
    return SqlStdOperatorTable.ITEM.createCall(SqlParserPos.ZERO, sqlCall.getOperandList().get(0), newIndex);
  }
}
