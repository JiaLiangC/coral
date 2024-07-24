/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.ddl;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;


public class SqlSkewedByOnClause extends SqlCall {

  private final SqlNodeList valuesList;

  public SqlSkewedByOnClause(SqlParserPos pos, SqlNodeList valuesList) {
    super(pos);
    this.valuesList = Objects.requireNonNull(valuesList, "valuesList");
  }

  public SqlNodeList getValuesList() {
    return valuesList;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(valuesList);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ON");
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode valueList : valuesList) {
      writer.sep(",");
      valueList.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }

  private static final SqlOperator OPERATOR = new SqlSpecialOperator("SKEWED BY ON", SqlKind.OTHER);

  // Convenience method to get the number of value lists
  public int getValueListCount() {
    return valuesList.size();
  }

  // Convenience method to get a specific value list
  public SqlNode getValueList(int index) {
    return valuesList.get(index);
  }
}
