/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


public class SqlValueTuple extends SqlCall {

  public SqlValueTuple(SqlParserPos pos, SqlNode... values) {
    super(pos);
    this.values = ImmutableList.copyOf(values);
  }

  public List<SqlNode> getValues() {
    return values;
  }

  private final ImmutableList<SqlNode> values;

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return values;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode value : values) {
      writer.sep(",");
      value.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }

  private static final SqlOperator OPERATOR = new SqlSpecialOperator("VALUE_TUPLE", SqlKind.OTHER);
}
