/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;


/**
 * Properties of a DDL, a key-value pair with key as component identifier and value as string
 * literal.
 */
public class SqlProperty extends SqlCall {

  /** Use this operator only if you don't have a better one. */
  protected static final SqlOperator OPERATOR = new SqlSpecialOperator("Property", SqlKind.OTHER);

  private final SqlCharStringLiteral key;
  private final SqlNode value;

  public SqlProperty(SqlCharStringLiteral key, SqlNode value, SqlParserPos pos) {
    super(pos);
    this.key = requireNonNull(key, "Property key is missing");
    this.value = requireNonNull(value, "Property value is missing");
  }

  public SqlCharStringLiteral getKey() {
    return key;
  }

  public SqlNode getValue() {
    return value;
  }

  public String getKeyString() {
    return key.toString();
  }

  public String getValueString() {
    return ((NlsString) SqlLiteral.value(value)).getValue();
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(key, value);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    key.unparse(writer, leftPrec, rightPrec);
    writer.keyword("=");
    value.unparse(writer, leftPrec, rightPrec);
  }
}

// End SqlProperty.java
