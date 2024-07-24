/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.ddl;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;


/** SQL call for "SET" and "SET 'key' = 'value'". */
public class SqlSet extends SqlCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SET", SqlKind.OTHER);

  @Nullable
  private final SqlNode key;
  @Nullable
  private final SqlNode value;

  public SqlSet(SqlParserPos pos, SqlNode key, SqlNode value) {
    super(pos);
    this.key = Objects.requireNonNull(key, "key cannot be null");
    this.value = Objects.requireNonNull(value, "value cannot be null");
  }

  public SqlSet(SqlParserPos pos) {
    super(pos);
    this.key = null;
    this.value = null;
  }

  @Override
  @Nonnull
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  @Nonnull
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(key, value);
  }

  public @Nullable SqlNode getKey() {
    return key;
  }

  public @Nullable SqlNode getValue() {
    return value;
  }

  public @Nullable String getKeyString() {
    if (key == null) {
      return null;
    }

    return ((NlsString) SqlLiteral.value(key)).getValue();
  }

  public @Nullable String getValueString() {
    if (value == null) {
      return null;
    }

    return ((NlsString) SqlLiteral.value(value)).getValue();
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SET");

    if (key != null && value != null) {
      key.unparse(writer, leftPrec, rightPrec);
      writer.keyword("=");
      value.unparse(writer, leftPrec, rightPrec);
    }
  }
}
