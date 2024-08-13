/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.ddl;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;


/** USE [catalog.]database sql call. */
public class SqlUseDatabase extends SqlCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("USE DATABASE", SqlKind.OTHER_DDL){
    @Override
    public SqlCall createCall(@Nullable final SqlLiteral functionQualifier,
                              final SqlParserPos pos,
                              final @Nullable SqlNode... operands) {
      if (operands.length < 1) {
        throw new IllegalArgumentException("Invalid number of operands for CREATE TABLE");
      }
      SqlIdentifier databaseName = (SqlIdentifier) operands[0];

      return new SqlUseDatabase(pos, databaseName);
    }
  };
  private final SqlIdentifier databaseName;

  public SqlUseDatabase(SqlParserPos pos, SqlIdentifier databaseName) {
    super(pos);
    this.databaseName = databaseName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(databaseName);
  }

  public SqlIdentifier getDatabaseName() {
    return databaseName;
  }

  public String[] fullDatabaseName() {
    return databaseName.names.toArray(new String[0]);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("USE");
    databaseName.unparse(writer, leftPrec, rightPrec);
  }
}
