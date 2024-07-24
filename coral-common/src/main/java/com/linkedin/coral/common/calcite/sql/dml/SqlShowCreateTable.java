/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.dml;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/** SHOW CREATE TABLE sql call. */
public class SqlShowCreateTable extends SqlShowCreate {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW CREATE TABLE", SqlKind.OTHER_DDL);

  public SqlShowCreateTable(SqlParserPos pos, SqlIdentifier tableName) {
    super(pos, tableName);
  }

  public SqlIdentifier getTableName() {
    return sqlIdentifier;
  }

  public String[] getFullTableName() {
    return sqlIdentifier.names.toArray(new String[0]);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.singletonList(sqlIdentifier);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW CREATE TABLE");
    sqlIdentifier.unparse(writer, leftPrec, rightPrec);
  }
}
