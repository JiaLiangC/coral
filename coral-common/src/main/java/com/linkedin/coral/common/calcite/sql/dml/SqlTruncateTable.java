/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.dml;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/**
 * SqlNode to describe TRUNCATE TABLE statement.
 *
 * <p>We parse TRUNCATE TABLE statement in Flink since Calcite doesn't support TRUNCATE TABLE
 * statement currently. Should remove the parse logic for TRUNCATE TABLE statement from Flink after
 * the Calcite used by Flink includes [CALCITE-5688].
 */
public class SqlTruncateTable extends SqlCall {

  private final SqlIdentifier tableNameIdentifier;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("TRUNCATE TABLE", SqlKind.OTHER);

  public SqlTruncateTable(SqlParserPos pos, SqlIdentifier tableNameIdentifier) {
    super(pos);
    this.tableNameIdentifier = tableNameIdentifier;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.singletonList(tableNameIdentifier);
  }

  public String[] fullTableName() {
    return tableNameIdentifier.names.toArray(new String[0]);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("TRUNCATE TABLE");
    tableNameIdentifier.unparse(writer, leftPrec, rightPrec);
  }
}
