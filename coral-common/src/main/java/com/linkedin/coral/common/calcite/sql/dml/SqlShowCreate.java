/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.dml;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;


/** Abstract class for {@link SqlShowCreateTable} and {@link SqlShowCreateView}. */
public abstract class SqlShowCreate extends SqlCall {

  protected final SqlIdentifier sqlIdentifier;

  public SqlShowCreate(SqlParserPos pos, SqlIdentifier sqlIdentifier) {
    super(pos);
    this.sqlIdentifier = sqlIdentifier;
  }
}
