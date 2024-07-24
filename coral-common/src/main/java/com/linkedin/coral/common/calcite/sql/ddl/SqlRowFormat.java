/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.parser.SqlParserPos;


public abstract class SqlRowFormat extends SqlCall {
  public SqlRowFormat(SqlParserPos pos) {
    super(pos);
  }

  public abstract SqlRowFormatType getRowFormatType();

  public enum SqlRowFormatType {
    DELIMITED,
    SERDE
  }
}
