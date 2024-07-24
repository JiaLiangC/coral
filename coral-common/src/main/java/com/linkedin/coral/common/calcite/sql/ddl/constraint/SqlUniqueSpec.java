/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.ddl.constraint;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;


/** Enumeration of SQL unique specification. */
public enum SqlUniqueSpec {
  PRIMARY_KEY("PRIMARY KEY"),
  UNIQUE("UNIQUE");

  private final String digest;

  SqlUniqueSpec(String digest) {
    this.digest = digest;
  }

  @Override
  public String toString() {
    return this.digest;
  }

  /**
   * Creates a parse-tree node representing an occurrence of this keyword at a particular position
   * in the parsed text.
   */
  public SqlLiteral symbol(SqlParserPos pos) {
    return SqlLiteral.createSymbol(this, pos);
  }
}
