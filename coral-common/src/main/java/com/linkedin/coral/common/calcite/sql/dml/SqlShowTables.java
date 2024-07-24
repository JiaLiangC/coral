/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.dml;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/** SHOW Tables sql call. */
public class SqlShowTables extends SqlCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW TABLES", SqlKind.OTHER);

  protected final SqlIdentifier databaseName;
  protected final String preposition;
  protected final boolean notLike;
  protected final SqlCharStringLiteral likeLiteral;

  public SqlShowTables(SqlParserPos pos) {
    super(pos);
    this.preposition = null;
    this.databaseName = null;
    this.notLike = false;
    this.likeLiteral = null;
  }

  public SqlShowTables(SqlParserPos pos, String preposition, SqlIdentifier databaseName, boolean notLike,
      SqlCharStringLiteral likeLiteral) {
    super(pos);
    this.preposition = preposition;
    this.databaseName = preposition != null ? requireNonNull(databaseName, "Database name must not be null.") : null;
    this.notLike = notLike;
    this.likeLiteral = likeLiteral;
  }

  public String getLikeSqlPattern() {
    return Objects.isNull(this.likeLiteral) ? null : likeLiteral.getValueAs(String.class);
  }

  public boolean isNotLike() {
    return notLike;
  }

  public SqlCharStringLiteral getLikeLiteral() {
    return likeLiteral;
  }

  public boolean isWithLike() {
    return Objects.nonNull(likeLiteral);
  }

  public String getPreposition() {
    return preposition;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Objects.isNull(this.databaseName) ? Collections.emptyList() : Collections.singletonList(databaseName);
  }

  public String[] fullDatabaseName() {
    return Objects.isNull(this.databaseName) ? new String[] {} : databaseName.names.toArray(new String[0]);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (this.preposition == null) {
      writer.keyword("SHOW TABLES");
    } else if (databaseName != null) {
      writer.keyword("SHOW TABLES " + this.preposition);
      databaseName.unparse(writer, leftPrec, rightPrec);
    }
    if (isWithLike()) {
      if (isNotLike()) {
        writer.keyword(String.format("NOT LIKE '%s'", getLikeSqlPattern()));
      } else {
        writer.keyword(String.format("LIKE '%s'", getLikeSqlPattern()));
      }
    }
  }
}
