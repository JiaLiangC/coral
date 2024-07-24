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


/**
 * Show Functions sql call. The full syntax for show functions is as followings:
 *
 * <pre>{@code
 * SHOW [USER] FUNCTIONS [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] (LIKE | ILIKE)
 * <sql_like_pattern> ] statement
 * }</pre>
 */
public class SqlShowFunctions extends SqlCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW FUNCTIONS", SqlKind.OTHER);

  private final boolean requireUser;
  private final String preposition;
  private final SqlIdentifier databaseName;
  // different like type such as like, ilike
  private final String likeType;
  private final SqlCharStringLiteral likeLiteral;
  private final boolean notLike;

  public SqlShowFunctions(SqlParserPos pos, boolean requireUser, String preposition, SqlIdentifier databaseName,
      String likeType, SqlCharStringLiteral likeLiteral, boolean notLike) {
    super(pos);
    this.requireUser = requireUser;
    this.preposition = preposition;
    this.databaseName = preposition != null ? requireNonNull(databaseName, "Database name must not be null.") : null;
    if (likeType != null) {
      this.likeType = likeType;
      this.likeLiteral = requireNonNull(likeLiteral, "Like pattern must not be null");
      this.notLike = notLike;
    } else {
      this.likeType = null;
      this.likeLiteral = null;
      this.notLike = false;
    }
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Objects.isNull(databaseName) ? Collections.emptyList() : Collections.singletonList(databaseName);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    String keyword;
    if (requireUser) {
      keyword = "SHOW USER FUNCTIONS";
    } else {
      keyword = "SHOW FUNCTIONS";
    }
    if (preposition == null) {
      writer.keyword(keyword);
    } else if (databaseName != null) {
      writer.keyword(keyword + " " + preposition);
      databaseName.unparse(writer, leftPrec, rightPrec);
    }
    if (isWithLike()) {
      if (isNotLike()) {
        writer.keyword(String.format("NOT %s '%s'", likeType, getLikeSqlPattern()));
      } else {
        writer.keyword(String.format("%s '%s'", likeType, getLikeSqlPattern()));
      }
    }
  }

  public boolean requireUser() {
    return requireUser;
  }

  public String getPreposition() {
    return preposition;
  }

  public String[] fullDatabaseName() {
    return Objects.isNull(this.databaseName) ? new String[] {} : databaseName.names.toArray(new String[0]);
  }

  public boolean isWithLike() {
    return likeType != null;
  }

  public String getLikeType() {
    return likeType;
  }

  public String getLikeSqlPattern() {
    return Objects.isNull(likeLiteral) ? null : likeLiteral.getValueAs(String.class);
  }

  public boolean isNotLike() {
    return notLike;
  }
}
