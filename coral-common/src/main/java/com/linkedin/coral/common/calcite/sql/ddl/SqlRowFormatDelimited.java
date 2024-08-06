/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.ddl;

import java.util.List;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;


public class SqlRowFormatDelimited extends SqlRowFormat {
  private final SqlCharStringLiteral fieldsTerminatedBy;
  private final SqlCharStringLiteral escapedBy;
  private final SqlCharStringLiteral collectionItemsTerminatedBy;
  private final SqlCharStringLiteral mapKeysTerminatedBy;
  private final SqlCharStringLiteral linesTerminatedBy;
  private final SqlCharStringLiteral nullDefinedAs;

  public SqlRowFormatDelimited(SqlParserPos pos, SqlCharStringLiteral fieldsTerminatedBy,
                               SqlCharStringLiteral escapedBy, SqlCharStringLiteral collectionItemsTerminatedBy,
                               SqlCharStringLiteral mapKeysTerminatedBy, SqlCharStringLiteral linesTerminatedBy,
                               SqlCharStringLiteral nullDefinedAs) {
    super(pos);
    this.fieldsTerminatedBy = fieldsTerminatedBy;
    this.escapedBy = escapedBy;
    this.collectionItemsTerminatedBy = collectionItemsTerminatedBy;
    this.mapKeysTerminatedBy = mapKeysTerminatedBy;
    this.linesTerminatedBy = linesTerminatedBy;
    this.nullDefinedAs = nullDefinedAs;
  }

  public SqlCharStringLiteral getFieldsTerminatedBy() {
    return fieldsTerminatedBy;
  }

  public SqlCharStringLiteral getEscapedBy() {
    return escapedBy;
  }

  public SqlCharStringLiteral getCollectionItemsTerminatedBy() {
    return collectionItemsTerminatedBy;
  }

  public SqlCharStringLiteral getMapKeysTerminatedBy() {
    return mapKeysTerminatedBy;
  }

  public SqlCharStringLiteral getLinesTerminatedBy() {
    return linesTerminatedBy;
  }

  public SqlCharStringLiteral getNullDefinedAs() {
    return nullDefinedAs;
  }


  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ROW FORMAT DELIMITED");

    if (fieldsTerminatedBy != null) {
      writer.newlineAndIndent();
      writer.keyword("FIELDS TERMINATED BY");
      fieldsTerminatedBy.unparse(writer, leftPrec, rightPrec);
    }

    if (escapedBy != null) {
      writer.newlineAndIndent();
      writer.keyword("ESCAPED BY");
      escapedBy.unparse(writer, leftPrec, rightPrec);
    }

    if (collectionItemsTerminatedBy != null) {
      writer.newlineAndIndent();
      writer.keyword("COLLECTION ITEMS TERMINATED BY");
      collectionItemsTerminatedBy.unparse(writer, leftPrec, rightPrec);
    }

    if (mapKeysTerminatedBy != null) {
      writer.newlineAndIndent();
      writer.keyword("MAP KEYS TERMINATED BY");
      mapKeysTerminatedBy.unparse(writer, leftPrec, rightPrec);
    }

    if (linesTerminatedBy != null) {
      writer.newlineAndIndent();
      writer.keyword("LINES TERMINATED BY");
      linesTerminatedBy.unparse(writer, leftPrec, rightPrec);
    }

    if (nullDefinedAs != null) {
      writer.newlineAndIndent();
      writer.keyword("NULL DEFINED AS");
      nullDefinedAs.unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override
  public SqlRowFormatType getRowFormatType() {
    return SqlRowFormatType.DELIMITED;
  }
  @Override
  public SqlOperator getOperator() {
    return new SqlSpecialOperator("ROW FORMAT DELIMITED", SqlKind.OTHER);
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(fieldsTerminatedBy, escapedBy, collectionItemsTerminatedBy, mapKeysTerminatedBy,
            linesTerminatedBy, nullDefinedAs);
  }
}