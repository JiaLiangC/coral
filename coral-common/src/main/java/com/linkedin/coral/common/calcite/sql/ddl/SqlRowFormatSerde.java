/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.ddl;

import java.util.List;

import com.linkedin.coral.common.calcite.sql.ddl.constraint.SqlColumnConstraint;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;


public class SqlRowFormatSerde extends SqlRowFormat {
  private final SqlCharStringLiteral serdeName;
  private final SqlNodeList serdeProperties;

  public static final SqlSpecialOperator OPERATOR =
          new SqlSpecialOperator("ROW FORMAT SERDE", SqlKind.OTHER) {
            @Override
            public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
                assert operands != null;
                if (operands.length < 2) {
                throw new IllegalArgumentException("Invalid number of operands");
              }
              SqlCharStringLiteral serdeName = (SqlCharStringLiteral) operands[0];
              SqlNodeList serdeProperties = (SqlNodeList)operands[1];
             return new SqlRowFormatSerde(pos, serdeName, serdeProperties);
            }
          };
  public SqlRowFormatSerde(SqlParserPos pos, SqlCharStringLiteral serdeName, SqlNodeList serdeProperties) {
    super(pos);
    this.serdeName = serdeName;
    this.serdeProperties = serdeProperties;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(serdeName, serdeProperties);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ROW FORMAT SERDE");
    serdeName.unparse(writer, leftPrec, rightPrec);
    if (serdeProperties != null && serdeProperties.size() > 0) {
      writer.keyword("WITH SERDEPROPERTIES");
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode property : serdeProperties) {
        writer.sep(",");
        property.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
  }

  @Override
  public SqlRowFormatType getRowFormatType() {
    return SqlRowFormatType.SERDE;
  }

  // Getter methods...
}
