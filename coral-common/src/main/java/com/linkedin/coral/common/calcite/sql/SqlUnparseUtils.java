/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql;

import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

import com.linkedin.coral.common.calcite.sql.ddl.constraint.SqlTableConstraint;


/** Utils to unparse DDLs. */
public class SqlUnparseUtils {

  private SqlUnparseUtils() {
  }

  public static void unparseTableSchema(SqlWriter writer, int leftPrec, int rightPrec, SqlNodeList columnList,
      List<SqlTableConstraint> constraints) {
    SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
    for (SqlNode column : columnList) {
      printIndent(writer);
      column.unparse(writer, leftPrec, rightPrec);
    }
    if (constraints.size() > 0) {
      for (SqlTableConstraint constraint : constraints) {
        printIndent(writer);
        constraint.unparse(writer, leftPrec, rightPrec);
      }
    }
    //        if (watermark != null) {
    //            printIndent(writer);
    //            watermark.unparse(writer, leftPrec, rightPrec);
    //        }

    writer.newlineAndIndent();
    writer.endList(frame);
  }

  public static void printIndent(SqlWriter writer) {
    writer.sep(",", false);
    writer.newlineAndIndent();
    writer.print("  ");
  }
}
