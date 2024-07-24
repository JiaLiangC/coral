/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql.dml;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.linkedin.coral.common.calcite.sql.SqlPartitionUtils;


/** SHOW PARTITIONS sql call. */
public class SqlShowPartitions extends SqlCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("SHOW PARTITIONS", SqlKind.OTHER);

  private final SqlIdentifier tableIdentifier;
  @Nullable
  private final SqlNodeList partitionSpec;

  public SqlShowPartitions(SqlParserPos pos, SqlIdentifier tableName, @Nullable SqlNodeList partitionSpec) {
    super(pos);
    this.tableIdentifier = requireNonNull(tableName, "tableName should not be null");
    this.partitionSpec = partitionSpec;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> operands = new ArrayList<>();
    operands.add(tableIdentifier);
    operands.add(partitionSpec);
    return operands;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW PARTITIONS");
    tableIdentifier.unparse(writer, leftPrec, rightPrec);
    SqlNodeList partitionSpec = getPartitionSpec();
    if (partitionSpec != null && partitionSpec.size() > 0) {
      writer.keyword("PARTITION");
      partitionSpec.unparse(writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
    }
  }

  public String[] fullTableName() {
    return tableIdentifier.names.toArray(new String[0]);
  }

  /**
   * Returns the partition spec if the SHOW should be applied to partitions, and null otherwise.
   */
  public SqlNodeList getPartitionSpec() {
    return partitionSpec;
  }

  /** Get partition spec as key-value strings. */
  public LinkedHashMap<String, String> getPartitionKVs() {
    return SqlPartitionUtils.getPartitionKVs(getPartitionSpec());
  }
}
