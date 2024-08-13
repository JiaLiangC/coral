/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.common.calcite.sql;

import java.util.LinkedHashMap;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.NlsString;


/** Utils methods for partition DDLs. */
public class SqlPartitionUtils {

  private SqlPartitionUtils() {
  }

  /**
   * Get static partition key value pair as strings.
   *
   * <p>For character literals we return the unquoted and unescaped values. For other types we use
   * {@link SqlLiteral#toString()} to get the string format of the value literal.
   *
   * @return the mapping of column names to values of partition specifications, returns an empty
   *     map if there is no partition specifications.
   */
  public static LinkedHashMap<String, String> getPartitionKVs(SqlNodeList partitionSpec) {
    if (partitionSpec == null) {
      return null;
    }
    LinkedHashMap<String, String> ret = new LinkedHashMap<>();
    if (partitionSpec.size() == 0) {
      return ret;
    }
    for (SqlNode node : partitionSpec.getList()) {
      SqlProperty sqlProperty = (SqlProperty) node;
      Comparable<?> comparable = SqlLiteral.value(sqlProperty.getValue());
      String value = comparable instanceof NlsString ? ((NlsString) comparable).getValue() : comparable.toString();
      ret.put(sqlProperty.getKey().toString(), value);
    }
    return ret;
  }
}
