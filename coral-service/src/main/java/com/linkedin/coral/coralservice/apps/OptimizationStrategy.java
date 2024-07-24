package com.linkedin.coral.coralservice.apps;

import org.apache.calcite.sql.SqlNode;

public interface OptimizationStrategy {
    SqlNode optimize(SqlNode node);
    String getName();
}