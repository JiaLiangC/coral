package com.linkedin.coral.coralservice.apps.err;

import org.apache.calcite.sql.SqlNode;

public class OptimizationException extends RuntimeException {
    private final String strategyName;
    private final SqlNode nodeBeforeOptimization;

    public OptimizationException(String message, String strategyName, SqlNode nodeBeforeOptimization) {
        super(message);
        this.strategyName = strategyName;
        this.nodeBeforeOptimization = nodeBeforeOptimization;
    }

    public OptimizationException(String message, String strategyName, SqlNode nodeBeforeOptimization, Throwable cause) {
        super(message, cause);
        this.strategyName = strategyName;
        this.nodeBeforeOptimization = nodeBeforeOptimization;
    }

    public String getStrategyName() {
        return strategyName;
    }

    public SqlNode getNodeBeforeOptimization() {
        return nodeBeforeOptimization;
    }

    @Override
    public String toString() {
        return "OptimizationException: " + getMessage() +
                "\nStrategy Name: " + strategyName +
                "\nNode Before Optimization: " + nodeBeforeOptimization;
    }
}
