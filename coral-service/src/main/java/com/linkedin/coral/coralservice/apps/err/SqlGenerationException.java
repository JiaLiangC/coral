package com.linkedin.coral.coralservice.apps.err;

import org.apache.calcite.sql.SqlNode;

public class SqlGenerationException extends RuntimeException {
    private final String dialectName;
    private final SqlNode node;

    public SqlGenerationException(String message, String dialectName, SqlNode node) {
        super(message);
        this.dialectName = dialectName;
        this.node = node;
    }

    public SqlGenerationException(String message, String dialectName, SqlNode node, Throwable cause) {
        super(message, cause);
        this.dialectName = dialectName;
        this.node = node;
    }

    public String getDialectName() {
        return dialectName;
    }

    public SqlNode getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "SqlGenerationException: " + getMessage() +
                "\nDialect Name: " + dialectName +
                "\nSQL Node: " + node;
    }
}
