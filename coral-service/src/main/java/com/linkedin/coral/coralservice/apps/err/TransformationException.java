package com.linkedin.coral.coralservice.apps.err;

import org.apache.calcite.sql.SqlNode;

public class TransformationException extends RuntimeException {
    private final String ruleName;
    private final SqlNode nodeBeforeTransformation;

    public TransformationException(String message, String ruleName, SqlNode nodeBeforeTransformation) {
        super(message);
        this.ruleName = ruleName;
        this.nodeBeforeTransformation = nodeBeforeTransformation;
    }

    public TransformationException(String message, String ruleName, SqlNode nodeBeforeTransformation, Throwable cause) {
        super(message, cause);
        this.ruleName = ruleName;
        this.nodeBeforeTransformation = nodeBeforeTransformation;
    }

    public String getRuleName() {
        return ruleName;
    }

    public SqlNode getNodeBeforeTransformation() {
        return nodeBeforeTransformation;
    }

    @Override
    public String toString() {
        return "TransformationException: " + getMessage() +
                "\nRule Name: " + ruleName +
                "\nNode Before Transformation: " + nodeBeforeTransformation;
    }
}
