package com.linkedin.coral.coralservice.apps.err;

public class SqlParseException extends RuntimeException {
    private final String sqlText;
    private final int errorPosition;

    public SqlParseException(String message, String sqlText, int errorPosition) {
        super(message);
        this.sqlText = sqlText;
        this.errorPosition = errorPosition;
    }

    public SqlParseException(String message, String sqlText, int errorPosition, Throwable cause) {
        super(message, cause);
        this.sqlText = sqlText;
        this.errorPosition = errorPosition;
    }

    public String getSqlText() {
        return sqlText;
    }

    public int getErrorPosition() {
        return errorPosition;
    }

    @Override
    public String toString() {
        return "SqlParseException: " + getMessage() +
                "\nSQL Text: " + sqlText +
                "\nError Position: " + errorPosition;
    }
}
