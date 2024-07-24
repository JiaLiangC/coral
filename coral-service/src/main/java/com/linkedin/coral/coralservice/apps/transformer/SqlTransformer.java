package com.linkedin.coral.coralservice.apps.transformer;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;


public abstract class SqlTransformer<T extends SqlNode> {
    private final SqlDialect sourceDialect;
    private final SqlDialect targetDialect;


    public SqlTransformer(SqlDialect sourceDialect, SqlDialect targetDialect){
        if (sourceDialect == null || targetDialect == null) {
            throw new IllegalArgumentException("Source and target dialects must not be null");
        }
        this.sourceDialect = sourceDialect;
        this.targetDialect = targetDialect;
    }

    public SqlDialect getSourceDialect() {
        return sourceDialect;
    }

    public SqlDialect getTargetDialect() {
        return targetDialect;
    }

    /**
     * Condition of the transformer, it's used to determine if the SqlCall should be transformed or not
     */
    protected abstract boolean condition(T sqlNode);

    /**
     * Implementation of the transformation, returns the transformed SqlCall
     */
    protected abstract T transform(T sqlNode);

    /**
     * Public entry of the transformer, it returns the result of transformed SqlCall if `condition(SqlCall)` returns true,
     * otherwise returns the input SqlCall without any transformation
     */
    public T apply(T sqlCall) {
        if (condition(sqlCall)) {
            return transform(sqlCall);
        } else {
            return sqlCall;
        }
    }
}