package com.linkedin.coral.coralservice.apps.transformer;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;


/**
 * Abstract class for generic transformations on SqlCalls
 */
public abstract class SqlCallTransformer extends SqlTransformer<SqlCall> {
    public SqlCallTransformer(SqlDialect sourceDialect, SqlDialect targetDialect) {
        super(sourceDialect, targetDialect);
    }
}
