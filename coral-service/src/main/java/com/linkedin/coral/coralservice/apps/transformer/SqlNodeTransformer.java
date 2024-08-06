package com.linkedin.coral.coralservice.apps.transformer;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;


/**
 * Abstract class for generic transformations on SqlCalls
 */
public abstract class SqlNodeTransformer extends SqlTransformer<SqlNode> {
    public SqlNodeTransformer(SqlDialect sourceDialect, SqlDialect targetDialect) {
        super(sourceDialect, targetDialect);
    }
}
