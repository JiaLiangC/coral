package com.linkedin.coral.coralservice.apps.transformer;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.util.Arrays;

public class SqlTransformers <T extends SqlTransformer>{

    private final ImmutableList<T> sqlCallTransformers;

    SqlTransformers(ImmutableList<T> sqlTransformers) {
        this.sqlCallTransformers = sqlTransformers;
    }

    public static SqlTransformers of(SqlTransformer... sqlCallTransformers) {
        return new SqlTransformers(
                ImmutableList.<SqlTransformer> builder().addAll(Arrays.asList(sqlCallTransformers)).build());
    }

    public static SqlTransformers of(ImmutableList<SqlTransformer> sqlCallTransformers) {
        return new SqlTransformers(sqlCallTransformers);
    }

    public SqlNode apply(SqlNode sqlCall) {
        for (T sqlCallTransformer : sqlCallTransformers) {
            sqlCall = sqlCallTransformer.apply(sqlCall);
        }
        return sqlCall;
    }

}
