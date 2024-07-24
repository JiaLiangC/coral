package com.linkedin.coral.coralservice.apps;

import com.google.common.collect.ImmutableList;
import com.linkedin.coral.coralservice.apps.err.ErrorHandler;
import com.linkedin.coral.coralservice.apps.parser.SqlParser;
import com.linkedin.coral.coralservice.apps.plugin.PluginRegistry;
import com.linkedin.coral.coralservice.apps.transformer.SqlTransformer;
import com.linkedin.coral.coralservice.apps.transformer.SqlTransformers;
import com.linkedin.coral.spark.dialect.SparkSqlDialect;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import java.util.Arrays;
import java.util.List;

public class SqlDialectConverter {
    private final PluginRegistry registry;
    private final SqlParser parser;
    private final ErrorHandler errorHandler;

    public SqlDialectConverter(PluginRegistry registry, SqlParser parser,ErrorHandler errorHandler) {
        this.registry = registry;
        this.parser = parser;
        this.errorHandler = errorHandler;
    }

    public String convert(String sql, SqlDialect sourceDialect, SqlDialect targetDialect) {
        try {
        SqlNode parsedNode = parser.parse(sql);

        List<SqlTransformer> rules = registry.getSqlTransformers(sourceDialect, targetDialect);
        SqlTransformers transformers =  SqlTransformers.of(ImmutableList.<SqlTransformer> builder().addAll(rules).build());
        SqlConverter converter = new SqlConverter(transformers);
        SqlNode convertedNode = parsedNode.accept(converter);
        String result =  convertedNode.toSqlString(SparkSqlDialect.INSTANCE).getSql();

        return result;
        } catch (Exception e) {
            errorHandler.handleError(e);
            return null;
        }
    }
}