package com.linkedin.coral.coralservice.apps.parser;

import com.linkedin.coral.coralservice.apps.parser.SqlParser;
import com.linkedin.coral.coralservice.apps.plugin.PluginRegistry;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;

public class SqlParserFactory {
    public static SqlParser createParser(String dialectName, PluginRegistry registry) {
        SqlDialect dialect = registry.getSqlDialect(dialectName);
        return new HiveSqlParser(HiveSqlDialect.DEFAULT);
    }
}