package com.linkedin.coral.coralservice.apps.plugin;

import com.linkedin.coral.coralservice.apps.parser.SqlParser;
import com.linkedin.coral.coralservice.apps.plugin.Plugin;
import org.apache.calcite.sql.SqlDialect;

public interface SqlDialectPlugin extends Plugin {
    String getDialectName();
    SqlDialect getDialect();
    Class<? extends SqlParser> getParserClass();
}