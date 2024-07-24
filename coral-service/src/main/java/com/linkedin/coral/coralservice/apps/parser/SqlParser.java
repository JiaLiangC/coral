package com.linkedin.coral.coralservice.apps.parser;

import com.linkedin.coral.coralservice.apps.err.SqlParseException;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

public interface SqlParser {
    SqlNode parse(String sql) throws SqlParseException;
    SqlDialect getDialect();
}
