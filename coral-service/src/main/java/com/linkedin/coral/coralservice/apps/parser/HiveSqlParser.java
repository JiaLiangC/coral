package com.linkedin.coral.coralservice.apps.parser;

import com.linkedin.coral.coralservice.apps.err.SqlParseException;
import com.linkedin.coral.hive.hive2rel.functions.HiveFunctionResolver;
import com.linkedin.coral.hive.hive2rel.functions.StaticHiveFunctionRegistry;
import com.linkedin.coral.hive.hive2rel.parsetree.ParseTreeBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlDialect;

import java.util.concurrent.ConcurrentHashMap;

public class HiveSqlParser implements SqlParser {
    private final SqlDialect dialect;
    private final ParseTreeBuilder parseTreeBuilder;

    private final HiveFunctionResolver functionResolver =
            new HiveFunctionResolver(new StaticHiveFunctionRegistry(), new ConcurrentHashMap<>());

    public HiveSqlParser(SqlDialect dialect) {
        this.dialect = dialect;
        this.parseTreeBuilder = new ParseTreeBuilder(functionResolver);
    }

    @Override
    public SqlNode parse(String sql) throws SqlParseException {
        //todo view 处理 Hive SQL 特定的解析逻辑
        return parseTreeBuilder.process(trimParenthesis(sql), null);
    }

    @Override
    public SqlDialect getDialect() {
        return dialect;
    }

    private static String trimParenthesis(String value) {
        String str = value.trim();
        if (str.startsWith("(") && str.endsWith(")")) {
            return trimParenthesis(str.substring(1, str.length() - 1));
        }
        return str;
    }
}