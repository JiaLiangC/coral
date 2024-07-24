package com.linkedin.coral.coralservice.apps;

import com.linkedin.coral.coralservice.apps.err.DefaultErrorHandler;
import com.linkedin.coral.coralservice.apps.err.ErrorHandler;
import com.linkedin.coral.coralservice.apps.parser.SqlParser;
import com.linkedin.coral.coralservice.apps.parser.SqlParserFactory;
import com.linkedin.coral.coralservice.apps.plugin.PluginManager;
import com.linkedin.coral.coralservice.apps.plugin.PluginRegistry;
import com.linkedin.coral.coralservice.apps.transformer.SqlTransformer;
import com.linkedin.coral.coralservice.apps.transformer.SqlTransformers;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;

import java.util.List;

public class SqlTransformationApplication {
    private final ConfigurationManager configManager;
    private final PluginManager pluginManager;
    private final PluginRegistry pluginRegistry;

    public SqlTransformationApplication() {
        this.configManager = new ConfigurationManager();
        this.pluginRegistry = new PluginRegistry();
        this.pluginManager = new PluginManager(pluginRegistry);
    }

    public void initialize(String configFile, String pluginDir) {
        configManager.loadConfig(configFile);
        pluginManager.loadPlugins(pluginDir);
        pluginManager.initializePlugins();
    }

    public String transformSql(String inputSql, String sourceDialect, String targetDialect) {

        // 1.TransformationRule 一定会带source 和target rule 的属性，因为Rule是服务于从a 方言转换为b方言的
        // 2.pluginRegistry 获得所有 从a到b 方言转换的rule,然后使用 继承了 sql shuttle 的 SqlNode Converter 转换后的 sql node 生成 Sql String

        //get source dialect parser
        SqlParser parser = SqlParserFactory.createParser(sourceDialect, pluginRegistry);

        //get rules from pluginRegistry by source and target sqldialect
        List<SqlTransformer> rules = pluginRegistry.getSqlTransformers();
        ErrorHandler errorHandler = new DefaultErrorHandler(); // 假设有一个默认实现

        SqlDialectConverter converter = new SqlDialectConverter(pluginRegistry, parser, errorHandler);

        String hiveSql = "SELECT * FROM my_table WHERE id > 100";
        String sparkSql = converter.convert(hiveSql, HiveSqlDialect.DEFAULT, SparkSqlDialect.DEFAULT);
        System.out.println("Converted SQL: " + sparkSql);

        return sparkSql;
    }

    public void shutdown() {
        pluginManager.shutdownPlugins();
    }
}
