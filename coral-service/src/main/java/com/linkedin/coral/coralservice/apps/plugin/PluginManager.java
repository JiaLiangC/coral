package com.linkedin.coral.coralservice.apps.plugin;

import com.linkedin.coral.coralservice.apps.parser.HiveSqlParser;
import com.linkedin.coral.coralservice.apps.transformer.DataTypeTransformer;
import com.linkedin.coral.coralservice.apps.transformer.ShiftArrayIndexTransformer;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class PluginManager {
    private static volatile PluginManager instance;
    private final List<Plugin> plugins = new ArrayList<>();

    public PluginRegistry getRegistry() {
        return registry;
    }

    private final PluginRegistry registry;

    private PluginManager(PluginRegistry registry) {
        this.registry = registry;
    }

    public static PluginManager getInstance() {
        if (instance == null) {
            synchronized (PluginManager.class) {
                if (instance == null) {
                    instance = new PluginManager(PluginRegistry.getInstance());
                }
            }
        }
        return instance;
    }

    public void loadPlugins(String pluginDir) {
        File dir = new File(pluginDir);
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException(pluginDir + " is not a directory");
        }

        File[] jarFiles = dir.listFiles((d, name) -> name.endsWith(".jar"));
        if (jarFiles == null) {
            return;
        }

        try {
            URL[] urls = new URL[jarFiles.length];
            for (int i = 0; i < jarFiles.length; i++) {
                urls[i] = jarFiles[i].toURI().toURL();
            }

            try (URLClassLoader classLoader = new URLClassLoader(urls, getClass().getClassLoader())) {
                ServiceLoader<Plugin> loader = ServiceLoader.load(Plugin.class, classLoader);
                for (Plugin plugin : loader) {
                    plugins.add(plugin);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load plugins", e);
        }
    }

    public void initializePlugins() {
        //do BuiltIn plugins initialize
        registerBuiltInComponents();

        //do external plugins initialize
        for (Plugin plugin : plugins) {
            try {
                plugin.initialize();
                registerPluginComponents(plugin);
            } catch (Exception e) {
                System.err.println("Failed to initialize plugin: " + plugin.getClass().getName());
                e.printStackTrace();
            }
        }
    }

    private void registerBuiltInComponents() {
        // 注册内置的 SQL 方言
        registerBuiltInSqlDialects();
        // 注册内置的转换规则
        registerBuiltInTransformationRules();
        // 注册内置的优化策略
        registerBuiltInOptimizationStrategies();
    }

    private void registerBuiltInSqlDialects() {
        SqlDialect hiveDialect =  HiveSqlDialect.DEFAULT;
        registry.registerSqlDialect(hiveDialect);
        registry.registerSqlParserClass(hiveDialect.getClass().getName(), HiveSqlParser.class);
    }

    private void registerBuiltInTransformationRules() {
        registry.registerSqlTransformer(new DataTypeTransformer());
    }

    private void registerBuiltInOptimizationStrategies() {
        //registry.registerOptimizationStrategy(new PushDownPredicateStrategy());
        //registry.registerOptimizationStrategy(new MergeProjectionsStrategy());
    }

    private void registerPluginComponents(Plugin plugin) {
        if (plugin instanceof SqlDialectPlugin) {
            registerSqlDialectPlugin((SqlDialectPlugin) plugin);
        }
        if (plugin instanceof TransformationRulePlugin) {
            registerTransformationRulePlugin((TransformationRulePlugin) plugin);
        }
        if (plugin instanceof OptimizationStrategyPlugin) {
            registerOptimizationStrategyPlugin((OptimizationStrategyPlugin) plugin);
        }
        // other plugins
    }

    private void registerSqlDialectPlugin(SqlDialectPlugin plugin) {
        registry.registerSqlDialect(plugin.getDialect());
        registry.registerSqlParserClass(plugin.getDialectName(), plugin.getParserClass());
    }

    private void registerTransformationRulePlugin(TransformationRulePlugin plugin) {
        registry.registerSqlTransformer(plugin.getTransformationRule());
    }

    private void registerOptimizationStrategyPlugin(OptimizationStrategyPlugin plugin) {
        registry.registerOptimizationStrategy(plugin.getOptimizationStrategy());
    }

    public void shutdownPlugins() {
        for (Plugin plugin : plugins) {
            plugin.shutdown();
        }
    }
}
