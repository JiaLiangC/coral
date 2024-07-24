package com.linkedin.coral.coralservice.apps.plugin;

import com.linkedin.coral.coralservice.apps.OptimizationStrategy;
import com.linkedin.coral.coralservice.apps.transformer.SqlTransformer;
import com.linkedin.coral.coralservice.apps.parser.SqlParser;
import org.apache.calcite.sql.SqlDialect;

import java.util.HashMap;
import java.util.Map;

import java.util.*;

public class PluginRegistry {
    private final Map<String, SqlDialect> sqlDialects = new HashMap<>();
    private final Map<String, Class<? extends SqlParser>> sqlParserClasses = new HashMap<>();
    private final Map<String, SqlTransformer> transformationRules = new HashMap<>();
    private final Map<String, OptimizationStrategy> optimizationStrategies = new HashMap<>();
    private final Map<String, Map<String, List<SqlTransformer>>> dialectSqlTransformers = new HashMap<>();


    public void registerSqlTransformer(SqlTransformer rule) {
        SqlDialect sourceDialect = rule.getSourceDialect();
        SqlDialect targetDialect = rule.getTargetDialect();

        dialectSqlTransformers
                .computeIfAbsent(sourceDialect.getClass().getName(), k -> new HashMap<>())
                .computeIfAbsent(targetDialect.getClass().getName(), k -> new ArrayList<>())
                .add(rule);
    }

    public List<SqlTransformer> getSqlTransformers(SqlDialect sourceDialect, SqlDialect targetDialect) {
        return dialectSqlTransformers
                .getOrDefault(sourceDialect.getClass().getName(), Collections.emptyMap())
                .getOrDefault(targetDialect.getClass().getName(), Collections.emptyList());
    }

    // SQL Dialect 相关方法
    public void registerSqlDialect(SqlDialect dialect) {
        sqlDialects.put(dialect.getClass().getName(), dialect);
    }

    public SqlDialect getSqlDialect(String name) {
        return sqlDialects.get(name);
    }

    public Set<String> getSqlDialectNames() {
        return new HashSet<>(sqlDialects.keySet());
    }

    // SQL Parser 相关方法
    public void registerSqlParserClass(String dialectName, Class<? extends SqlParser> parserClass) {
        sqlParserClasses.put(dialectName, parserClass);
    }

    public Class<? extends SqlParser> getSqlParserClass(String dialectName) {
        return sqlParserClasses.get(dialectName);
    }

    // Transformation Rule 相关方法


    public List<SqlTransformer> getSqlTransformers() {
        return new ArrayList<>(transformationRules.values());
    }

    // Optimization Strategy 相关方法
    public void registerOptimizationStrategy(OptimizationStrategy strategy) {
        optimizationStrategies.put(strategy.getName(), strategy);
    }

    public OptimizationStrategy getOptimizationStrategy(String name) {
        return optimizationStrategies.get(name);
    }

    public List<OptimizationStrategy> getOptimizationStrategies() {
        return new ArrayList<>(optimizationStrategies.values());
    }

    // 通用方法
    public void clear() {
        sqlDialects.clear();
        sqlParserClasses.clear();
        transformationRules.clear();
        optimizationStrategies.clear();
    }

    public boolean hasSqlDialect(String name) {
        return sqlDialects.containsKey(name);
    }

    public boolean hasSqlParserClass(String dialectName) {
        return sqlParserClasses.containsKey(dialectName);
    }

    public boolean hasSqlTransformer(String name) {
        return transformationRules.containsKey(name);
    }

    public boolean hasOptimizationStrategy(String name) {
        return optimizationStrategies.containsKey(name);
    }

    // 获取注册的组件数量
    public int getSqlDialectCount() {
        return sqlDialects.size();
    }

    public int getSqlParserClassCount() {
        return sqlParserClasses.size();
    }

    public int getSqlTransformerCount() {
        return transformationRules.size();
    }

    public int getOptimizationStrategyCount() {
        return optimizationStrategies.size();
    }
}

