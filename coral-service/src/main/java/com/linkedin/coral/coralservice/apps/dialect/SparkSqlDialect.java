package com.linkedin.coral.coralservice.apps.dialect;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;

public class SparkSqlDialect implements SqlDialect {
    private static final Set<String> KEYWORDS = new HashSet<>(Arrays.asList(
            "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY",
            "LIMIT", "UNION", "ALL", "DISTINCT", "INSERT", "OVERWRITE", "INTO",
            "CREATE", "TABLE", "VIEW", "USING", "PARTITIONED BY",
            "CLUSTERED BY", "SORTED BY", "BUCKETS", "LOCATION",
            "TBLPROPERTIES", "AS", "WITH", "TEMPORARY", "GLOBAL"
    ));

    private static final Map<String, String> FUNCTION_MAPPINGS = new HashMap<>();
    static {
        FUNCTION_MAPPINGS.put("SUBSTR", "SUBSTRING");
        FUNCTION_MAPPINGS.put("CURRENT_DATE", "CURRENT_DATE()");
        FUNCTION_MAPPINGS.put("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()");
        FUNCTION_MAPPINGS.put("NVL", "COALESCE");
        FUNCTION_MAPPINGS.put("TRUNC", "DATE_TRUNC");
    }

    @Override
    public String getName() {
        return "Spark";
    }

    @Override
    public Set<String> getKeywords() {
        return Collections.unmodifiableSet(KEYWORDS);
    }

    @Override
    public Map<String, String> getFunctionMappings() {
        return Collections.unmodifiableMap(FUNCTION_MAPPINGS);
    }

    // 其他 Spark 特定的方法
    public boolean supportsNestedSchemas() {
        return true;
    }

    public boolean supportsStructuredStreaming() {
        return true;
    }

    public String getDefaultFileFormat() {
        return "PARQUET";
    }
}
