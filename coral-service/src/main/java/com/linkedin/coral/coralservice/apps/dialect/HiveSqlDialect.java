package com.linkedin.coral.coralservice.apps.dialect;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Collections;

public class HiveSqlDialect implements SqlDialect {
    private static final Set<String> KEYWORDS = new HashSet<>(Arrays.asList(
            "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY",
            "DISTRIBUTE BY", "SORT BY", "CLUSTER BY", "LIMIT",
            "UNION", "ALL", "DISTINCT", "INSERT", "OVERWRITE", "INTO",
            "CREATE", "TABLE", "VIEW", "EXTERNAL", "PARTITIONED BY",
            "STORED AS", "LOCATION", "ROW FORMAT", "DELIMITED",
            "FIELDS TERMINATED BY", "COLLECTION ITEMS TERMINATED BY",
            "MAP KEYS TERMINATED BY", "LINES TERMINATED BY"
    ));

    private static final Map<String, String> FUNCTION_MAPPINGS = new HashMap<>();
    static {
        FUNCTION_MAPPINGS.put("SUBSTR", "SUBSTRING");
        FUNCTION_MAPPINGS.put("CURRENT_DATE", "TO_DATE(CURRENT_TIMESTAMP())");
        FUNCTION_MAPPINGS.put("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()");
        FUNCTION_MAPPINGS.put("NVL", "COALESCE");
        FUNCTION_MAPPINGS.put("TRUNC", "DATE_TRUNC");
    }

    @Override
    public String getName() {
        return "Hive";
    }

    @Override
    public Set<String> getKeywords() {
        return Collections.unmodifiableSet(KEYWORDS);
    }

    @Override
    public Map<String, String> getFunctionMappings() {
        return Collections.unmodifiableMap(FUNCTION_MAPPINGS);
    }

    // 其他 Hive 特定的方法
    public String getDefaultNullOrdering() {
        return "NULLS FIRST";
    }

    public boolean supportsWindowFunctions() {
        return true;
    }

    public String getTimestampFormat() {
        return "yyyy-MM-dd HH:mm:ss";
    }
}
