package com.linkedin.coral.coralservice.apps.dialect;

import java.util.Map;
import java.util.Set;

public interface SqlDialect {
    String getName();
    Set<String> getKeywords();
    Map<String, String> getFunctionMappings();
    // 其他方言特定的方法
}