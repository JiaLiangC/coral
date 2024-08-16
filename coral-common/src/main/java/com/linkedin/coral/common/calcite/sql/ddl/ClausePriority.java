package com.linkedin.coral.common.calcite.sql.ddl;

public enum ClausePriority {
    TABLE_NAME(0),
    PARTITION(1),
    CLUSTERED_BY(2),
    SORTED_BY(3),
    BUCKETS(4),
    OTHER(5);

    private final int priority;

    ClausePriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }
}
