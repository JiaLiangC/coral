package com.linkedin.coral.common.calcite.sql;


public enum SqlIntervalQualifier {
    YEAR_TO_MONTH(1052, "YEAR TO MONTH", new String[]{"YEAR", "YEARS", "MONTH", "MONTHS"}),
    DAY_TO_SECOND(1045, "DAY TO SECOND", new String[]{"DAY", "DAYS", "SECOND", "SECONDS"}),
    YEAR(1050, "YEAR", new String[]{"YEAR", "YEARS"}),
    MONTH(1048, "MONTH", new String[]{"MONTH", "MONTHS"}),
    DAY(1043, "DAY", new String[]{"DAY", "DAYS"}),
    HOUR(1046, "HOUR", new String[]{"HOUR", "HOURS"}),
    MINUTE(1047, "MINUTE", new String[]{"MINUTE", "MINUTES"}),
    SECOND(1049, "SECOND", new String[]{"SECOND", "SECONDS"});

    private final int code;
    private final String sql;
    private final String[] variants;

    SqlIntervalQualifier(int code, String sql, String[] variants) {
        this.code = code;
        this.sql = sql;
        this.variants = variants;
    }

    public static SqlIntervalQualifier valueOf(int code) {
        for (SqlIntervalQualifier qualifier : values()) {
            if (qualifier.code == code) {
                return qualifier;
            }
        }
        throw new IllegalArgumentException("Invalid interval qualifier code: " + code);
    }

    public static SqlIntervalQualifier fromString(String s) {
        for (SqlIntervalQualifier qualifier : values()) {
            for (String variant : qualifier.variants) {
                if (variant.equalsIgnoreCase(s)) {
                    return qualifier;
                }
            }
        }
        throw new IllegalArgumentException("Invalid interval qualifier: " + s);
    }


    @Override
    public String toString() {
        return sql;
    }

    public String[] getVariants() {
        return variants;
    }
}
