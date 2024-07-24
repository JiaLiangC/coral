package com.linkedin.coral.coralservice.apps;

import java.util.Properties;

public class ConfigurationManager {
    private final Properties config = new Properties();

    public void loadConfig(String configFile) {
        // 从配置文件加载配置
    }

    public String getProperty(String key) {
        return config.getProperty(key);
    }

    public void setProperty(String key, String value) {
        config.setProperty(key, value);
    }
}
