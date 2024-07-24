package com.linkedin.coral.coralservice.apps.plugin;

import com.linkedin.coral.coralservice.apps.OptimizationStrategy;
import com.linkedin.coral.coralservice.apps.plugin.Plugin;

public interface OptimizationStrategyPlugin extends Plugin {
    OptimizationStrategy getOptimizationStrategy();
}