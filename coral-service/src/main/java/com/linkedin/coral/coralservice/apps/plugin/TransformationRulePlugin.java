package com.linkedin.coral.coralservice.apps.plugin;

import com.linkedin.coral.coralservice.apps.plugin.Plugin;
import com.linkedin.coral.coralservice.apps.transformer.SqlTransformer;

public interface TransformationRulePlugin extends Plugin {
    SqlTransformer getTransformationRule();
}