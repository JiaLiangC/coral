package com.linkedin.coral.coralservice.apps;

import com.linkedin.coral.coralservice.apps.transformer.SqlTransformers;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;


/**
 * Converts Hive SqlNode to Coral SqlNode
 */
public class SqlConverter extends SqlShuttle {
    private final SqlTransformers operatorTransformerList;

    public SqlConverter(SqlTransformers transformerList) {
        this.operatorTransformerList = transformerList;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        return super.visit((SqlCall) operatorTransformerList.apply(call));
    }


    @Override
    public SqlNode visit(SqlDataTypeSpec typeSpec) {
        return super.visit((SqlDataTypeSpec) operatorTransformerList.apply(typeSpec));
    }
}
