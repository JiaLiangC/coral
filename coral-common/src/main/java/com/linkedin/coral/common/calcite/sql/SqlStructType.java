package com.linkedin.coral.common.calcite.sql;



import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;


public class SqlStructType extends SqlDataTypeSpec {
    private final SqlStructTypeNameSpec structTypeNameSpec;

    public SqlStructType(SqlNodeList fields, SqlParserPos pos) {
        super(new SqlStructTypeNameSpec(fields, pos), null, pos);
        this.structTypeNameSpec = (SqlStructTypeNameSpec) getTypeNameSpec();
    }

    public SqlStructTypeNameSpec getStructTypeNameSpec() {
        return structTypeNameSpec;
    }

    public SqlNodeList getFields() {
        return structTypeNameSpec.getFields();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        getTypeNameSpec().unparse(writer, leftPrec, rightPrec);
    }
}