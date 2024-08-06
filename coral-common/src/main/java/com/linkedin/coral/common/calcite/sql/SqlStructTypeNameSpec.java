package com.linkedin.coral.common.calcite.sql;


import com.linkedin.coral.common.calcite.sql.ddl.SqlTableColumn;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;


public class SqlStructTypeNameSpec extends SqlTypeNameSpec {
    private final SqlNodeList fields;

    public SqlStructTypeNameSpec(SqlNodeList fields, SqlParserPos pos) {
        super(new SqlIdentifier("STRUCT", pos), pos);
        this.fields = fields;
    }

    public SqlNodeList getFields() {
        return fields;
    }

    @Override
    public RelDataType deriveType(SqlValidator sqlValidator) {
        // 实现类型推导逻辑
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("STRUCT");
        writer.keyword("<");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                writer.print(",");
            }
            SqlNode field = fields.get(i);
            if (field instanceof SqlTableColumn.SqlRegularColumn) {
                SqlTableColumn.SqlRegularColumn column = (SqlTableColumn.SqlRegularColumn) field;
                column.getName().unparse(writer, 0, 0);
                writer.keyword(":");
                column.getType().unparse(writer, 0, 0);
            } else {
                field.unparse(writer, 0, 0);
            }
        }
        writer.keyword(">");
    }

    @Override
    public boolean equalsDeep(SqlTypeNameSpec node, Litmus litmus) {
        if (!(node instanceof SqlStructTypeNameSpec)) {
            return litmus.fail("{} != SqlStructTypeNameSpec", node);
        }
        SqlStructTypeNameSpec that = (SqlStructTypeNameSpec) node;
        return litmus.succeed() && this.fields.equalsDeep(that.fields, litmus);
    }
}
