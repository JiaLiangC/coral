package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;
import java.util.List;

public class SqlAlterTableUnsetSerdeProperties extends SqlCall {
    private final SqlNodeList properties;

    public SqlAlterTableUnsetSerdeProperties(SqlParserPos pos, SqlNodeList properties) {
        super(pos);
        this.properties = properties;
    }

    public SqlOperator getOperator() {
        return new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE) {
            @Override
            public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                      SqlParserPos pos,
                                      @Nullable SqlNode... operands) {
                assert operands != null;
                SqlNodeList properties = (SqlNodeList) operands[0];
                return new SqlAlterTableUnsetSerdeProperties(pos, properties);
            }
        };
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(properties);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

        writer.keyword("UNSET SERDEPROPERTIES ");

        SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 0; i < properties.size(); i++) {
            SqlNode property = properties.get(i);
            if (i > 0) {
                writer.print(",");
            }
            property.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);

    }
}