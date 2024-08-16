package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.Arrays;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.linkedin.coral.common.calcite.sql.ddl.SqlAlterTable;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;
import java.util.List;

import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;

public class SqlTableColValuePair extends SqlCall {
    private final SqlNodeList value_pairs;
    public SqlTableColValuePair(SqlParserPos pos, SqlNodeList value_pairs) {
        super(pos);
        this.value_pairs = value_pairs;
    }

    @Override
    public SqlOperator getOperator() {
        return new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE) {
            @Override
            public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                      SqlParserPos pos,
                                      @Nullable SqlNode... operands) {
                assert operands != null;
                SqlNodeList value_pairs = (SqlNodeList) operands[0];
                return new SqlTableColValuePair(pos, value_pairs);
            }
        };
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(value_pairs);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

        SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 0; i < value_pairs.size(); i++) {
            SqlNode property = value_pairs.get(i);
            if (i > 0) {
                writer.print(",");
            }
            property.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }
}
