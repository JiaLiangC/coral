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

public class SqlAlterTableRename  extends SqlCall{
    private final SqlNode newTableName;

    public SqlAlterTableRename(SqlParserPos pos, SqlNode newTableName) {
        super(pos);
        this.newTableName = newTableName;
    }


    public SqlOperator getOperator() {
        return new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE) {
            @Override
            public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                      SqlParserPos pos,
                                      @Nullable SqlNode... operands) {
                assert operands != null;
                SqlNode newTableName = (SqlNode) operands[0];
                return new SqlAlterTableRename(pos, newTableName);
            }
        };
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(newTableName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("RENAME TO");
        newTableName.unparse(writer, leftPrec, rightPrec);
    }
}