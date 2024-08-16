package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableDropBranch extends SqlCall {
    private final SqlIdentifier branchName;

    public SqlAlterTableDropBranch(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier branchName) {
        super(pos);
        this.branchName = branchName;
    }

    @Override
    public SqlOperator getOperator() {
        return null;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }
}
