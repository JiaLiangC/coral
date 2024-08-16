package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableUpdateColumnStats extends SqlCall {
    private final SqlIdentifier columnName;
    private final SqlNodeList columnStats;

    public SqlAlterTableUpdateColumnStats(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier columnName, SqlNodeList columnStats) {
        super(pos);
        this.columnName = columnName;
        this.columnStats = columnStats;
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
