package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableUpdateTableStats extends SqlCall {
    private final SqlNodeList tableStats;

    public SqlAlterTableUpdateTableStats(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList tableStats) {
        super(pos);
        this.tableStats = tableStats;
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