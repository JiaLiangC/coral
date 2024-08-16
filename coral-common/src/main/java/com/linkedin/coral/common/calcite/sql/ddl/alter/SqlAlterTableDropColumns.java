package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableDropColumns extends SqlCall {
    private final SqlNodeList columnsToDrop;

    public SqlAlterTableDropColumns(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList columnsToDrop) {
        super(pos);
        this.columnsToDrop = columnsToDrop;
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