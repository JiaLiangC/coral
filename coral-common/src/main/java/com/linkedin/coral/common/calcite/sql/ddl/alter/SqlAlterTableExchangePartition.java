package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableExchangePartition extends SqlCall {
    private final SqlNode partitionSpec;
    private final SqlIdentifier sourceTable;

    public SqlAlterTableExchangePartition(SqlParserPos pos, SqlIdentifier tableName, SqlNode partitionSpec, SqlIdentifier sourceTable) {
        super(pos);
        this.partitionSpec = partitionSpec;
        this.sourceTable = sourceTable;
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