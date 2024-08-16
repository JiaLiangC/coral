package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableAddPartition extends SqlCall {
    private final SqlNode partitionSpec;
    private final SqlNode location;
    private final boolean ifNotExists;

    public SqlAlterTableAddPartition(SqlParserPos pos, SqlIdentifier tableName, SqlNode partitionSpec, SqlNode location, boolean ifNotExists) {
        super(pos);
        this.partitionSpec = partitionSpec;
        this.location = location;
        this.ifNotExists = ifNotExists;
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