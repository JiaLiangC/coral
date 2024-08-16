package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableMergeFiles extends SqlCall {
    private final SqlNode partitionSpec;

    public SqlAlterTableMergeFiles(SqlParserPos pos, SqlIdentifier tableName, SqlNode partitionSpec) {
        super(pos);
        this.partitionSpec = partitionSpec;
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