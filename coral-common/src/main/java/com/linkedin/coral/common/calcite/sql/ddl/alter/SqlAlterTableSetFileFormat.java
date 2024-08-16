package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableSetFileFormat extends SqlCall {
    private final SqlNode partitionSpec;
    private final SqlNode fileFormat;

    public SqlAlterTableSetFileFormat(SqlParserPos pos, SqlIdentifier tableName, SqlNode partitionSpec, SqlNode fileFormat) {
        super(pos);
        this.partitionSpec = partitionSpec;
        this.fileFormat = fileFormat;
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