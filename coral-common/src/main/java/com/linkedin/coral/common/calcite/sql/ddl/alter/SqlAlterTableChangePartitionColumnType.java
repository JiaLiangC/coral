package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
public class SqlAlterTableChangePartitionColumnType extends SqlCall {
    private final SqlNode partitionSpec;
    private final SqlDataTypeSpec newColType;

    public SqlAlterTableChangePartitionColumnType(SqlParserPos pos, SqlIdentifier tableName, SqlNode partitionSpec, SqlDataTypeSpec newColType) {
        super(pos);
        this.partitionSpec = partitionSpec;
        this.newColType = newColType;
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
