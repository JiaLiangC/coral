package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableDropPartition extends SqlCall {
    private final SqlNodeList partitionSpecs;
    private final boolean ifExists;
    private final boolean purge;

    public SqlAlterTableDropPartition(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList partitionSpecs, boolean ifExists, boolean purge) {
        super(pos);
        this.partitionSpecs = partitionSpecs;
        this.ifExists = ifExists;
        this.purge = purge;
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