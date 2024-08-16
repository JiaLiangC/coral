package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableSetOwner extends SqlCall {
    private final SqlIdentifier newOwner;

    public SqlAlterTableSetOwner(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier newOwner) {
        super(pos);
        this.newOwner = newOwner;
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
