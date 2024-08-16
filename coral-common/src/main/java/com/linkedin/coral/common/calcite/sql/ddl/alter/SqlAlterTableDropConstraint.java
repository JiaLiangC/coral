package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
public class SqlAlterTableDropConstraint extends SqlCall {
    private final SqlIdentifier constraintName;

    public SqlAlterTableDropConstraint(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier constraintName) {
        super(pos);
        this.constraintName = constraintName;
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