package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
public class SqlAlterTableAddConstraint  extends SqlCall  {
    private final SqlNode constraint;

    public SqlAlterTableAddConstraint(SqlParserPos pos, SqlIdentifier tableName, SqlNode constraint) {
        super(pos);
        this.constraint = constraint;
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