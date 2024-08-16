package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableUnsetProperties extends SqlCall {
    private final SqlNodeList properties;

    public SqlAlterTableUnsetProperties(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList properties) {
        super(pos);
        this.properties = properties;
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