package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableDropTag extends SqlCall {
    private final SqlIdentifier tagName;

    public SqlAlterTableDropTag(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier tagName) {
        super(pos);
        this.tagName = tagName;
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