package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableCreateTag extends SqlCall {
    private final SqlIdentifier tagName;
    private final SqlNode asOfClause;

    public SqlAlterTableCreateTag(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier tagName, SqlNode asOfClause) {
        super(pos);
        this.tagName = tagName;
        this.asOfClause = asOfClause;
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
