package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableReplaceColumns extends SqlCall {
    private final SqlNodeList newColumns;

    public SqlAlterTableReplaceColumns(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList newColumns) {
        super(pos);
        this.newColumns = newColumns;
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