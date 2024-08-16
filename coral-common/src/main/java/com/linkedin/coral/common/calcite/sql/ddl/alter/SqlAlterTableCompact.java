package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
public class SqlAlterTableCompact extends SqlCall {
    private final SqlNode compactionType;
    private final SqlNodeList compactionProps;

    public SqlAlterTableCompact(SqlParserPos pos, SqlIdentifier tableName, SqlNode compactionType, SqlNodeList compactionProps) {
        super(pos);
        this.compactionType = compactionType;
        this.compactionProps = compactionProps;
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