package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
public class SqlAlterTableChangeColumn extends SqlCall {
    private final SqlIdentifier oldColName;
    private final SqlIdentifier newColName;
    private final SqlDataTypeSpec newColType;
    private final SqlNode colComment;
    private final SqlIdentifier afterCol;

    public SqlAlterTableChangeColumn(SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier oldColName, SqlIdentifier newColName, SqlDataTypeSpec newColType, SqlNode colComment, SqlIdentifier afterCol) {
        super(pos);
        this.oldColName = oldColName;
        this.newColName = newColName;
        this.newColType = newColType;
        this.colComment = colComment;
        this.afterCol = afterCol;
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