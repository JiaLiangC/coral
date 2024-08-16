package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterTableSetSkewedLocation extends SqlCall {
    private final SqlNodeList skewedLocations;

    public SqlAlterTableSetSkewedLocation(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList skewedLocations) {
        super(pos);
        this.skewedLocations = skewedLocations;
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
