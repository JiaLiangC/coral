package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlStoredAsDirs extends SqlIdentifier  {

    public SqlStoredAsDirs(String name, SqlParserPos pos) {
        super(name, pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("STORED AS DIRECTORIES");
    }
}
