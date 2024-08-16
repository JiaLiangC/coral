package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;


public class SqlAlterTableSetSerde extends SqlCall {
    private final SqlNode serdeName;
    private final SqlNodeList serdeProperties;

    public SqlAlterTableSetSerde(SqlParserPos pos, SqlNode serdeName, SqlNodeList serdeProperties) {
        super(pos);
        this.serdeName = serdeName;
        this.serdeProperties = serdeProperties;
    }

    public SqlOperator getOperator() {
        return new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE) {
            @Override
            public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                      SqlParserPos pos,
                                      @Nullable SqlNode... operands) {
                assert operands != null;
                SqlNode serdeName = (SqlNode) operands[0];
                SqlNodeList serdeProperties = (SqlNodeList) operands[0];
                return new SqlAlterTableSetSerde(pos, serdeName, serdeProperties);
            }
        };
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(serdeName, serdeProperties);
    }


    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

        writer.keyword("SET SERDE ");
        if (serdeName!=null){
            serdeName.unparse(writer, leftPrec, rightPrec);
        }

        writer.keyword("WITH SERDEPROPERTIES ");
        SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 0; i < serdeProperties.size(); i++) {
            SqlNode property = serdeProperties.get(i);
            if (i > 0) {
                writer.print(",");
            }
            property.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }


}