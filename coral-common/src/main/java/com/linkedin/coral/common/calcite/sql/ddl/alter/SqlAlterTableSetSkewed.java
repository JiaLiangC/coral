package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;
import java.util.List;

public class SqlAlterTableSetSkewed extends SqlCall {
    private final SqlNodeList skewedColumns;
    private final SqlNode skewedValues;
    private final SqlNode skewedValuePair;
    private final SqlNode storedAsDirectories;
    private final boolean notSkewed;

    public SqlAlterTableSetSkewed(SqlParserPos pos, SqlNodeList skewedColumns, SqlNode skewedValues, SqlNode skewedValuePair, SqlNode storedAsDirectories,boolean notSkewed) {
        super(pos);
        this.skewedColumns = skewedColumns;
        this.skewedValues = skewedValues;
        this.skewedValuePair = skewedValuePair;
        this.storedAsDirectories = storedAsDirectories;
        this.notSkewed = notSkewed;
    }

    public SqlOperator getOperator() {
        return new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE) {
            @Override
            public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                      SqlParserPos pos,
                                      @Nullable SqlNode... operands) {
                assert operands != null;
                SqlNodeList skewedColumns = (SqlNodeList) operands[0];
                SqlNode skewedValues = (SqlNode) operands[1];
                SqlNode skewedValuePair = (SqlNode) operands[2];
                SqlNode storedAsDirectories = (SqlNode) operands[3];
                boolean notSkewed = operands[3] != null && ((SqlLiteral) operands[3]).booleanValue();
                return new SqlAlterTableSetSkewed(pos, skewedColumns, skewedValues,skewedValuePair,storedAsDirectories, notSkewed);
            }
        };
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(skewedColumns, skewedValues,skewedValuePair,storedAsDirectories);
    }


    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if(notSkewed){
            writer.keyword("NOT SKEWED");
        }else {
            writer.keyword("SKEWED BY");
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (int i = 0; i < skewedColumns.size(); i++) {
                SqlNode property = skewedColumns.get(i);
                if (i > 0) {
                    writer.print(",");
                }
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.endList(frame);

            if (skewedValuePair != null) {
                writer.keyword("ON");
                skewedValuePair.unparse(writer, leftPrec, rightPrec);
            } else {
                writer.keyword("ON");
                skewedValues.unparse(writer, leftPrec, rightPrec);
            }

            if (storedAsDirectories != null) {
                storedAsDirectories.unparse(writer, leftPrec, rightPrec);
            }
        }
    }
}
