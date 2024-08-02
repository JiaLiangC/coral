package com.linkedin.coral.common.calcite.sql;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;

public class SqlHiveInternalIntervalOperator extends SqlSpecialOperator {

    public static final SqlHiveInternalIntervalOperator INSTANCE = new SqlHiveInternalIntervalOperator();

    private SqlHiveInternalIntervalOperator() {
        super("HIVE_INTERNAL_INTERVAL", SqlKind.INTERVAL, 20, true,
                ReturnTypes.ARG0, InferTypes.FIRST_KNOWN,
                OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY));
    }

    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
        return new SqlBasicCall(this, operands, pos);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlNode functionName = call.operand(0);
        SqlNode intervalType = call.operand(1);
        SqlNode value = call.operand(2);

        if (!"internal_interval".equals(((SqlIdentifier) functionName).getSimple())) {
            throw new IllegalStateException("Unexpected function name for SqlHiveInternalIntervalOperator");
        }

        SqlIntervalQualifier qualifier = getIntervalQualifier(intervalType);


        if (value instanceof SqlCharStringLiteral || value instanceof SqlNumericLiteral ) {
            //intervalLiteral Case: value qualifiers
            writer.keyword("INTERVAL");
            value.unparse(writer, 0, 0);
            writer.keyword(qualifier.toString());
        } else if (value instanceof SqlBasicCall ) {
            // Case:interval (expr) qualifiers
            writer.keyword("INTERVAL");
            writer.print("(");
            value.unparse(writer, 0, 0);
            writer.print(")");
            writer.keyword(qualifier.toString());
        }else {
            throw new IllegalStateException("Unexpected interval expression");
        }
    }

    private SqlIntervalQualifier getIntervalQualifier(SqlNode intervalType) {
        if (!(intervalType instanceof SqlNumericLiteral)) {
            throw new IllegalArgumentException("Interval type must be a numeric literal");
        }
        int typeCode = ((SqlNumericLiteral) intervalType).intValue(true);
        return SqlIntervalQualifier.valueOf(typeCode);
    }

    // You might want to override validate and other methods as needed
}

