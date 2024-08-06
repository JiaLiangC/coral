package com.linkedin.coral.common.calcite.sql.ddl.constraint;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
public class SqlColumnConstraint extends SqlCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlColumnConstraint", SqlKind.OTHER);

    private final SqlLiteral constraintType;
    @Nullable private final SqlNode defaultValue;
    private final SqlNode checkExpression;
    private final SqlLiteral enable;
    private final SqlLiteral validate;
    private final SqlLiteral rely;

    public SqlColumnConstraint(SqlLiteral constraintType, SqlNode defaultValue, SqlNode checkExpression,
                               SqlLiteral enable, SqlLiteral validate, SqlLiteral rely,
                               SqlParserPos pos) {
        super(pos);
        this.constraintType = Objects.requireNonNull(constraintType, "constraintType must not be null");
        this.defaultValue = defaultValue;
        this.checkExpression = checkExpression;
        this.enable = enable;
        this.validate = validate;
        this.rely = rely;
    }



    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }


    public List<SqlNode> getOperandList() {
        return Stream.of(constraintType, defaultValue, checkExpression, enable, validate, rely)
                .filter(Objects::nonNull)
                .collect(ImmutableList.toImmutableList());
    }


    public SqlColumnConstraintType getConstraintType() {
        return constraintType.symbolValue(SqlColumnConstraintType.class);
    }

    public SqlNode getDefaultValue() {
        return defaultValue;
    }

    public SqlNode getCheckExpression() {
        return checkExpression;
    }

    public boolean isEnabled() {
        return enable != null && enable.booleanValue();
    }

    public boolean isValidated() {
        return validate != null && validate.booleanValue();
    }

    public boolean isRely() {
        return rely != null && rely.booleanValue();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getConstraintType().name());

        if (defaultValue != null) {
//            writer.keyword("DEFAULT");
            defaultValue.unparse(writer, leftPrec, rightPrec);
        }

        if (checkExpression != null) {
//            writer.keyword("CHECK");
            checkExpression.unparse(writer, leftPrec, rightPrec);
        }

        if (enable != null) {
            writer.keyword(isEnabled() ? "ENABLE" : "DISABLE");
        }

        if (validate != null) {
            writer.keyword(isValidated() ? "VALIDATE" : "NOVALIDATE");
        }

        if (rely != null) {
            writer.keyword(isRely() ? "RELY" : "NORELY");
        }
    }

    public enum SqlColumnConstraintType implements SqlLiteral.SqlSymbol {
        PRIMARY_KEY, UNIQUE, NOT_NULL, DEFAULT, CHECK
    }

    // Factory methods for creating different types of constraints
    public static SqlColumnConstraint createPrimaryKey(SqlParserPos pos) {
        return new SqlColumnConstraint(
                SqlLiteral.createSymbol(SqlColumnConstraintType.PRIMARY_KEY, pos),
                null, null,
                SqlLiteral.createBoolean(true, pos),
                SqlLiteral.createBoolean(false, pos),
                SqlLiteral.createBoolean(false, pos),
                pos
        );
    }

    public static SqlColumnConstraint createUnique(SqlParserPos pos) {
        return new SqlColumnConstraint(
                SqlLiteral.createSymbol(SqlColumnConstraintType.UNIQUE, pos),
                null, null,
                SqlLiteral.createBoolean(true, pos),
                SqlLiteral.createBoolean(false, pos),
                SqlLiteral.createBoolean(false, pos),
                pos
        );
    }

    public static SqlColumnConstraint createNotNull(SqlParserPos pos) {
        return new SqlColumnConstraint(
                SqlLiteral.createSymbol(SqlColumnConstraintType.NOT_NULL, pos),
                null, null,
                SqlLiteral.createBoolean(true, pos),
                SqlLiteral.createBoolean(false, pos),
                SqlLiteral.createBoolean(false, pos),
                pos
        );
    }

    public static SqlColumnConstraint createDefault(SqlNode defaultValue, SqlParserPos pos) {
        return new SqlColumnConstraint(
                SqlLiteral.createSymbol(SqlColumnConstraintType.DEFAULT, pos),
                defaultValue, null,
                null, null, null,
                pos
        );
    }

    public static SqlColumnConstraint createCheck(SqlNode checkExpression, SqlParserPos pos) {
        return new SqlColumnConstraint(
                SqlLiteral.createSymbol(SqlColumnConstraintType.CHECK, pos),
                null, checkExpression,
                SqlLiteral.createBoolean(true, pos),
                SqlLiteral.createBoolean(false, pos),
                SqlLiteral.createBoolean(false, pos),
                pos
        );
    }
}
