package com.linkedin.coral.common.calcite.sql.ddl.constraint;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlTableConstraint extends SqlCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlTableConstraint", SqlKind.OTHER);

    private final SqlIdentifier constraintName;
    private final SqlLiteral constraintType;
    private final SqlNodeList columns;
    private final SqlNode condition;
    private final SqlLiteral enable;
    private final SqlLiteral validate;
    private final SqlLiteral rely;
    private final boolean isTableConstraint;

    public SqlTableConstraint(@Nullable SqlIdentifier constraintName, SqlLiteral constraintType,
                              @Nullable SqlNodeList columns, @Nullable SqlNode condition, SqlLiteral enable,
                              SqlLiteral validate, SqlLiteral rely, boolean isTableConstraint, SqlParserPos pos) {
        super(pos);
        this.constraintName = constraintName;
        this.constraintType = constraintType;
        this.columns = columns;
        this.condition = condition;
        this.enable = enable;
        this.validate = validate;
        this.rely = rely;
        this.isTableConstraint = isTableConstraint;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public boolean isPrimaryKey() {
        return this.constraintType.getValueAs(SqlConstraintType.class) == SqlConstraintType.PRIMARY_KEY;
    }

    public boolean isUnique() {
        return this.constraintType.getValueAs(SqlConstraintType.class) == SqlConstraintType.UNIQUE;
    }

    public boolean isCheck() {
        return this.constraintType.getValueAs(SqlConstraintType.class) == SqlConstraintType.CHECK;
    }

    public boolean isEnabled() {
        return this.enable.getValueAs(Boolean.class);
    }

    public boolean isValidated() {
        return this.validate.getValueAs(Boolean.class);
    }

    public boolean isRely() {
        return this.rely.getValueAs(Boolean.class);
    }

    public Optional<String> getConstraintName() {
        return Optional.ofNullable(constraintName).map(SqlIdentifier::getSimple);
    }

    public Optional<SqlIdentifier> getConstraintNameIdentifier() {
        return Optional.ofNullable(constraintName);
    }

    public SqlNodeList getColumns() {
        return columns;
    }

    public SqlNode getCondition() {
        return condition;
    }

    public boolean isTableConstraint() {
        return isTableConstraint;
    }

    public String[] getColumnNames() {
        return columns == null ? new String[0] :
                columns.getList().stream().map(col -> ((SqlIdentifier) col).getSimple()).toArray(String[]::new);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(constraintName, constraintType, columns, condition, enable, validate, rely);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (constraintName != null) {
            writer.keyword("CONSTRAINT");
            constraintName.unparse(writer, leftPrec, rightPrec);
        }
        constraintType.unparse(writer, leftPrec, rightPrec);
        if (isTableConstraint && columns != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode column : columns) {
                writer.sep(",");
                column.unparse(writer, leftPrec, rightPrec);
            }
            writer.endList(frame);
        }
        if (condition != null) {
            writer.keyword("CHECK");
            condition.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword(isEnabled() ? "ENABLE" : "DISABLE");
        writer.keyword(isValidated() ? "VALIDATE" : "NOVALIDATE");
        writer.keyword(isRely() ? "RELY" : "NORELY");
    }

    public enum SqlConstraintType implements SqlLiteral.SqlSymbol {
        PRIMARY_KEY, UNIQUE, CHECK
    }
}
