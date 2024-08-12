package com.linkedin.coral.common.calcite.sql.ddl;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.linkedin.coral.common.calcite.sql.SqlProperty;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableNullableList;

import com.linkedin.coral.common.calcite.sql.ExtendedSqlNode;
import com.linkedin.coral.common.calcite.sql.SqlConstraintValidator;
import com.linkedin.coral.common.calcite.sql.SqlUnparseUtils;
import com.linkedin.coral.common.calcite.sql.ddl.SqlTableColumn.SqlComputedColumn;
import com.linkedin.coral.common.calcite.sql.ddl.SqlTableColumn.SqlRegularColumn;
import com.linkedin.coral.common.calcite.sql.ddl.constraint.SqlTableConstraint;
import com.linkedin.coral.common.calcite.sql.error.SqlValidateException;
import org.apache.derby.iapi.types.SQLBoolean;


public class SqlCreateDatabase extends SqlCall {
    private final SqlIdentifier databaseName;
    private final boolean ifNotExists;
    private final SqlNode comment;
    private final SqlNode location;
    private final SqlNode managedLocation;
    private final SqlNodeList properties;
    private final boolean isSchema;
    private final boolean isRemote;
    private final SqlIdentifier connector;



    public SqlCreateDatabase(
            SqlParserPos pos,
            SqlIdentifier databaseName,
            boolean ifNotExists,
            SqlNode comment,
            SqlNode location,
            SqlNode managedLocation,
            SqlNodeList properties,
            boolean isSchema,
            boolean isRemote,
            SqlIdentifier connector) {
        super(pos);
        this.databaseName = databaseName;
        this.ifNotExists = ifNotExists;
        this.comment = comment;
        this.location = location;
        this.managedLocation = managedLocation;
        this.properties = properties;
        this.isSchema = isSchema;
        this.isRemote = isRemote;
        this.connector = connector;
    }

    @Override
    public SqlOperator getOperator() {
        return new SqlSpecialOperator(isSchema ? "CREATE SCHEMA" : "CREATE DATABASE", SqlKind.CREATE_SCHEMA){
            @Override
            public SqlCall createCall(@Nullable final SqlLiteral functionQualifier,
                                      final SqlParserPos pos,
                                      final @Nullable SqlNode... operands) {
                if (operands.length < 6) {
                    throw new IllegalArgumentException("Invalid number of operands for CREATE TABLE");
                }

                SqlIdentifier databaseName = (SqlIdentifier) operands[0];
                boolean ifNotExists = operands[1] != null && ((SqlLiteral) operands[1]).booleanValue();
                SqlCharStringLiteral comment = (SqlCharStringLiteral) operands[2];
                SqlCharStringLiteral location = (SqlCharStringLiteral) operands[3];
                SqlCharStringLiteral managedLocation = (SqlCharStringLiteral) operands[4];
                SqlNodeList properties = (SqlNodeList) operands[5];
                boolean isSchema = operands[6] != null && ((SqlLiteral) operands[6]).booleanValue();
                boolean isRemote = operands[7] != null && ((SqlLiteral) operands[7]).booleanValue();
                SqlIdentifier connector = (SqlIdentifier) operands[8];

                return new SqlCreateDatabase(pos, databaseName, ifNotExists, comment, location,
                        managedLocation, properties,isSchema,isRemote,connector);
            }
        };
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(databaseName,
                SqlLiteral.createBoolean(ifNotExists, getParserPosition()),
                comment,
                location,
                managedLocation,
                properties,
                SqlLiteral.createBoolean(isSchema, getParserPosition()),
                SqlLiteral.createBoolean(isRemote, getParserPosition()),
                connector);
    }

    // Implement other necessary methods like unparse(), validate(), etc.

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(isSchema ? "CREATE SCHEMA" : "CREATE DATABASE");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        databaseName.unparse(writer, leftPrec, rightPrec);
        if (comment != null) {
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }

        if (location != null) {
            writer.keyword("LOCATION");
            location.unparse(writer, leftPrec, rightPrec);
        }
        if (managedLocation != null) {
            writer.keyword("MANAGEDLOCATION");
            managedLocation.unparse(writer, leftPrec, rightPrec);
        }

        if (isRemote && connector != null) {
            writer.keyword("USING");
            connector.unparse(writer, leftPrec, rightPrec);
        }

        if (properties != null && properties.size() > 0) {
            writer.keyword("WITH DBPROPERTIES");
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (int i = 0; i < properties.size(); i++) {
                SqlProperty property = (SqlProperty) properties.get(i);
                if (i > 0) {
                    writer.print(",");
                    writer.newlineAndIndent();
                }
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.endList(frame);
        }
    }
}