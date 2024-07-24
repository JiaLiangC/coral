package com.linkedin.coral.common.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Objects;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlMapTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Class to capture SQL map type specification.
 */
public class SqlMapTypeSpec extends SqlDataTypeSpec {

    public SqlMapTypeSpec(SqlDataTypeSpec keyType, SqlDataTypeSpec valType, SqlParserPos pos) {
        this(keyType, valType, null, pos);
    }

    public SqlMapTypeSpec(SqlDataTypeSpec keyType, SqlDataTypeSpec valType,
                          Boolean nullable, SqlParserPos pos) {
        this(new SqlMapTypeNameSpec(keyType, valType,pos), nullable, pos);
    }

    private SqlMapTypeSpec(SqlMapTypeNameSpec mapTypeNameSpec, Boolean nullable, SqlParserPos pos) {
        super(mapTypeNameSpec, null, nullable, pos);
    }

    public SqlDataTypeSpec getKeyTypeSpec() {
        return ((SqlMapTypeNameSpec) getTypeNameSpec()).getKeyType();
    }

    public SqlDataTypeSpec getValueTypeSpec() {
        return ((SqlMapTypeNameSpec) getTypeNameSpec()).getValType();
    }

    @Override public SqlNode clone(SqlParserPos pos) {
        return new SqlMapTypeSpec((SqlMapTypeNameSpec) getTypeNameSpec(),
                getNullable(), getParserPosition());
    }

    @Override public SqlDataTypeSpec withNullable(Boolean nullable) {
        if (Objects.equals(getNullable(), nullable)) {
            return this;
        }
        return new SqlMapTypeSpec((SqlMapTypeNameSpec) getTypeNameSpec(), nullable,
                getParserPosition());
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        getTypeNameSpec().unparse(writer, leftPrec, rightPrec);
    }
}

// End SqlMapTypeSpec.java
