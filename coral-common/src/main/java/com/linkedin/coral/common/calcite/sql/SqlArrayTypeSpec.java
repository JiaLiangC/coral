package com.linkedin.coral.common.calcite.sql;

import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Objects;


/**
 * SQL array type specification
 */
public class SqlArrayTypeSpec extends SqlDataTypeSpec {

    private final SqlDataTypeSpec typeSpec;

    public SqlArrayTypeSpec(SqlDataTypeSpec type, SqlParserPos pos) {
        this(type, null, pos);
    }

    public SqlArrayTypeSpec(SqlDataTypeSpec type, Boolean nullable, SqlParserPos pos) {
        super(new SqlCollectionTypeNameSpec(type.getTypeNameSpec(), SqlTypeName.ARRAY, pos),
                null, nullable, pos);
        this.typeSpec = type;
    }

    public SqlDataTypeSpec getElementTypeSpec() {
        return typeSpec;
    }

    @Override public SqlNode clone(SqlParserPos pos) {
        return new SqlArrayTypeSpec(this.typeSpec, getNullable(), pos);
    }

    @Override public SqlDataTypeSpec withNullable(Boolean nullable) {
        if (Objects.equals(getNullable(), nullable)) {
            return this;
        }
        return new SqlArrayTypeSpec(typeSpec, getNullable(), getParserPosition());
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getTypeName().getSimple());
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "<", ">");
        writer.setNeedWhitespace(false);
        typeSpec.unparse(writer, leftPrec, rightPrec);
        writer.setNeedWhitespace(false);
        writer.endList(frame);
    }
}

// End SqlArrayTypeSpec.java