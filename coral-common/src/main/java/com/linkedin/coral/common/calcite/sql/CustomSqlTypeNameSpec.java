package com.linkedin.coral.common.calcite.sql;

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class CustomSqlTypeNameSpec extends SqlBasicTypeNameSpec {
    private final String customTypeName;

    public CustomSqlTypeNameSpec(String typeName, SqlParserPos pos) {
        super(SqlTypeName.OTHER, pos);
        this.customTypeName = typeName;
    }

    @Override
    public SqlIdentifier getTypeName() {
        return new SqlIdentifier(customTypeName, getParserPos());
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(customTypeName);
//        if (getPrecision() >= 0) {
//            SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
//            writer.print(Integer.toString(getPrecision()));
//            if (getScale() >= 0) {
//                writer.print(",");
//                writer.print(Integer.toString(getScale()));
//            }
//            writer.endList(frame);
//        }
//        if (getCharSetName() != null) {
//            writer.keyword("CHARACTER SET");
//            writer.identifier(getCharSetName(),true);
//        }
    }

}

