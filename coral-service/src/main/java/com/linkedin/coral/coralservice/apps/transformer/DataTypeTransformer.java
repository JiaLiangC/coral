/**
 * Copyright 2022-2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.coralservice.apps.transformer;

import com.google.common.collect.ImmutableList;
import com.linkedin.coral.common.calcite.sql.CustomSqlTypeNameSpec;
import com.linkedin.coral.common.calcite.sql.SqlArrayTypeSpec;
import com.linkedin.coral.common.calcite.sql.SqlStructType;
import com.linkedin.coral.common.calcite.sql.ddl.SqlTableColumn;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;


/**
 * Transformer to convert SqlCall from array[i] to array[i+1] to ensure array indexes start at 1.
 */
public class DataTypeTransformer extends SqlNodeTransformer {

    public DataTypeTransformer() {
        super(HiveSqlDialect.DEFAULT, SparkSqlDialect.DEFAULT);
    }

    @Override
    public boolean condition(SqlNode sqlnode) {
        return sqlnode instanceof SqlDataTypeSpec;
    }


    private static SqlTypeName getSqlTypeFromIdentifier(SqlIdentifier identifier) {
        if (identifier == null) {
            return null;
        }
        String typeString = identifier.getSimple();
        try {
            return SqlTypeName.valueOf(typeString.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(String.format(
                    "Unable to convert SqlIdentifier '%s' to SqlTypeName. " +
                            "Valid SqlTypeNames are: %s",
                    typeString,
                    String.join(", ", Arrays.stream(SqlTypeName.values())
                            .map(Enum::name)
                            .collect(Collectors.toList()))
            ), e);
        }
    }

    @Override
    public SqlNode transform(SqlNode sqlNode) {
        try {
            if (sqlNode instanceof SqlDataTypeSpec) {
                SqlDataTypeSpec typeSpec = (SqlDataTypeSpec) sqlNode;
                SqlDataTypeSpec newSqlDataTypeSpec = DataTypeConverter.convertDataType(typeSpec);
                return newSqlDataTypeSpec;
            }else {
                if (sqlNode instanceof SqlStructType){
                    System.out.println("struct");
                }
            }

        }catch (Exception e){
            System.out.println("xxx");
        }
        return sqlNode;

    }

    private enum DataTypeConverter {


        STRUCT("STRUCT") {
            @Override
            SqlDataTypeSpec convert(SqlDataTypeSpec sqlDataTypeSpec) {
                return buildStructDataTypeString(sqlDataTypeSpec);
            }
        },

        ARRAY(SqlTypeName.ARRAY) {
            @Override
            SqlDataTypeSpec convert(SqlDataTypeSpec sqlDataTypeSpec) {
                return buildArrayDataTypeString(sqlDataTypeSpec);
            }
        },
        MAP(SqlTypeName.MAP) {
            @Override
            SqlDataTypeSpec convert(SqlDataTypeSpec sqlDataTypeSpec) {
                return buildMapDataTypeString(sqlDataTypeSpec);
            }
        },
        INTEGER(SqlTypeName.INTEGER, "int"),
        SMALLINT(SqlTypeName.SMALLINT, "smallint"),
        TINYINT(SqlTypeName.TINYINT, "tinyint"),
        BIGINT(SqlTypeName.BIGINT, "bigint"),
        DOUBLE(SqlTypeName.DOUBLE, "double"),
        REAL(SqlTypeName.REAL, "float"),
        FLOAT(SqlTypeName.FLOAT, "float"),
        BOOLEAN(SqlTypeName.BOOLEAN, "boolean"),
        CHAR(SqlTypeName.CHAR, "string"),
        VARCHAR(SqlTypeName.VARCHAR, "string"),
        DATE(SqlTypeName.DATE, "date"),
        TIME(SqlTypeName.TIME, "timestamp"),
        TIMESTAMP(SqlTypeName.TIMESTAMP, "timestamp"),
        BINARY(SqlTypeName.BINARY, "binary"),
        VARBINARY(SqlTypeName.VARBINARY, "binary"),
        OTHER(SqlTypeName.OTHER, "binary"),
        INTERVAL_DAY(SqlTypeName.INTERVAL_DAY, "interval"),
        INTERVAL_DAY_HOUR(SqlTypeName.INTERVAL_DAY_HOUR, "interval"),
        INTERVAL_DAY_MINUTE(SqlTypeName.INTERVAL_DAY_MINUTE, "interval"),
        INTERVAL_DAY_SECOND(SqlTypeName.INTERVAL_DAY_SECOND, "interval"),
        INTERVAL_HOUR(SqlTypeName.INTERVAL_HOUR, "interval"),
        INTERVAL_HOUR_MINUTE(SqlTypeName.INTERVAL_HOUR_MINUTE, "interval"),
        INTERVAL_HOUR_SECOND(SqlTypeName.INTERVAL_HOUR_SECOND, "interval"),
        INTERVAL_MINUTE(SqlTypeName.INTERVAL_MINUTE, "interval"),
        INTERVAL_MINUTE_SECOND(SqlTypeName.INTERVAL_MINUTE_SECOND, "interval"),
        INTERVAL_SECOND(SqlTypeName.INTERVAL_SECOND, "interval"),
        INTERVAL_MONTH(SqlTypeName.INTERVAL_MONTH, "interval"),
        INTERVAL_YEAR(SqlTypeName.INTERVAL_YEAR, "interval"),
        INTERVAL_YEAR_MONTH(SqlTypeName.INTERVAL_YEAR_MONTH, "interval"),
        NULL(SqlTypeName.NULL, "null");

        private final String typeName;
        private final String hiveType;



        private static final Set<String> SUPPORTED_TYPES = new HashSet<>();


        static {
            for (DataTypeConverter converter : values()) {
                SUPPORTED_TYPES.add(converter.typeName);
            }
        }

        DataTypeConverter(SqlTypeName typeName, String hiveType) {
            this.typeName = typeName.getName();
            this.hiveType = hiveType;
        }

        DataTypeConverter(String typeName, String hiveType) {
            this.typeName = typeName;
            this.hiveType = hiveType;
        }

        DataTypeConverter(SqlTypeName sqlTypeName) {
            this(sqlTypeName, null);
        }

        DataTypeConverter(String sqlTypeName) {
            this(sqlTypeName, null);
        }

        SqlDataTypeSpec convert(SqlDataTypeSpec sqlDataTypeSpec) {
             SqlTypeNameSpec sqlTypeNameSpec = sqlDataTypeSpec.getTypeNameSpec();
            SqlTypeNameSpec newTypeNameSpec = new CustomSqlTypeNameSpec(
                    hiveType,
                    SqlParserPos.ZERO
            );

            return new SqlDataTypeSpec(
                    newTypeNameSpec,
                    sqlDataTypeSpec.getTimeZone(),
                    sqlDataTypeSpec.getNullable(),
                    SqlParserPos.ZERO
            );

//            return hiveType;
        }

        static DataTypeConverter fromSqlTypeName(String sqlTypeName) {
            for (DataTypeConverter converter : values()) {
                if (converter.typeName.equals(sqlTypeName)) {
                    return converter;
                }
            }
            throw new RuntimeException(String.format(
                    "Unhandled RelDataType %s in Converter from RelDataType to Hive DataType", sqlTypeName));
        }

        /**
         * @param sqlDataTypeSpec a given RelDataType
         * @return a syntactically and semantically correct Hive type string for relDataType
         */
        public static SqlDataTypeSpec convertDataType(SqlDataTypeSpec sqlDataTypeSpec) {
            SqlTypeNameSpec typeNameSpec = sqlDataTypeSpec.getTypeNameSpec();
            SqlIdentifier originalTypeName = typeNameSpec.getTypeName();
            String typeNameString = originalTypeName.getSimple();


             if (!DataTypeConverter.isSupportedTypeEnumSet(typeNameString)){
                 throw new RuntimeException(String.format(
                     "Unsupported RelDataType %s in Converter from RelDataType to Hive DataType", typeNameString));
             }

            return DataTypeConverter.fromSqlTypeName(typeNameString).convert(sqlDataTypeSpec);
        }



        // Method using EnumSet
        static boolean isSupportedTypeEnumSet(String sqlTypeName) {

            return SUPPORTED_TYPES.contains(sqlTypeName);
        }


        private static SqlDataTypeSpec buildStructDataTypeString(SqlDataTypeSpec sqlDataTypeSpec) {
            SqlNodeList nodes = ((SqlStructType) sqlDataTypeSpec).getFields();
            SqlNodeList new_childs = SqlNodeList.EMPTY;
            for(SqlNode node : nodes){
                if( node instanceof SqlTableColumn.SqlRegularColumn){
                    SqlTableColumn.SqlRegularColumn old = (SqlTableColumn.SqlRegularColumn)node;
                    SqlDataTypeSpec newSqlDataTypeSpec = DataTypeConverter.convertDataType( old.getType());
                    new_childs.add(new SqlTableColumn.SqlRegularColumn(ZERO, old.getName(), old.getComment(), newSqlDataTypeSpec, old.getConstraint()));
                }else {
                    System.out.println("errrr-------");
                }

            }
            return new SqlStructType(new_childs, sqlDataTypeSpec.getParserPosition());
        }

        /**
         * Build a Hive array string with format:
         *   array<[field_type]>
         * @param sqlDataTypeSpec a given array RelDataType
         * @return a string that represents the given arraySqlType
         */
        private static SqlDataTypeSpec buildArrayDataTypeString(SqlDataTypeSpec sqlDataTypeSpec) {
            SqlDataTypeSpec elementTypeSpec = DataTypeConverter.convertDataType( ((SqlArrayTypeSpec)sqlDataTypeSpec).getElementTypeSpec());
            return new SqlArrayTypeSpec(elementTypeSpec, true, ZERO);
        }

        /**
         * Build a Hive map string with format:
         *   map<[key_type],[value_type]>
         * @param sqlDataTypeSpec a given map RelDataType
         * @return a string that represents the given mapSqlType
         */
        private static SqlDataTypeSpec buildMapDataTypeString(SqlDataTypeSpec sqlDataTypeSpec) {
            SqlMapTypeNameSpec  sqlMapTypeNameSpec= (SqlMapTypeNameSpec) sqlDataTypeSpec.getTypeNameSpec();
            SqlDataTypeSpec keySqlTypeNameSpec = DataTypeConverter.convertDataType(sqlMapTypeNameSpec.getKeyType());;
            SqlDataTypeSpec valSqlTypeNameSpec = DataTypeConverter.convertDataType(sqlMapTypeNameSpec.getValType());;
            SqlMapTypeNameSpec mapTypeSpec = new SqlMapTypeNameSpec(keySqlTypeNameSpec, valSqlTypeNameSpec, ZERO);
            return new SqlDataTypeSpec(mapTypeSpec, ZERO);
        }
    }

    /**
     * Transforms a RelDataType to a Hive type string such that it is parseable and semantically correct.
     * Some example of Hive type string for a RelDataType are as follows:
     *
     * Example 1:
     * RelDataType:
     *   struct(s1:integer,s2:varchar)
     * Hive Type String:
     *   struct&lt;s1:int,s2:string&gt;
     *
     * Example 2:
     * RelDataType:
     *   map(varchar,struct(s1:integer,s2:varchar))
     * Hive Type String:
     *   map&lt;string,struct&lt;s1:int,s2:string&gt;&gt;
     *
     * Example 3:
     * RelDataType:
     *   array(struct(s1:integer,s2:varchar))
     * Hive Type String:
     *   array&lt;struct&lt;s1:int,s2:string&gt;&gt;
     */

}