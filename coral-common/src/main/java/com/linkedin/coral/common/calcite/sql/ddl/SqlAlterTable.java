package com.linkedin.coral.common.calcite.sql.ddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;

public  class SqlAlterTable extends SqlCall {
    public enum AlterTableOperation {
        RENAME, SET_PROPERTIES, SET_SERDE, UNSET_SERDE_PROPERTIES, CLUSTER_SORT, SKEWED,NOT_SKEWED,
        ADD_PARTITION, DROP_PARTITION, RENAME_PARTITION, EXCHANGE_PARTITION, ARCHIVE_PARTITION,
        UNARCHIVE_PARTITION, SET_FILE_FORMAT, SET_LOCATION, TOUCH, COMPACT, CONCATENATE,
        UPDATE_COLUMNS, CHANGE_COLUMN, ADD_COLUMNS, REPLACE_COLUMNS,
        CHANGE_PARTITION_COLUMN_TYPE,
        DROP_CONSTRAINT,
        UPDATE_COLUMN_STATS,
        UPDATE_TABLE_STATS,
        DROP_PROPERTIES,
        SET_SKEWED_LOCATION,
        MERGE_FILES,
        SET_BUCKETS,
        SET_OWNER,
        SET_PARTITION_SPEC,
        CONVERT_TABLE,
        EXECUTE,
        DROP_BRANCH,
        CREATE_BRANCH,
        DROP_TAG,
        CREATE_TAG,
        RENAME_COLUMN
    }

    private final SqlIdentifier tableName;
    private final AlterTableOperation operation;
    private final SqlIdentifier newTableName;
    private final SqlNodeList properties;
    private final SqlNode serdeName;
    private final SqlNodeList serdeProperties;
    private final SqlNodeList clusterCols;
    private final SqlNodeList sortCols;
    private final SqlNode buckets;
    private final SqlNodeList skewedCols;
    private final SqlNodeList skewedValues;
    private final boolean storedAsDirs;
    private final SqlNode partitionSpec;
    private final SqlNode oldPartitionSpec;
    private final SqlNode newPartitionSpec;
    private final SqlNode location;
    private final boolean ifExists;
    private final boolean ifNotExists;
    private final boolean purge;
    private final SqlIdentifier sourceTable;
    private final SqlNode fileFormat;
    private final SqlNode compactionType;
    private final SqlNodeList compactionProps;
    private final SqlIdentifier oldColName;
    private final SqlIdentifier newColName;
    private final SqlDataTypeSpec newColType;
    private final SqlNode colComment;
    private final SqlIdentifier afterCol;
    private final SqlNodeList newColumns;
    private final SqlIdentifier constraintName;
    private final SqlNodeList columnStats;
    private final SqlNodeList tableStats;
    private final SqlNodeList dropProperties;
    private final SqlNodeList skewedLocations;
    private final SqlIdentifier newOwner;
    private final SqlNode convertType;
    private final SqlNode executeStatement;
    private final SqlIdentifier branchName;
    private final SqlIdentifier tagName;

    public SqlAlterTable(SqlParserPos pos, SqlIdentifier tableName, AlterTableOperation operation, SqlIdentifier newTableName, SqlNodeList properties, SqlNode serdeName, SqlNodeList serdeProperties, SqlNodeList clusterCols, SqlNodeList sortCols, SqlNode buckets, SqlNodeList skewedCols, SqlNodeList skewedValues, boolean storedAsDirs, SqlNode partitionSpec, SqlNode oldPartitionSpec, SqlNode newPartitionSpec, SqlNode location, boolean ifExists, boolean ifNotExists, boolean purge, SqlIdentifier sourceTable, SqlNode fileFormat, SqlNode compactionType, SqlNodeList compactionProps, SqlIdentifier oldColName, SqlIdentifier newColName, SqlDataTypeSpec newColType, SqlNode colComment, SqlIdentifier afterCol, SqlNodeList newColumns, SqlIdentifier constraintName, SqlNodeList columnStats, SqlNodeList tableStats, SqlNodeList dropProperties, SqlNodeList skewedLocations, SqlIdentifier newOwner, SqlNode convertType, SqlNode executeStatement, SqlIdentifier branchName, SqlIdentifier tagName) {
        super(pos);
        this.tableName = tableName;
        this.operation = operation;
        this.newTableName = newTableName;
        this.properties = properties;
        this.serdeName = serdeName;
        this.serdeProperties = serdeProperties;
        this.clusterCols = clusterCols;
        this.sortCols = sortCols;
        this.buckets = buckets;
        this.skewedCols = skewedCols;
        this.skewedValues = skewedValues;
        this.storedAsDirs = storedAsDirs;
        this.partitionSpec = partitionSpec;
        this.oldPartitionSpec = oldPartitionSpec;
        this.newPartitionSpec = newPartitionSpec;
        this.location = location;
        this.ifExists = ifExists;
        this.ifNotExists = ifNotExists;
        this.purge = purge;
        this.sourceTable = sourceTable;
        this.fileFormat = fileFormat;
        this.compactionType = compactionType;
        this.compactionProps = compactionProps;
        this.oldColName = oldColName;
        this.newColName = newColName;
        this.newColType = newColType;
        this.colComment = colComment;
        this.afterCol = afterCol;
        this.newColumns = newColumns;
        this.constraintName = constraintName;
        this.columnStats = columnStats;
        this.tableStats = tableStats;
        this.dropProperties = dropProperties;
        this.skewedLocations = skewedLocations;
        this.newOwner = newOwner;
        this.convertType = convertType;
        this.executeStatement = executeStatement;
        this.branchName = branchName;
        this.tagName = tagName;
    }

    @Override
    public SqlOperator getOperator() {
        return new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE) {
            @Override
            public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                      SqlParserPos pos,
                                      @Nullable SqlNode... operands) {
                if (operands.length < 2) {
                    throw new IllegalArgumentException("ALTER TABLE must have at least 2 operands");
                }

                SqlIdentifier tableName = (SqlIdentifier) operands[0];
                AlterTableOperation operation = AlterTableOperation.valueOf(((SqlLiteral) operands[1]).getStringValue());
                SqlIdentifier newTableName = (SqlIdentifier) operands[2];
                SqlNodeList properties = (SqlNodeList) operands[3];
                SqlNode serdeName = operands[4];
                SqlNodeList serdeProperties = (SqlNodeList) operands[5];
                SqlNodeList clusterCols = (SqlNodeList) operands[6];
                SqlNodeList sortCols = (SqlNodeList) operands[7];
                SqlNode buckets = operands[8];
                SqlNodeList skewedCols = (SqlNodeList) operands[9];
                SqlNodeList skewedValues = (SqlNodeList) operands[10];
                boolean storedAsDirs = ((SqlLiteral) operands[11]).booleanValue();
                SqlNode partitionSpec = operands[12];
                SqlNode oldPartitionSpec = operands[13];
                SqlNode newPartitionSpec = operands[14];
                SqlNode location = operands[15];
                boolean ifExists = ((SqlLiteral) operands[16]).booleanValue();
                boolean ifNotExists = ((SqlLiteral) operands[17]).booleanValue();
                boolean purge = ((SqlLiteral) operands[18]).booleanValue();
                SqlIdentifier sourceTable = (SqlIdentifier) operands[19];
                SqlNode fileFormat = operands[20];
                SqlNode compactionType = operands[21];
                SqlNodeList compactionProps = (SqlNodeList) operands[22];
                SqlIdentifier oldColName = (SqlIdentifier) operands[23];
                SqlIdentifier newColName = (SqlIdentifier) operands[24];
                SqlDataTypeSpec newColType = (SqlDataTypeSpec) operands[25];
                SqlNode colComment = operands[26];
                SqlIdentifier afterCol = (SqlIdentifier) operands[27];
                SqlNodeList newColumns = (SqlNodeList) operands[28];
                SqlIdentifier constraintName = (SqlIdentifier) operands[29];
                SqlNodeList columnStats = (SqlNodeList) operands[30];
                SqlNodeList tableStats = (SqlNodeList) operands[31];
                SqlNodeList dropProperties = (SqlNodeList) operands[32];
                SqlNodeList skewedLocations = (SqlNodeList) operands[33];
                SqlIdentifier newOwner = (SqlIdentifier) operands[34];
                SqlNode convertType = operands[35];
                SqlNode executeStatement = operands[36];
                SqlIdentifier branchName = (SqlIdentifier) operands[37];
                SqlIdentifier tagName = (SqlIdentifier) operands[38];

                return new SqlAlterTable(pos, tableName, operation, newTableName, properties,
                        serdeName, serdeProperties, clusterCols, sortCols, buckets, skewedCols,
                        skewedValues, storedAsDirs, partitionSpec, oldPartitionSpec, newPartitionSpec,
                        location, ifExists, ifNotExists, purge, sourceTable, fileFormat, compactionType,
                        compactionProps, oldColName, newColName, newColType, colComment, afterCol,
                        newColumns, constraintName, columnStats, tableStats, dropProperties,
                        skewedLocations, newOwner, convertType, executeStatement, branchName, tagName);
            }
        };
    }


    @Override
    public List<SqlNode> getOperandList() {
//        return ImmutableList.<SqlNode>builder()
//                .add(tableName)
//                .add()
//                .addAll(operands)
//                .build();

        return ImmutableNullableList.of(tableName, SqlLiteral.createSymbol(operation, ZERO),newTableName,properties,serdeName,serdeProperties,
                clusterCols,sortCols,buckets,skewedCols,skewedValues,SqlLiteral.createBoolean(storedAsDirs,ZERO),partitionSpec,oldPartitionSpec,
                newPartitionSpec,location,SqlLiteral.createBoolean(ifExists,ZERO),SqlLiteral.createBoolean(ifNotExists,ZERO),SqlLiteral.createBoolean(purge,ZERO) ,sourceTable,fileFormat,compactionType,compactionProps,
                oldColName,newColName,newColType,colComment,afterCol,newColumns,constraintName,
                columnStats,tableStats,dropProperties,skewedLocations,newOwner,convertType,executeStatement,branchName,tagName
                );
    }

//    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
//        writer.keyword("ALTER TABLE");
//        tableName.unparse(writer, leftPrec, rightPrec);
//
//        switch (operation) {
//            case RENAME:
//                writer.keyword("RENAME TO");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case SET_PROPERTIES:
//                writer.keyword("SET TBLPROPERTIES");
//                SqlWriter.Frame frame = writer.startList("(", ")");
//                for (int i = 0; i < operands.size(); i++) {
//                    if (i > 0) {
//                        writer.print(",");
//                        writer.newlineAndIndent();
//                    }
//                    operands.get(i).unparse(writer, leftPrec, rightPrec);
//                }
//                writer.endList(frame);
//                break;
//            case SET_SERDE:
//                writer.keyword("SET SERDE");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                if (operands.size() > 1) {
//                    writer.keyword("WITH SERDEPROPERTIES");
//                    frame = writer.startList("(", ")");
//                    operands.get(1).unparse(writer, leftPrec, rightPrec);
//                    writer.endList(frame);
//                }
//                break;
//            case UNSET_SERDE_PROPERTIES:
//                writer.keyword("UNSET SERDEPROPERTIES");
//                frame = writer.startList("(", ")");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                writer.endList(frame);
//                break;
//            case CLUSTER_SORT:
//                writer.keyword("CLUSTERED BY");
//                frame = writer.startList("(", ")");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                writer.endList(frame);
//                if (operands.size() > 2) {
//                    writer.keyword("SORTED BY");
//                    frame = writer.startList("(", ")");
//                    operands.get(1).unparse(writer, leftPrec, rightPrec);
//                    writer.endList(frame);
//                }
//                writer.keyword("INTO");
//                operands.get(operands.size() - 1).unparse(writer, leftPrec, rightPrec);
//                writer.keyword("BUCKETS");
//                break;
//            case SKEWED:
//                if (operands.isEmpty()) {
//                    writer.keyword("NOT SKEWED");
//                } else {
//                    writer.keyword("SKEWED BY");
//                    frame = writer.startList("(", ")");
//                    operands.get(0).unparse(writer, leftPrec, rightPrec);
//                    writer.endList(frame);
//                    writer.keyword("ON");
//                    frame = writer.startList("(", ")");
//                    operands.get(1).unparse(writer, leftPrec, rightPrec);
//                    writer.endList(frame);
//                    if (operands.size() > 2 && ((SqlLiteral) operands.get(2)).booleanValue()) {
//                        writer.keyword("STORED AS DIRECTORIES");
//                    }
//                }
//                break;
//            case ADD_PARTITION:
//                writer.keyword("ADD");
//                if (((SqlLiteral) operands.get(0)).booleanValue()) {
//                    writer.keyword("IF NOT EXISTS");
//                }
//                writer.keyword("PARTITION");
//                operands.get(1).unparse(writer, leftPrec, rightPrec);
//                if (operands.size() > 2) {
//                    writer.keyword("LOCATION");
//                    operands.get(2).unparse(writer, leftPrec, rightPrec);
//                }
//                break;
//            case DROP_PARTITION:
//                writer.keyword("DROP");
//                if (((SqlLiteral) operands.get(0)).booleanValue()) {
//                    writer.keyword("IF EXISTS");
//                }
//                writer.keyword("PARTITION");
//                operands.get(1).unparse(writer, leftPrec, rightPrec);
//                if (operands.size() > 2 && ((SqlLiteral) operands.get(2)).booleanValue()) {
//                    writer.keyword("PURGE");
//                }
//                break;
//            case RENAME_PARTITION:
//                writer.keyword("PARTITION");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                writer.keyword("RENAME TO PARTITION");
//                operands.get(1).unparse(writer, leftPrec, rightPrec);
//                break;
//            case EXCHANGE_PARTITION:
//                writer.keyword("EXCHANGE PARTITION");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                writer.keyword("WITH TABLE");
//                operands.get(1).unparse(writer, leftPrec, rightPrec);
//                break;
//            case ARCHIVE_PARTITION:
//                writer.keyword("ARCHIVE PARTITION");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case UNARCHIVE_PARTITION:
//                writer.keyword("UNARCHIVE PARTITION");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case SET_FILE_FORMAT:
//                if (operands.size() > 1) {
//                    writer.keyword("PARTITION");
//                    operands.get(0).unparse(writer, leftPrec, rightPrec);
//                }
//                writer.keyword("SET FILEFORMAT");
//                operands.get(operands.size() - 1).unparse(writer, leftPrec, rightPrec);
//                break;
//            case SET_LOCATION:
//                if (operands.size() > 1) {
//                    writer.keyword("PARTITION");
//                    operands.get(0).unparse(writer, leftPrec, rightPrec);
//                }
//                writer.keyword("SET LOCATION");
//                operands.get(operands.size() - 1).unparse(writer, leftPrec, rightPrec);
//                break;
//            case TOUCH:
//                writer.keyword("TOUCH");
//                if (!operands.isEmpty()) {
//                    writer.keyword("PARTITION");
//                    operands.get(0).unparse(writer, leftPrec, rightPrec);
//                }
//                break;
//            case COMPACT:
//                writer.keyword("COMPACT");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                if (operands.size() > 1) {
//                    writer.keyword("WITH OVERWRITE TBLPROPERTIES");
//                    frame = writer.startList("(", ")");
//                    operands.get(1).unparse(writer, leftPrec, rightPrec);
//                    writer.endList(frame);
//                }
//                break;
//            case CONCATENATE:
//                writer.keyword("CONCATENATE");
//                if (!operands.isEmpty()) {
//                    writer.keyword("PARTITION");
//                    operands.get(0).unparse(writer, leftPrec, rightPrec);
//                }
//                break;
//            case UPDATE_COLUMNS:
//                if (!operands.isEmpty()) {
//                    writer.keyword("PARTITION");
//                    operands.get(0).unparse(writer, leftPrec, rightPrec);
//                }
//                writer.keyword("UPDATE COLUMNS");
//                break;
//            case CHANGE_COLUMN:
//                if (operands.size() > 5) {
//                    writer.keyword("PARTITION");
//                    operands.get(0).unparse(writer, leftPrec, rightPrec);
//                }
//                writer.keyword("CHANGE COLUMN");
//                operands.get(operands.size() - 5).unparse(writer, leftPrec, rightPrec);
//                operands.get(operands.size() - 4).unparse(writer, leftPrec, rightPrec);
//                operands.get(operands.size() - 3).unparse(writer, leftPrec, rightPrec);
//                if (operands.get(operands.size() - 2) != null) {
//                    writer.keyword("COMMENT");
//                    operands.get(operands.size() - 2).unparse(writer, leftPrec, rightPrec);
//                }
//                if (operands.get(operands.size() - 1) != null) {
//                    writer.keyword("AFTER");
//                    operands.get(operands.size() - 1).unparse(writer, leftPrec, rightPrec);
//                }
//                break;
//            case ADD_COLUMNS:
//                if (operands.size() > 1) {
//                    writer.keyword("PARTITION");
//                    operands.get(0).unparse(writer, leftPrec, rightPrec);
//                }
//                writer.keyword("ADD COLUMNS");
//                frame = writer.startList("(", ")");
//                operands.get(operands.size() - 1).unparse(writer, leftPrec, rightPrec);
//                writer.endList(frame);
//                break;
//            case REPLACE_COLUMNS:
//                if (operands.size() > 1) {
//                    writer.keyword("PARTITION");
//                    operands.get(0).unparse(writer, leftPrec, rightPrec);
//                }
//                writer.keyword("REPLACE COLUMNS");
//                frame = writer.startList("(", ")");
//                operands.get(operands.size() - 1).unparse(writer, leftPrec, rightPrec);
//                writer.endList(frame);
//                break;
//            case DROP_CONSTRAINT:
//                writer.keyword("DROP CONSTRAINT");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case UPDATE_COLUMN_STATS:
//                writer.keyword("UPDATE COLUMN STATISTICS");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case UPDATE_TABLE_STATS:
//                writer.keyword("UPDATE STATISTICS");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case DROP_PROPERTIES:
//                writer.keyword("UNSET TBLPROPERTIES");
//                frame = writer.startList("(", ")");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                writer.endList(frame);
//                break;
//            case SET_SKEWED_LOCATION:
//                writer.keyword("SET SKEWED LOCATION");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case SET_BUCKETS:
//                writer.keyword("INTO");
//                operands.get(1).unparse(writer, leftPrec, rightPrec);
//                writer.keyword("BUCKETS");
//                break;
//            case SET_OWNER:
//                writer.keyword("SET OWNER");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case SET_PARTITION_SPEC:
//                writer.keyword("SET PARTITION SPEC");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case CONVERT_TABLE:
//                writer.keyword("CONVERT TO");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case EXECUTE:
//                writer.keyword("EXECUTE");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case DROP_BRANCH:
//                writer.keyword("DROP BRANCH");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case CREATE_BRANCH:
//                writer.keyword("CREATE BRANCH");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case DROP_TAG:
//                writer.keyword("DROP TAG");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case CREATE_TAG:
//                writer.keyword("CREATE TAG");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                break;
//            case RENAME_COLUMN:
//                writer.keyword("CHANGE COLUMN");
//                operands.get(0).unparse(writer, leftPrec, rightPrec);
//                operands.get(1).unparse(writer, leftPrec, rightPrec);
//                operands.get(2).unparse(writer, leftPrec, rightPrec);
//                if (operands.size() > 3 && operands.get(3) != null) {
//                    writer.keyword("COMMENT");
//                    operands.get(3).unparse(writer, leftPrec, rightPrec);
//                }
//                break;
//            default:
//                throw new UnsupportedOperationException("Unsupported ALTER TABLE operation: " + operation);
//        }
//    }

}
