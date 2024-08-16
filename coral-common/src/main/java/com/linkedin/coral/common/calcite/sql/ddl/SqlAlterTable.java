package com.linkedin.coral.common.calcite.sql.ddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import com.linkedin.coral.common.calcite.sql.ddl.alter.*;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;

public  class SqlAlterTable extends SqlCall {
    public enum AlterTableOperation {
        RENAME, SET_PROPERTIES, SET_SERDE, UNSET_SERDE_PROPERTIES, CLUSTER_SORT, SKEWED,NOT_SKEWED,
        ADD_PARTITION, DROP_PARTITION, RENAME_PARTITION, EXCHANGE_PARTITION, ARCHIVE_PARTITION,
        UNARCHIVE_PARTITION, SET_PARTITION_FILE_FORMAT,SET_FILE_FORMAT, SET_LOCATION,SET_PARTITION_LOCATION, TOUCH, COMPACT, CONCATENATE,
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

    private final SqlNodeList operands;

    public SqlAlterTable(SqlParserPos pos, SqlNodeList operands ) {
        super(pos);
        this.operands=operands;
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
                return new SqlAlterTable(pos, new SqlNodeList(Arrays.asList(operands),ZERO));
            }
        };
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(operands);
    }


    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

        writer.keyword("ALTER TABLE");
        List<SqlNode> sortedOperands = new ArrayList<>(operands);
        sortedOperands.sort(Comparator.comparingInt(node -> getClausePriority(node).getPriority()));


        for (int i = 0; i < sortedOperands.size(); i++) {
            SqlNode property = sortedOperands.get(i);
            property.unparse(writer, leftPrec, rightPrec);
        }
    }

    private ClausePriority getClausePriority(SqlNode node) {
        if (node instanceof SqlIdentifier) {
            return ClausePriority.TABLE_NAME;
        } else if (node instanceof SqlAlterPartition) {
            return ClausePriority.PARTITION;
        } else if (node instanceof SqlAlterTableBuckets) {
            return ClausePriority.CLUSTERED_BY; // 假设这个类包含CLUSTERED BY和SORTED BY
        } else {
            return ClausePriority.OTHER;
        }
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
