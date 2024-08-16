package com.linkedin.coral.common.calcite.sql.ddl.alter;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;
import java.util.List;

public class SqlAlterTableBuckets extends SqlCall {

    private final SqlNodeList clusterByList;
    private final SqlNodeList sortedByList;
    private final SqlNode bucketNum;
    public SqlAlterTableBuckets(SqlParserPos pos, SqlNodeList clusterByList, SqlNodeList sortedByList, SqlNode bucketNum) {
        super(pos);
        this.clusterByList=clusterByList;
        this.sortedByList=sortedByList;
        this.bucketNum=bucketNum;
    }


    public SqlOperator getOperator() {
        return new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE) {
            @Override
            public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                      SqlParserPos pos,
                                      @Nullable SqlNode... operands) {
                assert operands != null;
                SqlNodeList clusterByList = (SqlNodeList) operands[0];
                SqlNodeList sortedByList = (SqlNodeList) operands[1];
                SqlNode bucketNum = (SqlNode) operands[2];
                return new SqlAlterTableBuckets(pos, clusterByList, sortedByList,bucketNum);
            }
        };
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(clusterByList, sortedByList,bucketNum);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

        if (clusterByList != null && clusterByList.size() > 0) {
//            writer.newlineAndIndent();
            writer.keyword("CLUSTERED BY");
//            SqlWriter.Frame clusteredByFrame = writer.startList("(", ")");
            clusterByList.unparse(writer, leftPrec, rightPrec);
//            writer.endList(clusteredByFrame);
        }

        if (sortedByList != null && sortedByList.size() > 0) {
            writer.keyword("SORTED BY");
//            SqlWriter.Frame sortedByFrame = writer.startList("(", ")");
            sortedByList.unparse(writer, leftPrec, rightPrec);
//            writer.endList(sortedByFrame);
        }

        if (bucketNum != null) {
            writer.keyword("INTO");
            bucketNum.unparse(writer, leftPrec, rightPrec);
            writer.keyword("BUCKETS");
        }

    }
}
