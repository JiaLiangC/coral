package com.linkedin.coral.common.calcite.sql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;

import java.util.List;

public class HiveFunctionSqlNode extends SqlCall {
    private final SqlIdentifier functionName;
    private final SqlNodeList arguments;
    private final SqlOperator operator;

    public HiveFunctionSqlNode(SqlParserPos pos, SqlIdentifier functionName, SqlNodeList arguments) {
        super(pos);
        this.functionName = functionName;
        this.arguments = arguments;
        this.operator = new SqlSpecialOperator(functionName.getSimple(), SqlKind.OTHER_FUNCTION);
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return arguments.getList();
    }

    // Getter methods
    public SqlIdentifier getFunctionName() {
        return functionName;
    }

    public SqlNodeList getArguments() {
        return arguments;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        functionName.unparse(writer, leftPrec, rightPrec);
        final SqlWriter.Frame frame = writer.startList("(", ")");
        for (SqlNode operand : arguments) {
            writer.sep(",");
            operand.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }
}
