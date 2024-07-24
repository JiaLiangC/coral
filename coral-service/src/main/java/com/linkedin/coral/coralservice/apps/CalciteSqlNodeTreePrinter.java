package com.linkedin.coral.coralservice.apps;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class CalciteSqlNodeTreePrinter extends SqlShuttle {

    private StringBuilder sb = new StringBuilder();
    private List<Boolean> indentFlags = new ArrayList<>();

    private int indentLevel = 0;
    private static final String INDENT = "--";

    public String print(SqlNode node) {
        sb.setLength(0);
        indentLevel = 0;
        node.accept(this);
        return sb.toString();
    }


    private void appendPrefix(boolean isLast) {
        for (int i = 0; i < indentFlags.size() - 1; i++) {
            sb.append(indentFlags.get(i) ? "│ " : "  ");
        }
        sb.append(isLast ? "└─ " : "├─ ");
    }


    private String indent() {
        return StringUtils.repeat(INDENT, indentLevel);
    }

    private void appendNodeInfo(SqlNode node, boolean isLast) {
        appendPrefix(isLast);
        sb.append(node.getClass().getSimpleName()).append(": ");
        if (node instanceof SqlCall) {
            sb.append(((SqlCall) node).getOperator().getName());
        } else if (node instanceof SqlIdentifier) {
            sb.append("\"").append(((SqlIdentifier) node).toString()).append("\"");
        } else if (node instanceof SqlLiteral) {
            sb.append(((SqlLiteral) node).toString());
        } else {
            sb.append(node.toString());
        }
        sb.append("\n");
    }
    
    private void visitChildren(SqlCall call) {
        List<SqlNode> operands = call.getOperandList();
        for (int i = 0; i < operands.size(); i++) {
            SqlNode operand = operands.get(i);
            if (operand != null) {
                boolean isLast = (i == operands.size() - 1);
                indentFlags.add(!isLast);
                operand.accept(this);
                indentFlags.remove(indentFlags.size() - 1);
            }
        }
    }

    @Override
    public SqlNode visit(SqlLiteral literal) {
        appendNodeInfo(literal, true);
        return literal;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        appendNodeInfo(call,false);
        visitChildren(call);
        return call;
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
        appendNodeInfo(nodeList, false);
        indentFlags.add(true);
        for (int i = 0; i < nodeList.size(); i++) {
            SqlNode node = nodeList.get(i);
            boolean isLast = (i == nodeList.size() - 1);
            indentFlags.set(indentFlags.size() - 1, !isLast);
            node.accept(this);
        }
        indentFlags.remove(indentFlags.size() - 1);
        return nodeList;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        appendNodeInfo(id, true);
        return id;
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
        appendNodeInfo(type, true);
        return type;
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        appendNodeInfo(param,true);
        return param;
    }

    @Override
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
        appendNodeInfo(intervalQualifier, true);
        return intervalQualifier;
    }
}
