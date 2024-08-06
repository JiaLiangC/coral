package com.linkedin.coral.coralservice.apps.transformer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.linkedin.coral.common.calcite.sql.ddl.SqlTableColumn;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;

public class MySqlShuttle extends SqlBasicVisitor<@Nullable SqlNode> {
    public MySqlShuttle() {
    }

    public @Nullable SqlNode visit(SqlLiteral literal) {
        return literal;
    }

    public @Nullable SqlNode visit(SqlIdentifier id) {
        return id;
    }

    public @Nullable SqlNode visit(SqlDataTypeSpec type) {
        return type;
    }

    public @Nullable SqlNode visit(SqlDynamicParam param) {
        return param;
    }

    public @Nullable SqlNode visit(SqlIntervalQualifier intervalQualifier) {
        return intervalQualifier;
    }

    public @Nullable SqlNode visit(SqlCall call) {
        MySqlShuttle.CallCopyingArgHandler argHandler = new MySqlShuttle.CallCopyingArgHandler(call, false);
        call.getOperator().acceptCall(this, call, false, argHandler);
        return argHandler.result();
    }

//    public SqlNode visit(SqlNodeList nodeList) {
//        boolean update = false;
//        List<SqlNode> newList = new ArrayList(nodeList.size());
//
//        SqlNode clonedOperand;
//        for(Iterator var4 = nodeList.iterator(); var4.hasNext(); newList.add(clonedOperand)) {
//            SqlNode operand = (SqlNode)var4.next();
//            if (operand == null) {
//                clonedOperand = null;
//            } else {
//                clonedOperand = (SqlNode)operand.accept(this);
//                if (clonedOperand != operand) {
//                    update = true;
//                }
//            }
//        }
//
//        if (update) {
//            return SqlNodeList.of(nodeList.getParserPosition(), newList);
//        } else {
//            return nodeList;
//        }
//    }

    public SqlNode visit(SqlNodeList nodeList) {
        System.out.println("Entering visit(SqlNodeList) method");
        System.out.println("Original nodeList size: " + nodeList.size());

        boolean update = false;
        List<SqlNode> newList = new ArrayList<>(nodeList.size());
        System.out.println("Created new ArrayList with initial capacity: " + nodeList.size());

        int index = 0;
        for (SqlNode operand : nodeList) {
            System.out.println("Processing node at index: " + index);
            SqlNode clonedOperand;

            if (operand == null) {
                System.out.println("  Operand is null");
                clonedOperand = null;
            } else {
                System.out.println("  Operand type: " + operand.getClass().getSimpleName());
                clonedOperand = operand.accept(this);
                System.out.println("  Cloned operand type: " + (clonedOperand != null ? clonedOperand.getClass().getSimpleName() : "null"));

                if (clonedOperand != operand) {
                    System.out.println("  Operand was modified");
                    update = true;
                } else {
                    System.out.println("  Operand was not modified");
                }
            }

            newList.add(clonedOperand);
            System.out.println("  Added cloned operand to newList");
            index++;
        }

        System.out.println("Finished processing all nodes");
        System.out.println("Update flag: " + update);

        if (update) {
            System.out.println("Creating new SqlNodeList");
            SqlNodeList result = SqlNodeList.of(nodeList.getParserPosition(), newList);
            System.out.println("New SqlNodeList created with size: " + result.size());
            return result;
        } else {
            System.out.println("Returning original nodeList");
            return nodeList;
        }
    }


    protected class CallCopyingArgHandler implements SqlBasicVisitor.ArgHandler<@Nullable SqlNode> {
        boolean update;
        @Nullable SqlNode[] clonedOperands;
        private final SqlCall call;
        private final boolean alwaysCopy;

        public CallCopyingArgHandler(SqlCall call, boolean alwaysCopy) {
            this.call = call;
            this.update = false;
            List<SqlNode> operands = call.getOperandList();
            this.clonedOperands = (SqlNode[])operands.toArray(new SqlNode[0]);
            this.alwaysCopy = alwaysCopy;
        }

//        public SqlNode result() {
//            return !this.update && !this.alwaysCopy ? this.call : this.call.getOperator().createCall(this.call.getFunctionQuantifier(), this.call.getParserPosition(), this.clonedOperands);
//        }

        public SqlNode result() {
            System.out.println("Entering result() method");
            System.out.println("Update flag: " + this.update);
            System.out.println("AlwaysCopy flag: " + this.alwaysCopy);

            if (!this.update && !this.alwaysCopy) {
                System.out.println("No update and not always copy, returning original call");
                System.out.println("Original call: " + this.call);
                return this.call;
            } else {
                System.out.println("Creating new call");
                System.out.println("Operator: " + this.call.getOperator());
                System.out.println("Function Quantifier: " + this.call.getFunctionQuantifier());
                System.out.println("Parser Position: " + this.call.getParserPosition());

                System.out.println("Cloned Operands:");
                for (int i = 0; i < this.clonedOperands.length; i++) {
//                    System.out.println("  Operand " + i + ": " + this.clonedOperands[i]);
                }

                SqlNode newCall = this.call.getOperator().createCall(
                        this.call.getFunctionQuantifier(),
                        this.call.getParserPosition(),
                        this.clonedOperands
                );

//                System.out.println("New call created: " + newCall);
                return newCall;
            }
        }

        public @Nullable SqlNode visitChild(SqlVisitor<@Nullable SqlNode> visitor, SqlNode expr, int i, @Nullable SqlNode operand) {
            System.out.println("Entering visitChild method");
            System.out.println("Visitor type: " + visitor.getClass().getSimpleName());
            System.out.println("Expression type: " + expr.getClass().getSimpleName());
            System.out.println("Index: " + i);
            System.out.println("Operand: " + (operand != null ? operand.getClass().getSimpleName() : "null"));

            if (operand == null) {
                System.out.println("Operand is null, returning null");
                return null;
            } else {
                System.out.println("Processing operand");
                SqlNode newOperand = (SqlNode)operand.accept(MySqlShuttle.this);
                System.out.println("New operand after processing: " + (newOperand != null ? newOperand.getClass().getSimpleName() : "null"));

                if (newOperand != operand) {
                    System.out.println("Operand was modified");
                    this.update = true;
                    System.out.println("Update flag set to true");
                } else {
                    System.out.println("Operand was not modified");
                }

                System.out.println("Storing new operand in clonedOperands array at index " + i);
                this.clonedOperands[i] = newOperand;

                System.out.println("Returning new operand");
                return newOperand;
            }
        }



//        public @Nullable SqlNode visitChild(SqlVisitor<@Nullable SqlNode> visitor, SqlNode expr, int i, @Nullable SqlNode operand) {
//            if (operand == null) {
//                return null;
//            } else {
//                SqlNode newOperand = (SqlNode)operand.accept(MySqlShuttle.this);
//                if (newOperand != operand) {
//                    this.update = true;
//                }
//
//                this.clonedOperands[i] = newOperand;
//                return newOperand;
//            }
//        }
    }
}

