package com.linkedin.coral.coralservice.apps;

import org.antlr.runtime.tree.Tree;
import com.linkedin.coral.hive.hive2rel.parsetree.parser.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.util.ArrayList;
import java.util.List;

public class HiveAstPrinter {

    private static final String INDENT = "  ";
    private static final String BRANCH = "│";
    private static final String BRANCH_CORNER = "└";
    private static final String BRANCH_TEE = "├";

//    public static void printAst(String sql) throws ParseException {
//        ParseDriver pd = new ParseDriver();
//        ASTNode ast = pd.parse(sql);
//        printNode(ast, 0);
//    }

    public static void printAst(ASTNode ast) throws ParseException {
        printNode(ast, 0);
    }

    public static void printAstTree(ASTNode ast) throws ParseException {
        printNodeTree(ast, "", true);
    }

//
//    public static void printAstTree(String sql) throws ParseException {
//        ParseDriver pd = new ParseDriver();
//        ASTNode ast = pd.parse(sql);
//        printNodeTree(ast, "", true);
//    }

    private static void printNode(Tree node, int depth) {
        System.out.println(getIndent(depth) + getNodeText(node));
        for (int i = 0; i < node.getChildCount(); i++) {
            printNode(node.getChild(i), depth + 1);
        }
    }

    private static void printNodeTree(Tree node, String prefix, boolean isLast) {
        System.out.println(prefix + (isLast ? BRANCH_CORNER : BRANCH_TEE) + getNodeText(node));

        List<Tree> children = new ArrayList<>();
        for (int i = 0; i < node.getChildCount(); i++) {
            children.add(node.getChild(i));
        }

        for (int i = 0; i < children.size(); i++) {
            String newPrefix = prefix + (isLast ? "    " : BRANCH + "   ");
            printNodeTree(children.get(i), newPrefix, i == children.size() - 1);
        }
    }

    private static String getIndent(int depth) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append(INDENT);
        }
        return sb.toString();
    }

    private static String getNodeText(Tree node) {
        if (node instanceof ASTNode) {
            ASTNode astNode = (ASTNode) node;
            return String.format("%s (%d)", astNode.getText(), astNode.getType());
        } else {
            return node.getText();
        }
    }

//    public static void main(String[] args) {
//        String sql = "SELECT id, name FROM users WHERE age > 18";
//        try {
//            System.out.println("AST for SQL: " + sql);
//            System.out.println("\nSimple tree format:");
//            printAst(sql);
//            System.out.println("\nDetailed tree format:");
//            printAstTree(sql);
//        } catch (ParseException e) {
//            System.err.println("Error parsing SQL: " + e.getMessage());
//        }
//    }
}
