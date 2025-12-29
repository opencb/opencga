/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.clinical.interpreter.tree;

import org.opencb.opencga.analysis.clinical.interpreter.tree.evaluators.LoggingTreeEvaluator;
import org.opencb.opencga.analysis.clinical.interpreter.tree.evaluators.SetTreeEvaluator;
import org.opencb.opencga.analysis.clinical.interpreter.tree.evaluators.TreeEvaluator;
import org.opencb.opencga.analysis.clinical.interpreter.tree.node.OperatorTreeNode;
import org.opencb.opencga.analysis.clinical.interpreter.tree.node.QueryTreeNode;
import org.opencb.opencga.analysis.clinical.interpreter.tree.node.TreeNode;

import java.util.*;

public class TreeParser {

    private TreeNode root;
    private final TreeEvaluator evaluator;

    // Define constants
    private static final Map<String, Integer> OPERATOR_PRECEDENCE = new HashMap<>();
    static {
        OPERATOR_PRECEDENCE.put("OR", 1);
        OPERATOR_PRECEDENCE.put("AND", 2);
        OPERATOR_PRECEDENCE.put("NOT_IN", 3);
    }

    /**
     * Default constructor that uses SetTreeEvaluator for backward compatibility.
     */
    public TreeParser() {
        this(new LoggingTreeEvaluator(new SetTreeEvaluator()));
    }

    /**
     * Constructor that takes a TreeEvaluator for custom evaluation strategies.
     *
     * @param evaluator The evaluator implementation to use for evaluating the tree
     */
    public TreeParser(TreeEvaluator evaluator) {
        if (evaluator == null) {
            throw new IllegalArgumentException("TreeEvaluator cannot be null");
        }
        this.evaluator = evaluator;
    }

    public TreeNode parse(String input) {
        List<String> tokens = tokenize(input);
        Queue<String> rpn = shuntingYard(tokens);
        this.root = buildTree(rpn);
        return this.root;
    }

    /**
     * Step 1: Tokenize
     * Normalizes the string and splits it into a list of atomic parts.
     * Handles the conversion of "NOT IN" to a single token.
     * Supports two query formats:
     * 1. Simple query names: query1, query2, etc.
     * 2. Complex filter expressions: ct=missense;popFreq<0.1
     */
    private List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        int i = 0;

        while (i < input.length()) {
            char c = input.charAt(i);

            // Skip whitespace outside of filter expressions
            if (Character.isWhitespace(c)) {
                if (currentToken.length() > 0) {
                    addToken(tokens, currentToken.toString());
                    currentToken.setLength(0);
                }
                i++;
                continue;
            }

            // Handle opening parenthesis
            if (c == '(') {
                if (currentToken.length() > 0) {
                    addToken(tokens, currentToken.toString());
                    currentToken.setLength(0);
                }
                tokens.add("(");
                i++;
                continue;
            }

            // Handle closing parenthesis
            if (c == ')') {
                if (currentToken.length() > 0) {
                    addToken(tokens, currentToken.toString());
                    currentToken.setLength(0);
                }
                tokens.add(")");
                i++;
                continue;
            }

            // Check for "NOT IN" operator (case-insensitive)
            // We need to check for "NOT" followed by whitespace followed by "IN"
            if (i + 3 <= input.length() && input.substring(i, i + 3).equalsIgnoreCase("NOT")) {
                // Check if followed by whitespace and then "IN"
                int j = i + 3;
                while (j < input.length() && Character.isWhitespace(input.charAt(j))) {
                    j++;
                }
                if (j + 2 <= input.length() && input.substring(j, j + 2).equalsIgnoreCase("IN") &&
                        (j + 2 >= input.length() || Character.isWhitespace(input.charAt(j + 2)) || input.charAt(j + 2) == ')' || input.charAt(j + 2) == '(')) {
                    if (currentToken.length() > 0) {
                        addToken(tokens, currentToken.toString());
                        currentToken.setLength(0);
                    }
                    tokens.add("NOT_IN");
                    i = j + 2;
                    continue;
                }
            }

            // Check for "AND" operator (case-insensitive)
            if (i + 3 <= input.length() && input.substring(i, i + 3).equalsIgnoreCase("AND") &&
                    (i + 3 >= input.length() || Character.isWhitespace(input.charAt(i + 3)) || input.charAt(i + 3) == ')')) {
                if (currentToken.length() > 0) {
                    addToken(tokens, currentToken.toString());
                    currentToken.setLength(0);
                }
                tokens.add("AND");
                i += 3;
                continue;
            }

            // Check for "OR" operator (case-insensitive)
            if (i + 2 <= input.length() && input.substring(i, i + 2).equalsIgnoreCase("OR") &&
                    (i + 2 >= input.length() || Character.isWhitespace(input.charAt(i + 2)) || input.charAt(i + 2) == ')')) {
                if (currentToken.length() > 0) {
                    addToken(tokens, currentToken.toString());
                    currentToken.setLength(0);
                }
                tokens.add("OR");
                i += 2;
                continue;
            }

            // Build current token (query name or filter expression)
            // Filter expressions contain '=' and may contain ';' as delimiters
            currentToken.append(c);
            i++;
        }

        // Add last token if any
        if (currentToken.length() > 0) {
            addToken(tokens, currentToken.toString());
        }

        return tokens;
    }

    /**
     * Helper method to add a token to the list.
     * Validates that the token is not empty and trims it.
     */
    private void addToken(List<String> tokens, String token) {
        token = token.trim();
        if (!token.isEmpty()) {
            tokens.add(token);
        }
    }

    /**
     * Step 2: Shunting-yard Algorithm
     * Converts Infix (A AND B) to Postfix/RPN (A B AND).
     * This handles operator precedence and parentheses automatically.
     */
    private Queue<String> shuntingYard(List<String> tokens) {
        Queue<String> outputQueue = new LinkedList<>();
        Stack<String> operatorStack = new Stack<>();

        for (String token : tokens) {
            String upperToken = token.toUpperCase();

            if (OPERATOR_PRECEDENCE.containsKey(upperToken)) {
                // It is an operator (AND, OR, NOT_IN)
                while (!operatorStack.isEmpty() && !operatorStack.peek().equals("(") &&
                        OPERATOR_PRECEDENCE.getOrDefault(operatorStack.peek(), 0) >= OPERATOR_PRECEDENCE.get(upperToken)) {
                    outputQueue.add(operatorStack.pop());
                }
                operatorStack.push(upperToken);
            } else if (token.equals("(")) {
                operatorStack.push(token);
            } else if (token.equals(")")) {
                while (!operatorStack.isEmpty() && !operatorStack.peek().equals("(")) {
                    outputQueue.add(operatorStack.pop());
                }
                if (!operatorStack.isEmpty()) {
                    operatorStack.pop(); // Pop the left bracket
                }
            } else {
                // It is an operand (query1, query2, or filter expression like ct=missense;popFreq<0.1)
                outputQueue.add(token);
            }
        }

        while (!operatorStack.isEmpty()) {
            outputQueue.add(operatorStack.pop());
        }

        return outputQueue;
    }

    /**
     * Step 3: Build AST
     * Reads the RPN queue and constructs the tree objects.
     */
    private TreeNode buildTree(Queue<String> rpn) {
        Stack<TreeNode> stack = new Stack<>();

        for (String token : rpn) {
            if (token.equals("AND") || token.equals("OR") || token.equals("NOT_IN")) {
                // Operator: Pop two nodes, combine them, push back
                String displayOp = token.equals("NOT_IN") ? "NOT IN" : token;

                TreeNode right = stack.pop();
                TreeNode left = stack.pop();
                stack.push(new OperatorTreeNode(displayOp, left, right));
            } else {
                // Operand: Create leaf node, push to stack
                stack.push(new QueryTreeNode(token));
            }
        }

        return stack.isEmpty() ? null : stack.pop();
    }

    /**
     * Evaluates the tree starting from the root using the configured TreeEvaluator.
     *
     * @return The resulting Set of IDs.
     */
    public Set<String> evaluate() {
        return evaluator.evaluate(this.root);
    }

    /**
     * Evaluates the tree starting from the root using the configured TreeEvaluator.
     *
     * @param queriesSet The "Database" map connecting query names to actual ID Sets.
     * @return The resulting Set of IDs.
     */
    public Set<String> evaluate(Map<String, Set<String>> queriesSet) {
        return evaluator.evaluate(this.root, queriesSet);
    }

    /**
     * Evaluates the tree using the configured TreeEvaluator.
     *
     * @param node The current node in the AST.
     * @param queriesSet The "Database" map connecting query names to actual ID Sets.
     * @return The resulting Set of IDs.
     */
    public Set<String> evaluate(TreeNode node, Map<String, Set<String>> queriesSet) {
        return evaluator.evaluate(node, queriesSet);
    }

    public void printTree(String indent) {
        String safeIndent = indent == null ? "" : indent;
        printTree(this.root, safeIndent);
    }

    // Helper to print tree prettily
    public void printTree(TreeNode node, String indent) {
        if (node instanceof OperatorTreeNode) {
            OperatorTreeNode op = (OperatorTreeNode) node;
            System.out.println(indent + op.getOperator());
            printTree(op.getLeft(), indent + "  ├─ ");
            printTree(op.getRight(), indent + "  └─ ");
        } else {
            System.out.println(indent + node.toString());
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TreeParser{");
        sb.append("root=").append(root);
        sb.append(", evaluator=").append(evaluator);
        sb.append('}');
        return sb.toString();
    }

    public TreeNode getRoot() {
        return root;
    }

    public TreeParser setRoot(TreeNode root) {
        this.root = root;
        return this;
    }

    public TreeEvaluator getEvaluator() {
        return evaluator;
    }
}
