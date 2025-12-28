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

package org.opencb.opencga.analysis.clinical.meta.tree.node;

public class OperatorTreeNode implements TreeNode {

    private OperatorTreeType operator;
    private TreeNode left;
    private TreeNode right;

    /**
     * Constructor that accepts a String operator for backward compatibility.
     * Validates and converts the string to an Operator enum.
     *
     * @param operator the string representation of the operator (e.g., "AND", "OR", "NOT IN")
     * @param left the left child node
     * @param right the right child node
     * @throws IllegalArgumentException if the operator is invalid
     */
    public OperatorTreeNode(String operator, TreeNode left, TreeNode right) {
        this(OperatorTreeType.fromString(operator), left, right);
    }

    /**
     * Constructor that accepts an Operator enum.
     *
     * @param operator the operator enum
     * @param left the left child node
     * @param right the right child node
     * @throws IllegalArgumentException if the operator is null
     */
    public OperatorTreeNode(OperatorTreeType operator, TreeNode left, TreeNode right) {
        if (operator == null) {
            throw new IllegalArgumentException("Operator cannot be null");
        }
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OperatorTreeNode{");
        sb.append("operator='").append(operator).append('\'');
        sb.append(", left=").append(left);
        sb.append(", right=").append(right);
        sb.append('}');
        return sb.toString();
    }

    public OperatorTreeType getOperator() {
        return operator;
    }

    public OperatorTreeNode setOperator(OperatorTreeType operator) {
        if (operator == null) {
            throw new IllegalArgumentException("Operator cannot be null");
        }
        this.operator = operator;
        return this;
    }

    /**
     * Set the operator using a string value for backward compatibility.
     *
     * @param operator the string representation of the operator
     * @return this instance for method chaining
     * @throws IllegalArgumentException if the operator is invalid
     */
    public OperatorTreeNode setOperator(String operator) {
        return setOperator(OperatorTreeType.fromString(operator));
    }

    public TreeNode getLeft() {
        return left;
    }

    public OperatorTreeNode setLeft(TreeNode left) {
        this.left = left;
        return this;
    }

    public TreeNode getRight() {
        return right;
    }

    public OperatorTreeNode setRight(TreeNode right) {
        this.right = right;
        return this;
    }
}
