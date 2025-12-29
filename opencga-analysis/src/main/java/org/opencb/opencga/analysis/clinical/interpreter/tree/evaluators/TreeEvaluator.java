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

package org.opencb.opencga.analysis.clinical.interpreter.tree.evaluators;

import org.opencb.opencga.analysis.clinical.interpreter.tree.node.OperatorTreeNode;
import org.opencb.opencga.analysis.clinical.interpreter.tree.node.QueryTreeNode;
import org.opencb.opencga.analysis.clinical.interpreter.tree.node.TreeNode;

import java.util.Map;
import java.util.Set;

/**
 * Interface for implementing different evaluation strategies for tree nodes.
 * Implementations of this interface can provide different logic for evaluating
 * queries and combining results using operators.
 */
public interface TreeEvaluator {

    /**
     * Evaluates a tree node and returns the result.
     *
     * @param node The tree node to evaluate (can be QueryTreeNode or OperatorTreeNode)
     * @return The resulting set of IDs after evaluation
     */
    Set<String> evaluate(TreeNode node);

    /**
     * Evaluates a tree node and returns the result.
     *
     * @param node The tree node to evaluate (can be QueryTreeNode or OperatorTreeNode)
     * @param queriesSet The context map containing query names and their corresponding result sets
     * @return The resulting set of IDs after evaluation
     */
    Set<String> evaluate(TreeNode node, Map<String, Set<String>> queriesSet);

    /**
     * Evaluates a query node (leaf node).
     *
     * @param queryNode The query node containing the query name
     * @param queriesSet The context map containing query names and their corresponding result sets
     * @return The set of IDs for this query
     */
    Set<String> evaluateQuery(QueryTreeNode queryNode, Map<String, Set<String>> queriesSet);

    /**
     * Evaluates an operator node by combining the results of its left and right children.
     *
     * @param operatorNode The operator node containing the operator and children
     * @param leftResult The evaluated result from the left child
     * @param rightResult The evaluated result from the right child
     * @return The combined result based on the operator
     */
    Set<String> evaluateOperator(OperatorTreeNode operatorNode, Set<String> leftResult, Set<String> rightResult);
}

