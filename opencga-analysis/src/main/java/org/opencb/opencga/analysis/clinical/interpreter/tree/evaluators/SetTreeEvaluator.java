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
import org.opencb.opencga.analysis.clinical.interpreter.tree.node.OperatorTreeType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of TreeEvaluator that performs set operations.
 * This evaluator treats queries as sets of IDs and applies standard set operations
 * (union, intersection, difference) based on the operators.
 */
public class SetTreeEvaluator implements TreeEvaluator {

    @Override
    public Set<String> evaluate(TreeNode node) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> evaluate(TreeNode node, Map<String, Set<String>> queriesSet) {
        // Base Case: If it's a leaf (Query Name), delegate to evaluateQuery
        if (node instanceof QueryTreeNode) {
            return evaluateQuery((QueryTreeNode) node, queriesSet);
        }

        // Recursive Step: It's an operator
        OperatorTreeNode operatorNode = (OperatorTreeNode) node;

        // Recursively evaluate left and right children
        Set<String> leftResult = evaluate(operatorNode.getLeft(), queriesSet);
        Set<String> rightResult = evaluate(operatorNode.getRight(), queriesSet);

        // Combine results based on the operator
        return evaluateOperator(operatorNode, leftResult, rightResult);
    }

    @Override
    public Set<String> evaluateQuery(QueryTreeNode queryNode, Map<String, Set<String>> queriesSet) {
        String queryName = queryNode.getValue();

        if (!queriesSet.containsKey(queryName)) {
            return new HashSet<>(); // Return empty set if not found
        }

        // CRITICAL: Return a COPY of the set, because set operations
        // like retainAll() mutate the collection in-place.
        return new HashSet<>(queriesSet.get(queryName));
    }

    @Override
    public Set<String> evaluateOperator(OperatorTreeNode operatorNode, Set<String> leftResult, Set<String> rightResult) {
        OperatorTreeType operator = operatorNode.getOperator();

        switch (operator) {
            case OR:
                // Union: Add everything from Right into Left
                leftResult.addAll(rightResult);
                return leftResult;

            case AND:
                // Intersection: Keep only items present in both
                leftResult.retainAll(rightResult);
                return leftResult;

            case NOT_IN:
                // Difference: Remove items in Right from Left
                leftResult.removeAll(rightResult);
                return leftResult;

            default:
                throw new IllegalArgumentException("Unknown operator: " + operator);
        }
    }
}

