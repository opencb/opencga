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

package org.opencb.opencga.analysis.clinical.meta.tree.evaluators;

import org.opencb.opencga.analysis.clinical.meta.tree.node.OperatorTreeNode;
import org.opencb.opencga.analysis.clinical.meta.tree.node.QueryTreeNode;
import org.opencb.opencga.analysis.clinical.meta.tree.node.TreeNode;

import java.util.Map;
import java.util.Set;

/**
 * Example implementation of TreeEvaluator that logs evaluation steps.
 * This evaluator wraps another evaluator and adds logging functionality
 * to help debug query evaluation.
 */
public class LoggingTreeEvaluator implements TreeEvaluator {

    private final TreeEvaluator delegate;
    private int depth = 0;

    /**
     * Constructor with optional delegate. If no delegate is provided,
     * uses SetTreeEvaluator as default.
     */
    public LoggingTreeEvaluator(TreeEvaluator delegate) {
        this.delegate = delegate != null ? delegate : new SetTreeEvaluator();
    }

    /**
     * Constructor without delegate. Uses SetTreeEvaluator as default.
     */
    public LoggingTreeEvaluator() {
        this(new SetTreeEvaluator());
    }

    @Override
    public Set<String> evaluate(TreeNode node) {
        String indent = getIndent();

        if (node instanceof QueryTreeNode) {
            QueryTreeNode queryNode = (QueryTreeNode) node;
            System.out.println(indent + "→ Evaluating query: " + queryNode.getValue());
            Set<String> result = delegate.evaluate(node);
            System.out.println(indent + "← Query result size: " + result.size());
            return result;
        } else if (node instanceof OperatorTreeNode) {
            OperatorTreeNode opNode = (OperatorTreeNode) node;
            System.out.println(indent + "→ Evaluating operator: " + opNode.getOperator());
            depth++;
            Set<String> result = delegate.evaluate(node);
            depth--;
            System.out.println(indent + "← Operator result size: " + result.size());
            return result;
        }

        return delegate.evaluate(node);
    }

    @Override
    public Set<String> evaluate(TreeNode node, Map<String, Set<String>> queriesSet) {
        String indent = getIndent();

        if (node instanceof QueryTreeNode) {
            QueryTreeNode queryNode = (QueryTreeNode) node;
            System.out.println(indent + "→ Evaluating query: " + queryNode.getValue());
            Set<String> result = delegate.evaluate(node, queriesSet);
            System.out.println(indent + "← Query result size: " + result.size());
            return result;
        } else if (node instanceof OperatorTreeNode) {
            OperatorTreeNode opNode = (OperatorTreeNode) node;
            System.out.println(indent + "→ Evaluating operator: " + opNode.getOperator());
            depth++;
            Set<String> result = delegate.evaluate(node, queriesSet);
            depth--;
            System.out.println(indent + "← Operator result size: " + result.size());
            return result;
        }

        return delegate.evaluate(node, queriesSet);
    }

    @Override
    public Set<String> evaluateQuery(QueryTreeNode queryNode, Map<String, Set<String>> queriesSet) {
        return delegate.evaluateQuery(queryNode, queriesSet);
    }

    @Override
    public Set<String> evaluateOperator(OperatorTreeNode operatorNode, Set<String> leftResult, Set<String> rightResult) {
        return delegate.evaluateOperator(operatorNode, leftResult, rightResult);
    }

    private String getIndent() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }
}

