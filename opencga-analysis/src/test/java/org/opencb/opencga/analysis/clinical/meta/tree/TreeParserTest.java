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

package org.opencb.opencga.analysis.clinical.meta.tree;

import org.junit.Before;
import org.junit.Test;
import org.opencb.opencga.analysis.clinical.interpreter.tree.TreeParser;
import org.opencb.opencga.analysis.clinical.interpreter.tree.evaluators.LoggingTreeEvaluator;
import org.opencb.opencga.analysis.clinical.interpreter.tree.evaluators.SetTreeEvaluator;
import org.opencb.opencga.analysis.clinical.interpreter.tree.node.OperatorTreeNode;
import org.opencb.opencga.analysis.clinical.interpreter.tree.node.QueryTreeNode;
import org.opencb.opencga.analysis.clinical.interpreter.tree.node.TreeNode;

import java.util.*;

import static org.junit.Assert.*;

public class TreeParserTest {

    private TreeParser parser;
    private Map<String, Set<String>> queriesSet;

    @Before
    public void setUp() {
        parser = new TreeParser();
        queriesSet = new HashMap<>();
        queriesSet.put("query1", new HashSet<>(Arrays.asList("A", "B", "C")));
        queriesSet.put("query2", new HashSet<>(Arrays.asList("B", "C", "D")));
        queriesSet.put("query3", new HashSet<>(Arrays.asList("E", "F")));
        queriesSet.put("query4", new HashSet<>(Arrays.asList("A", "E")));
    }

    @Test
    public void testParseSimpleQuery() {
        TreeNode node = parser.parse("query1");

        assertNotNull(node);
        assertTrue(node instanceof QueryTreeNode);
        assertEquals("query1", ((QueryTreeNode) node).getValue());
    }

    @Test
    public void testParseAndOperator() {
        TreeNode node = parser.parse("query1 AND query2");

        assertNotNull(node);
        assertTrue(node instanceof OperatorTreeNode);
        assertEquals("AND", ((OperatorTreeNode) node).getOperator().toString());
    }

    @Test
    public void testParseOrOperator() {
        TreeNode node = parser.parse("query1 OR query2");

        assertNotNull(node);
        assertTrue(node instanceof OperatorTreeNode);
        assertEquals("OR", ((OperatorTreeNode) node).getOperator().toString());
    }

    @Test
    public void testParseNotInOperator() {
        TreeNode node = parser.parse("query1 NOT IN query2");

        assertNotNull(node);
        assertTrue(node instanceof OperatorTreeNode);
        assertEquals("NOT IN", ((OperatorTreeNode) node).getOperator().toString());
    }

    @Test
    public void testParseWithParentheses() {
        TreeNode node = parser.parse("(query1 AND query2) OR query3");

        assertNotNull(node);
        assertTrue(node instanceof OperatorTreeNode);
        OperatorTreeNode root = (OperatorTreeNode) node;
        assertEquals("OR", root.getOperator().toString());
        assertTrue(root.getLeft() instanceof OperatorTreeNode);
        assertTrue(root.getRight() instanceof QueryTreeNode);
    }

    @Test
    public void testParseComplexExpression() {
        String query = "(query1 OR query2) AND (query3 OR query4)";
        TreeNode node = parser.parse(query);

        assertNotNull(node);
        assertTrue(node instanceof OperatorTreeNode);
        OperatorTreeNode root = (OperatorTreeNode) node;
        assertEquals("AND", root.getOperator().toString());
        assertTrue(root.getLeft() instanceof OperatorTreeNode);
        assertTrue(root.getRight() instanceof OperatorTreeNode);
    }

    @Test
    public void testParseComplexExpressionWithNotIn() {
        String query = "((query1 OR query2) AND query3) NOT IN query4";
        TreeNode node = parser.parse(query);
        parser.printTree(node, "");

        assertNotNull(node);
        assertTrue(node instanceof OperatorTreeNode);
        OperatorTreeNode root = (OperatorTreeNode) node;
        assertEquals("NOT IN", root.getOperator().toString());
        assertTrue(root.getLeft() instanceof OperatorTreeNode);
        assertTrue(root.getRight() instanceof QueryTreeNode);
    }

    @Test
    public void testParseFilterExpressionSimple() {
        String query = "ct=missense";
        queriesSet.put("ct=missense", new HashSet<>(Arrays.asList("V1", "V2")));

        TreeNode node = parser.parse(query);

        assertNotNull(node);
        assertTrue(node instanceof QueryTreeNode);
        assertEquals("ct=missense", ((QueryTreeNode) node).getValue());
    }

    @Test
    public void testParseFilterExpressionWithSemicolon() {
        String query = "ct=missense;popFreq<0.1";
        queriesSet.put("ct=missense;popFreq<0.1", new HashSet<>(Arrays.asList("V1", "V2")));

        TreeNode node = parser.parse(query);

        assertNotNull(node);
        assertTrue(node instanceof QueryTreeNode);
        assertEquals("ct=missense;popFreq<0.1", ((QueryTreeNode) node).getValue());
    }

    @Test
    public void testParseFilterExpressionWithOperator() {
        String query = "ct=missense;popFreq<0.1 OR sample=0/1,1/1;filter=PASS";
        queriesSet.put("ct=missense;popFreq<0.1", new HashSet<>(Arrays.asList("V1", "V2")));
        queriesSet.put("sample=0/1,1/1;filter=PASS", new HashSet<>(Arrays.asList("V2", "V3")));

        TreeNode node = parser.parse(query);

        assertNotNull(node);
        assertTrue(node instanceof OperatorTreeNode);
        OperatorTreeNode root = (OperatorTreeNode) node;
        assertEquals("OR", root.getOperator().toString());
        assertTrue(root.getLeft() instanceof QueryTreeNode);
        assertTrue(root.getRight() instanceof QueryTreeNode);
    }

    @Test
    public void testParseComplexFilterExpression() {
        String query = "(ct=missense;popFreq<0.1 OR sample=0/1,1/1;filter=PASS) AND gene=BRCA2";
        queriesSet.put("ct=missense;popFreq<0.1", new HashSet<>(Arrays.asList("V1", "V2")));
        queriesSet.put("sample=0/1,1/1;filter=PASS", new HashSet<>(Arrays.asList("V2", "V3")));
        queriesSet.put("gene=BRCA2", new HashSet<>(Arrays.asList("V1", "V3", "V4")));

        TreeNode node = parser.parse(query);
        parser.printTree(node, "");

        assertNotNull(node);
        assertTrue(node instanceof OperatorTreeNode);
        OperatorTreeNode root = (OperatorTreeNode) node;
        assertEquals("AND", root.getOperator().toString());
        assertTrue(root.getLeft() instanceof OperatorTreeNode);
        assertTrue(root.getRight() instanceof QueryTreeNode);
    }

    @Test
    public void testParseComplexFilterExpressionWithNotIn() {
        String query = "((ct=missense;popFreq<0.1 OR sample=0/1,1/1;filter=PASS) AND (gene=BRCA2,BRCA1 OR xref=A;B;C;qual=5)) NOT IN chromosome=1,2";
        queriesSet.put("ct=missense;popFreq<0.1", new HashSet<>(Arrays.asList("V1", "V2")));
        queriesSet.put("sample=0/1,1/1;filter=PASS", new HashSet<>(Arrays.asList("V2", "V3")));
        queriesSet.put("gene=BRCA2,BRCA1", new HashSet<>(Arrays.asList("V1", "V3", "V4")));
        queriesSet.put("xref=A;B;C;qual=5", new HashSet<>(Arrays.asList("V4", "V5")));
        queriesSet.put("chromosome=1,2", new HashSet<>(Arrays.asList("V1", "V2")));

        TreeNode node = parser.parse(query);
        parser.printTree(node, "");

        assertNotNull(node);
        assertTrue(node instanceof OperatorTreeNode);
        OperatorTreeNode root = (OperatorTreeNode) node;
        assertEquals("NOT IN", root.getOperator().toString());
    }

    @Test
    public void testEvaluateWithSetEvaluator() {
        TreeParser setParser = new TreeParser(new SetTreeEvaluator());
        TreeNode node = setParser.parse("query1 AND query2");

        Set<String> result = setParser.evaluate(queriesSet);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("B"));
        assertTrue(result.contains("C"));
    }

    @Test
    public void testEvaluateOrOperation() {
        TreeParser setParser = new TreeParser(new SetTreeEvaluator());
        TreeNode node = setParser.parse("query1 OR query3");

        Set<String> result = setParser.evaluate(queriesSet);

        assertNotNull(result);
        assertEquals(5, result.size());
        assertTrue(result.containsAll(Arrays.asList("A", "B", "C", "E", "F")));
    }

    @Test
    public void testEvaluateNotInOperation() {
        TreeParser setParser = new TreeParser(new SetTreeEvaluator());
        TreeNode node = setParser.parse("query1 NOT IN query2");

        Set<String> result = setParser.evaluate(queriesSet);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains("A"));
    }

    @Test
    public void testEvaluateComplexExpression() {
        TreeParser setParser = new TreeParser(new SetTreeEvaluator());
        TreeNode root = setParser.parse("(query1 OR query2) AND query4");

        Set<String> result = setParser.evaluate(root, queriesSet);

        assertNotNull(result);
        assertTrue(result.contains("A"));
    }

    @Test
    public void testEvaluateComplexExpressionWithNotIn() {
        TreeParser setParser = new TreeParser(new SetTreeEvaluator());
        TreeNode node = setParser.parse("((query1 OR query2) AND query3) NOT IN query4");

        Set<String> result = setParser.evaluate(queriesSet);

        // (query1 OR query2) = {A, B, C, D}
        // (query1 OR query2) AND query3 = {A, B, C, D} AND {E, F} = {}
        // {} NOT IN query4 = {} - {A, E} = {}
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testEvaluateWithLoggingEvaluator() {
        TreeParser loggingParser = new TreeParser(new LoggingTreeEvaluator());
        TreeNode node = loggingParser.parse("query1 AND query2");

        Set<String> result = loggingParser.evaluate(node, queriesSet);

        // LoggingTreeEvaluator should still return correct results
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("B"));
        assertTrue(result.contains("C"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNullEvaluator() {
        new TreeParser(null);
    }

    @Test
    public void testOperatorPrecedence() {
        TreeNode node = parser.parse("query1 OR query2 AND query3");

        // AND has higher precedence than OR
        assertTrue(node instanceof OperatorTreeNode);
        OperatorTreeNode root = (OperatorTreeNode) node;
        assertEquals("OR", root.getOperator().toString());
        assertTrue(root.getRight() instanceof OperatorTreeNode);
        assertEquals("AND", ((OperatorTreeNode) root.getRight()).getOperator().toString());
    }

    @Test
    public void testCaseInsensitiveOperators() {
        TreeNode node1 = parser.parse("query1 and query2");
        TreeNode node2 = parser.parse("query1 AND query2");
        TreeNode node3 = parser.parse("query1 AnD query2");

        assertTrue(node1 instanceof OperatorTreeNode);
        assertTrue(node2 instanceof OperatorTreeNode);
        assertTrue(node3 instanceof OperatorTreeNode);
        assertEquals("AND", ((OperatorTreeNode) node1).getOperator().toString());
        assertEquals("AND", ((OperatorTreeNode) node2).getOperator().toString());
        assertEquals("AND", ((OperatorTreeNode) node3).getOperator().toString());
    }

    @Test
    public void testCaseInsensitiveNotIn() {
        TreeNode node1 = parser.parse("query1 not in query2");
        TreeNode node2 = parser.parse("query1 NOT IN query2");
        TreeNode node3 = parser.parse("query1 NoT iN query2");

        assertTrue(node1 instanceof OperatorTreeNode);
        assertTrue(node2 instanceof OperatorTreeNode);
        assertTrue(node3 instanceof OperatorTreeNode);
        assertEquals("NOT IN", ((OperatorTreeNode) node1).getOperator().toString());
        assertEquals("NOT IN", ((OperatorTreeNode) node2).getOperator().toString());
        assertEquals("NOT IN", ((OperatorTreeNode) node3).getOperator().toString());
    }
}

