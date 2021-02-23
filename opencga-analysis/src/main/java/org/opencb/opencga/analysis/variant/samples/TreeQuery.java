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

package org.opencb.opencga.analysis.variant.samples;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TreeQuery {
    public static final String AND = "AND";
    public static final String OR = "OR";
    public static final String NOT = "NOT";

    private Node root;

    public TreeQuery(String q) {
        root = parse(q);
    }

    public Node getRoot() {
        return root;
    }

    public TreeQuery setRoot(Node root) {
        this.root = root;
        return this;
    }

    public void forEachQuery(Consumer<Query> queryConsumer) {
        forEachNode(node -> {
            if (node.getQuery() != null) {
                queryConsumer.accept(node.getQuery());
            }
        });
    }

    public void forEachNode(Consumer<Node> nodeConsumer) {
        forEachNode(root, nodeConsumer);
    }

    private void forEachNode(Node node, Consumer<Node> nodeConsumer) {
        nodeConsumer.accept(node);
        if (node.getNodes() != null) {
            for (Node subNode : node.getNodes()) {
                forEachNode(subNode, nodeConsumer);
            }
        }
    }
    @Override
    public String toString() {
        String s = root.toString();
        if (s.startsWith("(") && s.endsWith(")")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    public String toTreeString() {
        StringBuilder sb = new StringBuilder();
        root.toTreeString(sb, " ");
        return sb.toString();
    }

    public void log() {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        logger.info(toString());
        for (String s : toTreeString().split("\n")) {
            logger.info(s);
        }
    }

    private static Node parse(String q) {
        q = StringUtils.trim(q);
        int open = StringUtils.countMatches(q, "(");
        int close = StringUtils.countMatches(q, ")");
        if (open != close) {
            throw new IllegalArgumentException("Malformed query '" + q + "'");
        }
        if (open > 0) {
            return parseNodes(q);
        } else {
            if (q.startsWith(NOT)) {
                return new ComplementNode(parseQueryNode(q.replaceFirst(NOT, "")));
            } else {
                return parseQueryNode(q);
            }
        }

    }

    private static Node parseQueryNode(String q) {
        if (q.contains(OR)) {
            List<Node> unionNodes = new ArrayList<>();
            for (String token : q.split(" " + OR + " ")) {
                unionNodes.add(parseQueryNode(token));
            }
            return new UnionNode(unionNodes);
        }
        Query query = new Query();
        for (String token : q.split(" " + AND + " ")) {
            token = token.replace(" ", "");

//                String[] split = token.split(":", 2);
//                query.put(split[0], split[1]);

            String[] split = VariantQueryUtils.splitOperator(token);
            String key = split[0];
            String op = split[1];
            String value = split[2];
            if (key == null) {
                throw new IllegalArgumentException("Invalid filter '" + token + "'");
            }
            if (!op.equals("=") && !op.equals("==")) {
                value = op + value;
            }
            if (query.put(key, value) != null) {
                throw new IllegalArgumentException("Query param '" + key + "' found multiple times in statement '" + q + "'.");
//                        + "Please, rebuild the query adding brackets. e.g. 'key=A AND key=B' -> '(key=A) AND (key=B)'");
            }
        }
        return new QueryNode(query);
    }

    private static Node parseNodes(String q) {
//        if (q.charAt(0) != '(') {
//            throw new IllegalArgumentException("Malformed query '" + q + "'");
//        }
        List<Node> nodes = new LinkedList<>();
        int level = 0;
        int beginIndex = 0;
        int endIndex = 0;
        Node.Type type = null;
        boolean complement = false;
        q += " ";
        for (int i = 0; i < q.length(); i++) {
            char c = q.charAt(i);
            if (c == '(') {
                if (level == 0) {
                    beginIndex = i + 1;
                }
                level++;
            }
            if (c == ')') {
                level--;
                if (level == 0) {
                    endIndex = i;
                }
            }
            if (level == 0 && c == ' ') {
                String subQuery = q.substring(beginIndex, endIndex > 0 ? endIndex : i);
                if (subQuery.equals(NOT)) {
                    complement = true;
                    continue;
                }
                endIndex = -1;
                Node subNode = parse(subQuery);
                if (complement) {
                    subNode = new ComplementNode(subNode);
                    complement = false;
                }
                nodes.add(subNode);

                int nextOpen = q.indexOf("(", i);
                if (nextOpen < 0) {
                    break;
                }
                String operator = q.substring(i, nextOpen).toUpperCase();
                if (operator.contains(OR)) {
                    if (type == null) {
                        type = Node.Type.UNION;
                    } else if (type != Node.Type.UNION) {
                        throw new IllegalArgumentException("Unable to mix OR and AND in the same statement: " + q);
                    }
                    if (operator.contains(NOT)) {
                        complement = true;
                    }
                }
                if (operator.contains(AND)) {
                    if (type == null) {
                        type = Node.Type.INTERSECTION;
                    } else if (type != Node.Type.INTERSECTION) {
                        throw new IllegalArgumentException("Unable to mix OR and AND in the same statement: " + q);
                    }
                    if (operator.contains(NOT)) {
                        complement = true;
                    }
                }
                if (type == null) {
                    throw new IllegalArgumentException("Operator not found at '" + operator + "'");
                }
//                beginIndex = nextOpen + 1;
                i = nextOpen - 1;
            }
        }

        if (nodes.size() == 1) {
            return nodes.get(0);
        }

        if (type == Node.Type.UNION) {
            return new UnionNode(nodes);
        } else {
            return new IntersectionNode(nodes);
        }
    }

    public interface Node {
        enum Type {
            QUERY,
            COMPLEMENT,
            INTERSECTION,
            UNION;
        }

        Node.Type getType();

        default Query getQuery() {
            return null;
        }

        default List<Node> getNodes() {
            return null;
        }

        void toTreeString(StringBuilder sb, String indent);
    }

    public static class QueryNode implements Node {

        public QueryNode(Query query) {
            this.query = query;
        }

        private Query query;

        @Override
        public Type getType() {
            return Type.QUERY;
        }

        public Query getQuery() {
            return query;
        }

        public QueryNode setQuery(Query query) {
            this.query = query;
            return this;
        }

        @Override
        public String toString() {
            return query.entrySet().stream()
                    .map(e -> {
                        boolean withOperator = StringUtils.startsWithAny(e.getValue().toString(), "=", ">", "<", "!", "~");
                        return e.getKey() + (withOperator ? "" : "=") + e.getValue();
                    })
                    .collect(Collectors.joining(" " + AND + " ", "(", ")"));
        }

        @Override
        public void toTreeString(StringBuilder sb, String indent) {
            sb.append("QUERY").append("\n");
            for (Iterator<String> iterator = getQuery().keySet().iterator(); iterator.hasNext();) {
                String key = iterator.next();
                String value = query.getString(key);
                boolean withOperator = StringUtils.startsWithAny(value.toString(), "=", ">", "<", "!", "~");
                sb.append(indent).append(iterator.hasNext() ? "├──" : "└──").append(key);
                if (!withOperator) {
                    sb.append("=");
                }
                if (value.length() > 40) {
                    sb.append(value, 0, 37).append("...");
                } else {
                    sb.append(value);
                }
                sb.append("\n");
            }
        }
    }

    public static class ComplementNode implements Node {

        public ComplementNode(Node node) {
            this.node = node;
        }

        private Node node;

        @Override
        public Type getType() {
            return Type.COMPLEMENT;
        }

        @Override
        public List<Node> getNodes() {
            return Arrays.asList(node);
        }

        public ComplementNode setNode(Node node) {
            this.node = node;
            return this;
        }
        @Override
        public String toString() {
            return "(" + NOT + " " + node.toString() + ")";
        }

        @Override
        public void toTreeString(StringBuilder sb, String indent) {
            sb.append(NOT).append("\n");
            for (Iterator<Node> iterator = getNodes().iterator(); iterator.hasNext();) {
                Node node = iterator.next();
                sb.append(indent).append(iterator.hasNext() ? "├──" : "└──");
                node.toTreeString(sb, indent + (iterator.hasNext() ? "│   " : "    "));
            }
        }
    }

    public static class IntersectionNode implements Node {
        private final List<Node> nodes;

        public IntersectionNode() {
            nodes = new LinkedList<>();
        }

        public IntersectionNode(List<Node> nodes) {
            this.nodes = nodes;
        }

        @Override
        public Type getType() {
            return Type.INTERSECTION;
        }

        public List<Node> getNodes() {
            return nodes;
        }

        @Override
        public String toString() {
            return nodes.stream().map(Object::toString).collect(Collectors.joining(" " + AND + " ", "(", ")"));
        }

        @Override
        public void toTreeString(StringBuilder sb, String indent) {
            sb.append(AND).append("\n");
            for (Iterator<Node> iterator = getNodes().iterator(); iterator.hasNext();) {
                Node node = iterator.next();
                sb.append(indent).append(iterator.hasNext() ? "├──" : "└──");
                node.toTreeString(sb, indent + (iterator.hasNext() ? "│   " : "    "));
            }
        }
    }

    public static class UnionNode implements Node {
        private final List<Node> nodes;

        public UnionNode() {
            nodes = new LinkedList<>();
        }

        public UnionNode(List<Node> nodes) {
            this.nodes = nodes;
        }

        @Override
        public Type getType() {
            return Type.UNION;
        }

        public List<Node> getNodes() {
            return nodes;
        }

        @Override
        public String toString() {
            return nodes.stream().map(Object::toString).collect(Collectors.joining(" " + OR + " ", "(", ")"));
        }

        @Override
        public void toTreeString(StringBuilder sb, String indent) {
            sb.append(OR).append("\n");
            for (Iterator<Node> iterator = getNodes().iterator(); iterator.hasNext();) {
                Node node = iterator.next();
                sb.append(indent).append(iterator.hasNext() ? "├──" : "└──");
                node.toTreeString(sb, indent + (iterator.hasNext() ? "│   " : "    "));
            }
        }
    }

}
