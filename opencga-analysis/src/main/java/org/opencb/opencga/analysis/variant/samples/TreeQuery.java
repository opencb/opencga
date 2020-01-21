package org.opencb.opencga.analysis.variant.samples;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class TreeQuery {
    private final Node root;

    public TreeQuery(String q) {
        root = parse(q);
    }

    public Node getRoot() {
        return root;
    }

    public static Node parse(String q) {
        q = StringUtils.trim(q);
        int open = StringUtils.countMatches(q, "(");
        int close = StringUtils.countMatches(q, ")");
        if (open != close) {
            throw new IllegalArgumentException("Malformed query '" + q + "'");
        }
        if (open == 0) {
            Query query = new Query();
            for (String token : q.split(" AND ")) {
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
                query.put(key, value);
            }
            return new QueryNode(query);
        }

//        if (q.charAt(0) != '(') {
//            throw new IllegalArgumentException("Malformed query '" + q + "'");
//        }
        List<Node> nodes = new LinkedList<>();
        int level = 0;
        int beginIndex = 0;
        int endIndex = 0;
        Node.Type type = null;
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
                endIndex = -1;
                nodes.add(parse(subQuery));

                int nextOpen = q.indexOf("(", i);
                if (nextOpen < 0) {
                    break;
                }
                String operator = q.substring(i, nextOpen).toUpperCase();
                if (operator.contains("OR")) {
                    if (type == null) {
                        type = Node.Type.UNION;
                    } else if (type != Node.Type.UNION) {
                        throw new IllegalArgumentException();
                    }
                }
                if (operator.contains("AND")) {
                    if (type == null) {
                        type = Node.Type.INTERSECTION;
                    } else if (type != Node.Type.INTERSECTION) {
                        throw new IllegalArgumentException();
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
            QUERY,INTERSECTION,UNION
        }

        Node.Type getType();

        default Query getQuery() {
            return null;
        }

        default List<Node> getNodes() {
            return null;
        }
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
//                final StringBuilder sb = new StringBuilder("QueryNode{");
//                sb.append("query=").append(query.toJson());
//                sb.append('}');
            return query.entrySet().stream().map(e -> e.getKey() + (StringUtils.startsWithAny(e.getValue().toString(), "=", ">", "<", "!", "~") ? "" : "=") + e.getValue()).collect(Collectors.joining(" AND "));
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
//                final StringBuilder sb = new StringBuilder("IntersectionNode{");
//                sb.append("nodes=").append(nodes);
//                sb.append('}');
            return nodes.stream().map(Object::toString).collect(Collectors.joining(") AND (", "(", ")"));
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
//                final StringBuilder sb = new StringBuilder("UnionNode{");
//                sb.append("nodes=").append(nodes);
//                sb.append('}');
//                return sb.toString();
            return nodes.stream().map(Object::toString).collect(Collectors.joining(") OR (", "(", ")"));
        }
    }

}
