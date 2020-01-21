package org.opencb.opencga.analysis.variant.samples;

import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class VariantQueryOptimizer {

    public static TreeQuery.Node optimize(TreeQuery.Node node) {
        List<TreeQuery.Node> nodes = node.getNodes();
        if (nodes != null) {
            nodes.replaceAll(VariantQueryOptimizer::optimize);

            // Merge intersections
            if (node.getType().equals(TreeQuery.Node.Type.INTERSECTION)) {
                if (nodes.stream().map(TreeQuery.Node::getType).allMatch(type -> type.equals(TreeQuery.Node.Type.QUERY))) {
                    for (int j = 0; j < nodes.size(); j++) {
                        TreeQuery.Node thisNode = nodes.get(j);
                        Set<VariantQueryParam> thisNodeParams = VariantQueryUtils.validParams(thisNode.getQuery());

                        boolean collide = false;
                        int noCollides = 0;
                        // Check if "thisNode" does not collide with any other query
                        for (int i = 0; i < nodes.size(); i++) {
                            TreeQuery.Node subNode = nodes.get(i);
                            if (subNode == null || subNode == thisNode) {
                                continue;
                            }
                            if (checkQueryCollide(thisNode, thisNodeParams, subNode)) {
                                // Non disjoint params. Query nodes overlap
                                collide = true;
                                break;
                            } else {
                                noCollides++;
                            }
                        }
                        if (!collide && noCollides > 0) {
                            for (TreeQuery.Node subNode : nodes) {
                                if (subNode == null || subNode == thisNode) {
                                    continue;
                                }
                                subNode.getQuery().putAll(thisNode.getQuery());
                            }
                            nodes.set(j, null);
                        }
                    }
                    nodes.removeIf(Objects::isNull);
                }
            }

            if (nodes.size() == 1) {
                // Unwind single element nodes
                return nodes.get(0);
            }

        }
        return node;
    }

    public static boolean checkQueryCollide(TreeQuery.Node thisNode, Set<VariantQueryParam> thisNodeParams, TreeQuery.Node subNode) {
        if (Collections.disjoint(thisNodeParams, VariantQueryUtils.validParams(subNode.getQuery()))) {
            // Disjoint params. Does not collide
            return false;
        }
        for (VariantQueryParam thisNodeParam : thisNodeParams) {
            if (VariantQueryUtils.isValidParam(subNode.getQuery(), thisNodeParam)) {
                if (!thisNode.getQuery().getString(thisNodeParam.key()).equals(subNode.getQuery().getString(thisNodeParam.key()))) {
                    return true;
                }
            }
        }
        return false;
    }
}
