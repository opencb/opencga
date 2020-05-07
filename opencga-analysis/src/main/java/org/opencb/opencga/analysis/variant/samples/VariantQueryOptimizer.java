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

import org.opencb.opencga.analysis.variant.samples.TreeQuery.QueryNode;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class VariantQueryOptimizer {

    public static TreeQuery optimize(TreeQuery query) {
        return query.setRoot(optimize(query.getRoot()));
    }

    public static TreeQuery.Node optimize(TreeQuery.Node node) {
        if (node.getType().equals(TreeQuery.Node.Type.COMPLEMENT)) {
            TreeQuery.ComplementNode compNode = (TreeQuery.ComplementNode) node;
            if (compNode.getNodes().get(0).getType().equals(TreeQuery.Node.Type.COMPLEMENT)) {
                // Remove double negations
                return optimize(compNode.getNodes().get(0).getNodes().get(0));
            } else {
                TreeQuery.Node subNode = optimize(compNode.getNodes().get(0));
                compNode.setNode(subNode);
            }
        } else if (node.getType().equals(TreeQuery.Node.Type.INTERSECTION) || node.getType().equals(TreeQuery.Node.Type.UNION)) {
            List<TreeQuery.Node> nodes = node.getNodes();
            nodes.replaceAll(VariantQueryOptimizer::optimize);

            // Merge intersections
            if (node.getType().equals(TreeQuery.Node.Type.INTERSECTION)) {
                if (nodes.stream().map(TreeQuery.Node::getType).allMatch(type -> type.equals(TreeQuery.Node.Type.QUERY))) {
                    for (int j = nodes.size() - 1; j >= 0; j--) {
                        QueryNode thisNode = ((QueryNode) nodes.get(j));
                        Set<VariantQueryParam> thisNodeParams = VariantQueryUtils.validParams(thisNode.getQuery());

                        boolean collide = false;
                        int noCollides = 0;
                        // Check if "thisNode" does not collide with any other query
                        for (int i = 0; i < nodes.size(); i++) {
                            TreeQuery.Node subNode = nodes.get(i);
                            if (subNode == null || subNode == thisNode) {
                                continue;
                            }
                            if (checkQueryCollide(thisNode, thisNodeParams, ((QueryNode) subNode))) {
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

    public static boolean checkQueryCollide(QueryNode thisNode, Set<VariantQueryParam> thisNodeParams, QueryNode subNode) {
        for (VariantQueryParam thisNodeParam : thisNodeParams) {
            if (VariantQueryUtils.isValidParam(subNode.getQuery(), thisNodeParam)) {
                if (!thisNode.getQuery().getString(thisNodeParam.key()).equals(subNode.getQuery().getString(thisNodeParam.key()))) {
                    // Two elements with different content
                    return true;
                }
            }
        }
        if (withPositionalFilter(thisNode) && withPositionalFilter(subNode)) {
            // Different positional filters that are implemented with an OR underneath
            return true;
        }
//        if (Collections.disjoint(thisNodeParams, VariantQueryUtils.validParams(subNode.getQuery()))) {
//            // Disjoint params. Does not collide
//            return false;
//        }
        return false;
    }

    public static boolean withPositionalFilter(QueryNode queryNode) {
        return VariantQueryUtils.isValidParam(queryNode.getQuery(), VariantQueryParam.REGION)
                || VariantQueryUtils.isValidParam(queryNode.getQuery(), VariantQueryParam.GENE)
                || VariantQueryUtils.isValidParam(queryNode.getQuery(), VariantQueryParam.ID)
                || VariantQueryUtils.isValidParam(queryNode.getQuery(), VariantQueryParam.ANNOT_XREF);
    }
}
