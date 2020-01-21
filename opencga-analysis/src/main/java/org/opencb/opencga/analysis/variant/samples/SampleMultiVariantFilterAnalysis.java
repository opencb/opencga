package org.opencb.opencga.analysis.variant.samples;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;

import java.util.*;

@Tool(id="sample-multi-query", resource = Enums.Resource.VARIANT)
public class SampleMultiVariantFilterAnalysis extends OpenCgaToolScopeStudy {

    private SampleMultiVariantFilterAnalysisParams analysisParams = new SampleMultiVariantFilterAnalysisParams();
    private TreeQuery treeQuery;
//    private LinkedList<String> steps;

    private final static Comparator<TreeQuery.Node> COMPARATOR = Comparator.comparing(SampleMultiVariantFilterAnalysis::toQueryValue);

    @Override
    protected void check() throws Exception {
        super.check();
        analysisParams.updateParams(params);

        treeQuery = new TreeQuery(analysisParams.getQuery());

        VariantQueryOptimizer.optimize(treeQuery.getRoot());

//        steps = new LinkedList<>();
//        if (multiQuery.getTree().getType().equals(MultiQuery.Node.Type.QUERY)) {
//            int i = 0;
//            for (MultiQuery.Node node : multiQuery.getTree().getNodes()) {
//                steps.add("node-" + node.getType() + "-" + i);
//                i++;
//            }
//        }
//        steps.add("join-results");
    }

//    @Override
//    protected List<String> getSteps() {
//        return steps;
//    }

    @Override
    protected void run() throws Exception {
        List<String> inputSamples = new ArrayList<>(getVariantStorageManager().getIndexedSamples(getStudyFqn(), getToken()));
        Query baseQuery = new Query();
        baseQuery.put(VariantQueryParam.STUDY.key(), getStudyFqn());

        List<String> samplesResult = resolveNode(treeQuery.getRoot(), baseQuery, inputSamples);

        System.out.println("##num_samples="+samplesResult.size());
        System.out.println("#SAMPLE");
        for (String s : samplesResult) {
            System.out.println(s);
        }
    }

    // Return a value that will depend on the likely of the node to return a large or small number of samples
    private static Integer toQueryValue(TreeQuery.Node node) {
        switch (node.getType()) {
            case QUERY:
                int v = 1000;
                Query query = node.getQuery();
                if (VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_CONSEQUENCE_TYPE)) {
                    List<String> cts = VariantQueryUtils
                            .parseConsequenceTypes(query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key()));
                    if (VariantQueryUtils.LOF_SET.containsAll(cts)) {
                        v -= 500;
                    } else if (VariantQueryUtils.LOF_EXTENDED_SET.containsAll(cts)) {
                        v -= 250;
                    }
                }
                VariantQueryParser.VariantQueryXref xrefs = VariantQueryParser.parseXrefs(query);
                int fromXref = 0;
//                if (!xrefs.getGenes().isEmpty()) {
//
//                }
//                if (!xrefs.getOtherXrefs().isEmpty()) {
//
//                }
//
//                if (VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_CONSEQUENCE_TYPE)) {
//
//                }
//                if (VariantQueryUtils.isValidParam(query, VariantQueryParam.ANNOT_CONSEQUENCE_TYPE)) {
//
//                }
                v -= fromXref;
                return v;
            case UNION:
                return node.getNodes().stream().mapToInt(SampleMultiVariantFilterAnalysis::toQueryValue).max().orElse(0);
            case INTERSECTION:
                return node.getNodes().stream().mapToInt(SampleMultiVariantFilterAnalysis::toQueryValue).min().orElse(0);
            default:
                throw new IllegalArgumentException("Unknown node type " + node.getType());
        }

    }

    private List<String> resolveNode(TreeQuery.Node node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException {
        switch (node.getType()) {
            case QUERY:
                return resolveQuery(((TreeQuery.QueryNode) node), baseQuery, includeSamples);
            case INTERSECTION:
                return resolveIntersectNode(((TreeQuery.IntersectionNode) node), baseQuery, includeSamples);
            case UNION:
                return resolveUnionNode(((TreeQuery.UnionNode) node), baseQuery, includeSamples);
            default:
                throw new IllegalArgumentException("Unknown node type " + node.getType());
        }
    }

    private List<String> resolveUnionNode(TreeQuery.UnionNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException {

        Set<String> result = new HashSet<>();
        node.getNodes().sort(COMPARATOR.reversed());
        for (TreeQuery.Node subNode : node.getNodes()) {
            if (result.size()==includeSamples.size()) {
                logger.info("Skip node '{}'", subNode);
            } else {
                result.addAll(resolveNode(subNode, baseQuery, includeSamples));
            }
        }

        return new ArrayList<>(result);
    }

    private List<String> resolveIntersectNode(TreeQuery.IntersectionNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException {

        node.getNodes().sort(COMPARATOR.reversed());
        for (TreeQuery.Node subNode : node.getNodes()) {
            if (includeSamples.isEmpty()) {
                logger.info("Skip node '{}'", subNode);
            } else {
                includeSamples = resolveNode(subNode, baseQuery, includeSamples);
            }
        }

        return includeSamples;
    }

    private List<String> resolveQuery(TreeQuery.QueryNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException {

        Query query = new Query(baseQuery);
        query.putAll(node.getQuery());
        query.put(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSamples);
        query.put(VariantQueryParam.INCLUDE_FORMAT.key(), "GT," + VariantQueryParser.SAMPLE_ID);
        Set<String> samples = new HashSet<>();

        VariantDBIterator iterator = getVariantStorageManager().iterator(query, new QueryOptions(), getToken());
        while (iterator.hasNext()) {
            Variant next = iterator.next();
            for (List<String> samplesDatum : next.getStudies().get(0).getSamplesData()) {
                if (GenotypeClass.MAIN_ALT.test(samplesDatum.get(0))) {
                    samples.add(samplesDatum.get(1));
                }
            }
        }

        return new ArrayList<>(samples);
    }

}
