package org.opencb.opencga.analysis.variant.samples;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.SampleEligibilityAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.variant.samples.SampleEligibilityAnalysis.DESCRIPTION;

@Tool(id= SampleEligibilityAnalysis.ID, resource = Enums.Resource.VARIANT, description = DESCRIPTION)
public class SampleEligibilityAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "sample-eligibility";
    public static final String DESCRIPTION = "Filter samples by a complex query involving metadata and variants data";

    public static final String SAMPLE_PREFIX = "sample.";
    public static final String INDIVIDUAL_PREFIX = "individual.";
    private SampleEligibilityAnalysisParams analysisParams = new SampleEligibilityAnalysisParams();
    private TreeQuery treeQuery;
    private String studyFqn;
//    private LinkedList<String> steps;

    private final static Comparator<TreeQuery.Node> COMPARATOR = Comparator.comparing(SampleEligibilityAnalysis::toQueryValue);
    private static final Set<QueryParam> INVALID_QUERY_PARAMS;

    static {
        INVALID_QUERY_PARAMS = new HashSet<>();
        INVALID_QUERY_PARAMS.addAll(VariantQueryUtils.MODIFIER_QUERY_PARAMS);
//        INVALID_QUERY_PARAMS.add(VariantQueryParam.SAMPLE);
        INVALID_QUERY_PARAMS.add(VariantQueryParam.FILE);
        INVALID_QUERY_PARAMS.add(VariantQueryParam.STUDY);
        INVALID_QUERY_PARAMS.add(VariantQueryParam.FORMAT);
        INVALID_QUERY_PARAMS.add(VariantQueryParam.FILTER);
        INVALID_QUERY_PARAMS.add(VariantQueryParam.QUAL);
        INVALID_QUERY_PARAMS.add(VariantCatalogQueryUtils.FAMILY);
        INVALID_QUERY_PARAMS.add(VariantCatalogQueryUtils.PROJECT);
        INVALID_QUERY_PARAMS.add(VariantCatalogQueryUtils.FAMILY_DISORDER);
        INVALID_QUERY_PARAMS.add(VariantCatalogQueryUtils.FAMILY_MEMBERS);
        INVALID_QUERY_PARAMS.add(VariantCatalogQueryUtils.FAMILY_PROBAND);
        INVALID_QUERY_PARAMS.add(VariantCatalogQueryUtils.FAMILY_SEGREGATION);
        INVALID_QUERY_PARAMS.add(VariantCatalogQueryUtils.SAMPLE_ANNOTATION);
    }

    @Override
    protected void check() throws Exception {
        super.check();
        analysisParams.updateParams(params);
        studyFqn = getStudyFqn();

        if (analysisParams.isIndex()) {
            if (StringUtils.isEmpty(analysisParams.getCohortId())) {
                throw new IllegalArgumentException("Missing cohort-id");
            }
            Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), analysisParams.getCohortId());
            if (catalogManager.getCohortManager().count(studyFqn, query, getToken()).getNumResults() > 0) {
                throw new IllegalArgumentException("Unable to index result. Cohort '" + analysisParams.getCohortId() + "' already exists");
            }
        } else {
            if (StringUtils.isNotEmpty(analysisParams.getCohortId())) {
                throw new IllegalArgumentException("Found cohort-id, but index was not required.");
            }
        }

        treeQuery = new TreeQuery(analysisParams.getQuery());
        addAttribute("query", treeQuery.getRoot().toString());

        VariantQueryOptimizer.optimize(treeQuery);

        checkValidQueryFilters(treeQuery);

        treeQuery.log();
    }

    protected static void checkValidQueryFilters(TreeQuery treeQuery) throws ToolException {
        Set<String> invalidParams = new LinkedHashSet<>();
        treeQuery.forEachQuery(query -> {
            for (QueryParam invalidParam : INVALID_QUERY_PARAMS) {
                if (VariantQueryUtils.isValidParam(query, invalidParam)) {
                    invalidParams.add(invalidParam.key());
                }
            }
            if (VariantQueryUtils.isValidParam(query, VariantQueryParam.GENOTYPE)) {
                String genotype = query.getString(VariantQueryParam.GENOTYPE.key());
                if (genotype.contains(":") && !genotype.startsWith("*:")) {
                    invalidParams.add(VariantQueryParam.GENOTYPE.key());
                }
            }
            if (VariantQueryUtils.isValidParam(query, VariantQueryParam.SAMPLE)) {
                Object sample = query.remove(VariantQueryParam.SAMPLE.key());
                query.put(SAMPLE_PREFIX + SampleDBAdaptor.QueryParams.ID.key(), sample);
            }
            for (String param : query.keySet()) {
                if (!param.startsWith(SAMPLE_PREFIX) && !param.startsWith(INDIVIDUAL_PREFIX)) {
                    if (VariantCatalogQueryUtils.valueOf(param) == null) {
                        // Unknown Param
                        invalidParams.add(param);
                    }
                }
            }
        });
        if (!invalidParams.isEmpty()) {
            throw new ToolException("Invalid sample query. Unable to filter by params " + invalidParams);
        }
    }

    @Override
    protected List<String> getSteps() {
        if (analysisParams.isIndex()) {
            return Arrays.asList(getId(), "index");
        } else {
            return super.getSteps();
        }
    }

    @Override
    protected void run() throws Exception {
        List<String> samplesResult = new ArrayList<>();
        step(() -> {
            List<String> inputSamples = new ArrayList<>(getVariantStorageManager().getIndexedSamples(studyFqn, getToken()));
            Query baseQuery = new Query();
            baseQuery.put(VariantQueryParam.STUDY.key(), studyFqn);

            samplesResult.addAll(resolveNode(treeQuery.getRoot(), baseQuery, inputSamples));

            addAttribute("numSamples", samplesResult.size());
            logger.info("Found {} samples", samplesResult.size());

            try (PrintStream out = new PrintStream(getOutDir().resolve("samples.tsv").toFile())) {
                out.println("##num_samples=" + samplesResult.size());
                out.println("#SAMPLE");
                for (String s : samplesResult) {
                    out.println(s);
                }
            }
        });

        step("index", () -> {
            Cohort cohort = new Cohort()
                    .setId(analysisParams.getCohortId())
                    .setSamples(samplesResult.stream().map(s -> new Sample().setId(s)).collect(Collectors.toList()))
                    .setDescription("Result of analysis '" + getId() + "' after executing query " + analysisParams.getQuery());
            getCatalogManager().getCohortManager().create(studyFqn, cohort, new QueryOptions(), getToken());
        });
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
            case COMPLEMENT:
                return 1000 - toQueryValue(node.getNodes().get(0));
            case UNION:
                return node.getNodes().stream().mapToInt(SampleEligibilityAnalysis::toQueryValue).max().orElse(0);
            case INTERSECTION:
                return node.getNodes().stream().mapToInt(SampleEligibilityAnalysis::toQueryValue).min().orElse(0);
            default:
                throw new IllegalArgumentException("Unknown node type " + node.getType());
        }

    }

    private List<String> resolveNode(TreeQuery.Node node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException, IOException {
        switch (node.getType()) {
            case QUERY:
                return resolveQuery(((TreeQuery.QueryNode) node), baseQuery, includeSamples);
            case COMPLEMENT:
                return resolveComplementQuery(((TreeQuery.ComplementNode) node), baseQuery, includeSamples);
            case INTERSECTION:
                return resolveIntersectNode(((TreeQuery.IntersectionNode) node), baseQuery, includeSamples);
            case UNION:
                return resolveUnionNode(((TreeQuery.UnionNode) node), baseQuery, includeSamples);
            default:
                throw new IllegalArgumentException("Unknown node type " + node.getType());
        }
    }

    private List<String> resolveUnionNode(TreeQuery.UnionNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException, IOException {

        logger.info("Execute union-node with {} children for {} samples",
                node.getNodes().size(), includeSamples.size());

        includeSamples = new ArrayList<>(includeSamples);
        Set<String> result = new HashSet<>();
        node.getNodes().sort(COMPARATOR.reversed());
        for (TreeQuery.Node subNode : node.getNodes()) {
            if (includeSamples.isEmpty()) {
                logger.info("Skip node '{}'. All samples found", subNode);
            } else {
                List<String> thisNodeResult = resolveNode(subNode, baseQuery, includeSamples);
                includeSamples.removeAll(thisNodeResult);
                result.addAll(thisNodeResult);
            }
        }

        return new ArrayList<>(result);
    }

    private List<String> resolveIntersectNode(TreeQuery.IntersectionNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException, IOException {

        logger.info("Execute intersect-node with {} children at for {} samples",
                node.getNodes().size(), includeSamples.size());

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

    private List<String> resolveComplementQuery(TreeQuery.ComplementNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, IOException, StorageEngineException {
        logger.info("Execute complement-node for {} samples", includeSamples.size());
        List<String> subSamples = resolveNode(node.getNodes().get(0), baseQuery, includeSamples);
        logger.info("Discard {} of {} samples", subSamples.size(), includeSamples.size());

        includeSamples = new LinkedList<>(includeSamples);
        includeSamples.removeAll(subSamples);
        return includeSamples;
    }

    private List<String> resolveQuery(TreeQuery.QueryNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException, IOException {
        logger.info("Execute leaf-node '{}' for {} samples", node, includeSamples.size());

        Query variantsQuery = node.getQuery();
        Query sampleQuery = new Query();
        Query individualQuery = new Query();
        for (String key : new HashSet<>(variantsQuery.keySet())) {
            if (key.startsWith(SAMPLE_PREFIX)) {
                sampleQuery.put(key.substring(SAMPLE_PREFIX.length()), variantsQuery.getString(key));
                variantsQuery.remove(key);
            }
            if (key.startsWith(INDIVIDUAL_PREFIX)) {
                sampleQuery.put(key.substring(INDIVIDUAL_PREFIX.length()), variantsQuery.getString(key));
                variantsQuery.remove(key);
            }
        }
        if (!sampleQuery.isEmpty()) {
            int inputSampleSize = includeSamples.size();
            if (sampleQuery.containsKey(SampleDBAdaptor.QueryParams.ID.key())) {
                // Remove samples not in the query
                Set<String> samplesFromQuery = new HashSet<>(sampleQuery.getAsStringList(SampleDBAdaptor.QueryParams.ID.key()));
                includeSamples = new LinkedList<>(includeSamples);
                includeSamples.removeIf(s -> !samplesFromQuery.contains(s));
            }
            if (!includeSamples.isEmpty()) {
                sampleQuery.put(SampleDBAdaptor.QueryParams.ID.key(), includeSamples);
                includeSamples = getCatalogManager().getSampleManager()
                        .search(studyFqn, sampleQuery, new QueryOptions(QueryOptions.INCLUDE, "id"), getToken())
                        .getResults()
                        .stream()
                        .map(Sample::getId)
                        .collect(Collectors.toList());
            }

            logger.info("Filter samples with catalog samples metadata. Found {} samples out of {}",
                    includeSamples.size(), inputSampleSize);
            if (includeSamples.isEmpty()) {
                logger.info("Skip query leaf no sample passed the catalog sample filter.");
                return Collections.emptyList();
            }
        }

        if (!individualQuery.isEmpty()) {
            int inputSampleSize = includeSamples.size();
            individualQuery.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), includeSamples);
            includeSamples = getCatalogManager().getIndividualManager()
                    .search(studyFqn, individualQuery, new QueryOptions(QueryOptions.INCLUDE, "id"), getToken())
                    .getResults()
                    .stream()
                    .map(Individual::getSamples)
                    .flatMap(Collection::stream)
                    .map(Sample::getId)
                    .filter(new HashSet<>(includeSamples)::contains)
                    .collect(Collectors.toList());

            logger.info("Filter samples with catalog individuals metadata. Found {} samples out of {}",
                    includeSamples.size(), inputSampleSize);
            if (includeSamples.isEmpty()) {
                logger.info("Skip query leaf no sample passed the catalog individual filter.");
                return Collections.emptyList();
            }
        }

        Set<String> samples;
        if (params.getBoolean("direct")) {
            samples = resolveQueryDirect(node, baseQuery, includeSamples);
        } else {
            samples = resolveQuerySamplesData(node, baseQuery, includeSamples);
        }

        logger.info("Found {} sample in leaf '{}'", samples.size(), node);
        return new ArrayList<>(samples);
    }

    private Set<String> resolveQuerySamplesData(TreeQuery.QueryNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException, IOException {
        Query query = new Query(baseQuery);
        query.putAll(node.getQuery());
        String genotypes = null;
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.GENOTYPE)) {
            genotypes = query.getString(VariantQueryParam.GENOTYPE.key());
            query.remove(VariantQueryParam.GENOTYPE.key());
            if (genotypes.startsWith("*:")) {
                genotypes = genotypes.substring(2);
            }
        }
        Set<String> samples = new HashSet<>();
        includeSamples = new LinkedList<>(includeSamples);

        List<String> thisVariantSamples = new ArrayList<>(includeSamples.size());
        VariantDBIterator iterator = getVariantStorageManager()
                .iterator(new Query(query), new QueryOptions(VariantField.SUMMARY, true), getToken());
        while (iterator.hasNext()) {
            Variant next = iterator.next();
            StopWatch stopWatch = StopWatch.createStarted();
            logger.debug("[{}] start processing", next);
            includeSamples.removeAll(samples);
            if (includeSamples.isEmpty()) {
                logger.info("Shortcut at node '{}' after finding {} samples", node, samples.size());
                break;
            }

            int limit = 1000;
            int skip = 0;
            int numSamples;

            do {
                QueryOptions queryOptions = new QueryOptions();
                queryOptions.put(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSamples);
                queryOptions.put(QueryOptions.LIMIT, limit);
                queryOptions.put(QueryOptions.SKIP, skip);
                queryOptions.put(VariantQueryParam.GENOTYPE.key(), genotypes);

                Variant variant = getVariantStorageManager()
                        .getSampleData(next.toString(), studyFqn, queryOptions, getToken()).first();

                StudyEntry studyEntry = variant.getStudies().get(0);
                numSamples = studyEntry.getSamplesData().size();
                skip += numSamples;

                int sampleIdPos = studyEntry.getFormatPositions().get(VariantQueryParser.SAMPLE_ID);
                for (List<String> samplesDatum : studyEntry.getSamplesData()) {
                    if (GenotypeClass.MAIN_ALT.test(samplesDatum.get(0))) {
                        String sampleId = samplesDatum.get(sampleIdPos);
                        samples.add(sampleId);
                        thisVariantSamples.add(sampleId);
                    }
                }
            } while (numSamples == limit);

            logger.debug("[{}] found {} samples in {}", next, thisVariantSamples.size(), TimeUtils.durationToString(stopWatch));
            thisVariantSamples.clear();
        }
        try {
            iterator.close();
        } catch (Exception e) {
            throw VariantQueryException.internalException(e);
        }
        return samples;
    }

    private Set<String> resolveQueryDirect(TreeQuery.QueryNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException {
        Query query = new Query(baseQuery);
        query.putAll(node.getQuery());
        query.put(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSamples);
        query.put(VariantQueryParam.INCLUDE_FORMAT.key(), "GT," + VariantQueryParser.SAMPLE_ID);
        Predicate<String> genotypeFilter = GenotypeClass.MAIN_ALT;
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.GENOTYPE)) {
            String genotypes = query.getString(VariantQueryParam.GENOTYPE.key());
            query.remove(VariantQueryParam.GENOTYPE.key());
            if (genotypes.startsWith("*:")) {
                genotypes = genotypes.substring(2);
            }
            List<String> loadedGenotypes = getVariantStorageManager()
                    .getStudyMetadata(studyFqn, getToken()).getAttributes().getAsStringList(VariantStorageOptions.LOADED_GENOTYPES.key());
            List<String> genotypesList = VariantQueryParser.preProcessGenotypesFilter(Arrays.asList(genotypes.split(",")), loadedGenotypes);
            genotypeFilter = new HashSet<>(genotypesList)::contains;
        }
        Set<String> samples = new HashSet<>();

        VariantDBIterator iterator = getVariantStorageManager().iterator(query, new QueryOptions(), getToken());
        while (iterator.hasNext()) {
            Variant next = iterator.next();
            for (List<String> samplesDatum : next.getStudies().get(0).getSamplesData()) {
                String genotype = samplesDatum.get(0);
                if (GenotypeClass.MAIN_ALT.test(genotype) && genotypeFilter.test(genotype)) {
                    samples.add(samplesDatum.get(1));
                }
            }
        }
        try {
            iterator.close();
        } catch (Exception e) {
            throw VariantQueryException.internalException(e);
        }
        return samples;
    }

}
