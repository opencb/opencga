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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.OntologyTermAnnotation;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
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
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.variant.samples.SampleEligibilityAnalysis.DESCRIPTION;

@Tool(id= SampleEligibilityAnalysis.ID, resource = Enums.Resource.VARIANT, description = DESCRIPTION)
public class SampleEligibilityAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "sample-eligibility";
    public static final String DESCRIPTION = "Filter samples by a complex query involving metadata and variants data";

    public static final String SAMPLE_PREFIX = "sample.";
    public static final String INDIVIDUAL_PREFIX = "individual.";
    private static final int MAX_INCLUDE_SAMPLE_SIZE = 500;

    @ToolParams
    protected final SampleEligibilityAnalysisParams analysisParams = new SampleEligibilityAnalysisParams();

    private TreeQuery treeQuery;
    private String studyFqn;
    private ExecutorService executorService;
    private Future<List<String>> allSamplesFuture;
//    private LinkedList<String> steps;

    private final static Comparator<TreeQuery.Node> COMPARATOR = Comparator.comparing(SampleEligibilityAnalysis::toQueryValue);
    private static final Set<QueryParam> INVALID_QUERY_PARAMS;

    static {
        INVALID_QUERY_PARAMS = new HashSet<>();
        INVALID_QUERY_PARAMS.addAll(VariantQueryUtils.MODIFIER_QUERY_PARAMS);
//        INVALID_QUERY_PARAMS.add(VariantQueryParam.SAMPLE);
        INVALID_QUERY_PARAMS.add(VariantQueryParam.FILE);
        INVALID_QUERY_PARAMS.add(VariantQueryParam.STUDY);
        INVALID_QUERY_PARAMS.add(VariantQueryParam.SAMPLE_DATA);
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

    public static final String FILE_NAME_PREFIX = "sampleEligibilityAnalysisResult";

    @Override
    protected void check() throws Exception {
        super.check();
        studyFqn = getStudyFqn();
        if (StringUtils.isEmpty(analysisParams.getQuery())) {
            throw new IllegalArgumentException("Missing query");
        }

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
        executorService = Executors.newSingleThreadExecutor();
        logger.info("Num threads : {}", Runtime.getRuntime().availableProcessors());
        addAttribute("numThreads", Runtime.getRuntime().availableProcessors());
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
        Set<String> samplesResult = new HashSet<>();
        step(() -> {
            samplesResult.addAll(resolve(treeQuery));

            addAttribute("numSamples", samplesResult.size());
            logger.info("Found {} samples", samplesResult.size());

            printResult(samplesResult);
        });

        if (analysisParams.isIndex()) {
            step("index", () -> {
                Cohort cohort = new Cohort()
                        .setId(analysisParams.getCohortId())
                        .setSamples(samplesResult.stream().map(s -> new Sample().setId(s)).collect(Collectors.toList()))
                        .setDescription("Result of analysis '" + getId() + "' after executing query " + analysisParams.getQuery());
                getCatalogManager().getCohortManager().create(studyFqn, cohort, new QueryOptions(), getToken());
            });
        }
    }

    private void printResult(Set<String> samplesResult) throws CatalogException, IOException {
        SampleEligibilityAnalysisResult analysisResult = new SampleEligibilityAnalysisResult()
                .setDate(TimeUtils.getTime())
                .setQuery(analysisParams.getQuery())
                .setQueryPlan(treeQuery.getRoot())
                .setStudy(getStudyFqn())
                .setNumSamples(samplesResult.size())
                .setIndividuals(new ArrayList<>(samplesResult.size()));

        if (!samplesResult.isEmpty()) {
            Iterator<Individual> it = getCatalogManager().getIndividualManager()
                    .iterator(studyFqn,
                            new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), samplesResult),
                            new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                    IndividualDBAdaptor.QueryParams.ID.key(),
                                    IndividualDBAdaptor.QueryParams.NAME.key(),
                                    IndividualDBAdaptor.QueryParams.SEX.key(),
                                    IndividualDBAdaptor.QueryParams.DISORDERS.key(),
                                    IndividualDBAdaptor.QueryParams.PHENOTYPES.key(),
                                    IndividualDBAdaptor.QueryParams.SAMPLES.key() + "." + SampleDBAdaptor.QueryParams.ID.key(),
                                    IndividualDBAdaptor.QueryParams.SAMPLES.key() + "." + SampleDBAdaptor.QueryParams.CREATION_DATE.key(),
                                    IndividualDBAdaptor.QueryParams.SAMPLES.key() + "." + SampleDBAdaptor.QueryParams.SOMATIC.key()
                            )), getToken());
            Set<String> missingSamples = new HashSet<>(samplesResult);
            while (it.hasNext()) {
                Individual individual = it.next();
                List<Sample> samples = individual.getSamples().stream()
                        .filter(s -> samplesResult.contains(s.getId()))
                        .collect(Collectors.toList());
                for (Sample sample : samples) {
                    missingSamples.remove(sample.getId());
                    analysisResult.getIndividuals().add(new SampleEligibilityAnalysisResult.ElectedIndividual()
                            .setName(individual.getName())
                            .setId(individual.getId())
                            .setSex(individual.getSex())
                            .setDisorders(getIds(individual.getDisorders()))
                            .setPhenotypes(getIds(individual.getPhenotypes()))
                            .setSample(new SampleEligibilityAnalysisResult.SampleSummary()
                                    .setId(sample.getId())
                                    .setCreationDate(sample.getCreationDate())
                                    .setSomatic(sample.isSomatic()))
                    );
                }
            }
            if (!missingSamples.isEmpty()) {
                logger.warn("Individual not found for {} samples", missingSamples.size());
                Iterator<Sample> samples = catalogManager.getSampleManager()
                        .iterator(studyFqn, new Query(SampleDBAdaptor.QueryParams.ID.key(), new ArrayList<>(missingSamples)),
                                new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                                        SampleDBAdaptor.QueryParams.ID.key(),
                                        SampleDBAdaptor.QueryParams.CREATION_DATE.key(),
                                        SampleDBAdaptor.QueryParams.SOMATIC.key()
                                )), getToken());
                while (samples.hasNext()) {
                    Sample sample = samples.next();
                    analysisResult.getIndividuals().add(new SampleEligibilityAnalysisResult.ElectedIndividual()
                            .setName(sample.getId())
                            .setId(sample.getId())
                            .setSex(IndividualProperty.Sex.UNKNOWN)
                            .setDisorders(Collections.emptyList())
                            .setPhenotypes(Collections.emptyList())
                            .setSample(new SampleEligibilityAnalysisResult.SampleSummary()
                                    .setId(sample.getId())
                                    .setCreationDate(sample.getCreationDate())
                                    .setSomatic(sample.isSomatic()))
                    );
                }
            }
        }

        printTsv(samplesResult, analysisResult);
        printJson(analysisResult);

    }

    private void printJson(SampleEligibilityAnalysisResult analysisResult) throws IOException {
        File resultFile = getOutDir().resolve(FILE_NAME_PREFIX + ".json").toFile();
        new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL).writeValue(resultFile, analysisResult);
    }

    private void printTsv(Set<String> samplesResult, SampleEligibilityAnalysisResult analysisResult) throws FileNotFoundException {
        try (PrintStream out = new PrintStream(getOutDir().resolve(FILE_NAME_PREFIX + ".tsv").toFile())) {
            out.println("##study=" + analysisResult.getStudy());
            out.println("##query=" + analysisResult.getQuery());
            out.println("##num_samples=" + samplesResult.size());
            out.println("##date=" + analysisResult.getDate());
            out.println("#INDIVIDUAL\tSAMPLE\tPHENOTYPES\tDISOREDERS");
            for (SampleEligibilityAnalysisResult.ElectedIndividual individual : analysisResult.getIndividuals()) {
                String individualId = individual.getId();
                String sampleId = individual.getSample().getId();
                String phenotypes = String.join(",", individual.getPhenotypes());
                String disorders = String.join(",", individual.getDisorders());
                out.println(individualId + "\t" + sampleId + "\t" + phenotypes + "\t" + disorders);
            }
        }
    }

    private List<String> getIds(List<? extends OntologyTermAnnotation> elements) {
        return elements == null
                ? Collections.emptyList()
                : elements.stream().map(OntologyTermAnnotation::getId).collect(Collectors.toList());
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
                ParsedVariantQuery.VariantQueryXref xrefs = VariantQueryParser.parseXrefs(query);
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


    private List<String> resolve(TreeQuery treeQuery)
            throws CatalogException, StorageEngineException, IOException, ExecutionException, InterruptedException {

        allSamplesFuture = executorService
                .submit(() -> new ArrayList<>(getVariantStorageManager().getIndexedSamples(studyFqn, getToken())));

        return resolveNode(treeQuery.getRoot(), null);
    }

    private List<String> resolveNode(TreeQuery.Node node, List<String> includeSamples)
            throws CatalogException, ExecutionException, InterruptedException {
        switch (node.getType()) {
            case QUERY:
                return resolveQuery(((TreeQuery.QueryNode) node), includeSamples);
            case COMPLEMENT:
                return resolveComplementQuery(((TreeQuery.ComplementNode) node), includeSamples);
            case INTERSECTION:
                return resolveIntersectNode(((TreeQuery.IntersectionNode) node), includeSamples);
            case UNION:
                return resolveUnionNode(((TreeQuery.UnionNode) node), includeSamples);
            default:
                throw new IllegalArgumentException("Unknown node type " + node.getType());
        }
    }

    private List<String> resolveUnionNode(TreeQuery.UnionNode node, List<String> includeSamples)
            throws CatalogException, ExecutionException, InterruptedException {

        if (includeSamples == null) {
            includeSamples = getAllSamplesIfDone();
        }
        logger.info("Execute union-node with {} children for {} samples",
                node.getNodes().size(), includeSamples == null ? "?" : includeSamples.size());

        Set<String> result = new HashSet<>();
        node.getNodes().sort(COMPARATOR.reversed());
        for (TreeQuery.Node subNode : node.getNodes()) {
            if (includeSamples == null) {
                includeSamples = getAllSamplesIfDone();
                if (includeSamples != null) {
                    includeSamples.removeAll(result);
                }
            }
            if (includeSamples != null && includeSamples.isEmpty()) {
                logger.info("Skip node '{}'. All samples found", subNode);
            } else {
                List<String> thisNodeResult = resolveNode(subNode, includeSamples);
                if (includeSamples != null) {
                    includeSamples.removeAll(thisNodeResult);
                }
                result.addAll(thisNodeResult);
            }
        }

        return new ArrayList<>(result);
    }

    private List<String> resolveIntersectNode(TreeQuery.IntersectionNode node, List<String> includeSamples)
            throws CatalogException, ExecutionException, InterruptedException {

        logger.info("Execute intersect-node with {} children at for {} samples",
                node.getNodes().size(), includeSamples == null ? "?" : includeSamples.size());

        node.getNodes().sort(COMPARATOR.reversed());
        for (TreeQuery.Node subNode : node.getNodes()) {
            if (includeSamples != null && includeSamples.isEmpty()) {
                logger.info("Skip node '{}'", subNode);
            } else {
                includeSamples = resolveNode(subNode, includeSamples);
            }
        }

        return includeSamples;
    }

    private List<String> resolveComplementQuery(TreeQuery.ComplementNode node, List<String> includeSamples)
            throws CatalogException, ExecutionException, InterruptedException {

        logger.info("Execute complement-node for {} samples", includeSamples == null ? "?" : includeSamples.size());
        List<String> subSamples = resolveNode(node.getNodes().get(0), includeSamples);
        if (includeSamples == null) {
            // Force get all samples
            includeSamples = getAllSamplesForce();
        }
        logger.info("Discard {} of {} samples", subSamples.size(), includeSamples.size());

        includeSamples = new LinkedList<>(includeSamples);
        includeSamples.removeAll(subSamples);
        return includeSamples;
    }

    private List<String> resolveQuery(TreeQuery.QueryNode node, List<String> includeSamples)
            throws CatalogException, ExecutionException, InterruptedException {
        if (includeSamples == null) {
            logger.info("Execute leaf-node '{}'", node);
        } else {
            logger.info("Execute leaf-node '{}' for {} samples", node, includeSamples.size());
        }

        Query variantsQuery = new Query(node.getQuery());
        Query sampleQuery = new Query();
        Query individualQuery = new Query();
        for (String key : new HashSet<>(variantsQuery.keySet())) {
            if (key.startsWith(SAMPLE_PREFIX)) {
                sampleQuery.put(key.substring(SAMPLE_PREFIX.length()), variantsQuery.getString(key));
                variantsQuery.remove(key);
            }
            if (key.startsWith(INDIVIDUAL_PREFIX)) {
                individualQuery.put(key.substring(INDIVIDUAL_PREFIX.length()), variantsQuery.getString(key));
                variantsQuery.remove(key);
            }
        }

        Set<String> samples = resolveVariantQuery(node, variantsQuery, includeSamples);
        samples = resolveSampleCatalogQuery(sampleQuery, samples);
        samples = resolveIndividualCatalogQuery(individualQuery, samples);

        logger.info("Found {} sample in leaf '{}'", samples.size(), node);
        return new ArrayList<>(samples);
    }

    private Set<String> resolveVariantQuery(TreeQuery.QueryNode node, Query variantsQuery, List<String> includeSamples)
            throws ExecutionException, InterruptedException {
        if (variantsQuery.isEmpty()) {
            if (includeSamples == null) {
                // Force get all samples
                includeSamples = getAllSamplesForce();
            }
            return new HashSet<>(includeSamples);
        }
//        if (params.getBoolean("direct")) {
//            return resolveQueryDirect(node, baseQuery, includeSamples);
//        } else {
//            return resolveVariantQuerySamplesData(node, baseQuery, new AtomicReference<>(includeSamples));
//        }

        try {
            return resolveVariantQuerySamplesData(node, variantsQuery, new AtomicReference<>(includeSamples));
        } catch (Exception e) {
            try {
                logger.warn("Error resolving variant query node: {}", e.getMessage());
                logger.warn("Retry one time");
                return resolveVariantQuerySamplesData(node, variantsQuery, new AtomicReference<>(includeSamples));
            } catch (Exception e2) {
                e.addSuppressed(e2);
                throw e;
            }
        }

    }

    private Set<String> resolveSampleCatalogQuery(Query sampleQuery, Set<String> samples) throws CatalogException {
        if (!sampleQuery.isEmpty() && !samples.isEmpty()) {
            int inputSampleSize = samples.size();
            if (sampleQuery.containsKey(SampleDBAdaptor.QueryParams.ID.key())) {
                // Remove samples not in the query
                Set<String> samplesFromQuery = new HashSet<>(sampleQuery.getAsStringList(SampleDBAdaptor.QueryParams.ID.key()));
                samples = new HashSet<>(samples);
                samples.removeIf(s -> !samplesFromQuery.contains(s));
            }
            if (!samples.isEmpty()) {
                sampleQuery.put(SampleDBAdaptor.QueryParams.ID.key(), samples);
                samples = getCatalogManager().getSampleManager()
                        .search(studyFqn, sampleQuery, new QueryOptions(QueryOptions.INCLUDE, "id"), getToken())
                        .getResults()
                        .stream()
                        .map(Sample::getId)
                        .collect(Collectors.toSet());
            }

            logger.info("Filter samples with catalog samples metadata. Found {} samples out of {}",
                    samples.size(), inputSampleSize);
            if (samples.isEmpty()) {
                logger.info("Skip query leaf no sample passed the catalog sample filter.");
            }
        }
        return samples;
    }

    private Set<String> resolveIndividualCatalogQuery(Query individualQuery, Set<String> samples) throws CatalogException {
        if (!individualQuery.isEmpty() && !samples.isEmpty()) {
            int inputSampleSize = samples.size();
            individualQuery.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), samples);
            samples = getCatalogManager().getIndividualManager()
                    .search(studyFqn, individualQuery, new QueryOptions(QueryOptions.INCLUDE, "id,samples.id"), getToken())
                    .getResults()
                    .stream()
                    .map(Individual::getSamples)
                    .flatMap(Collection::stream)
                    .map(Sample::getId)
                    .filter(new HashSet<>(samples)::contains)
                    .collect(Collectors.toSet());

            logger.info("Filter samples with catalog individuals metadata. Found {} samples out of {}",
                    samples.size(), inputSampleSize);

            if (samples.isEmpty()) {
                logger.info("Skip query leaf no sample passed the catalog individual filter.");
            }
        }
        return samples;
    }

    private Set<String> resolveVariantQuerySamplesData(TreeQuery.QueryNode node, Query query,
                                                       AtomicReference<List<String>> includeSamplesInputR)
            throws ExecutionException, InterruptedException {
        final String genotypes;
        query = new Query(query);
        query.put(VariantQueryParam.STUDY.key(), studyFqn);
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.GENOTYPE)) {
            String genotypesValue = query.getString(VariantQueryParam.GENOTYPE.key());
            query.remove(VariantQueryParam.GENOTYPE.key());
            genotypes = genotypesValue.startsWith("*:") ? genotypesValue.substring(2) : genotypesValue;
        } else {
            genotypes = null;
        }
        Set<String> samples = new HashSet<>();
        AtomicReference<CopyOnWriteArrayList<String>> includeSamplesR = new AtomicReference<>(null);

        if (includeSamplesInputR.get() == null) {
            includeSamplesInputR.compareAndSet(null, getAllSamplesIfDone());
        }
        if (includeSamplesInputR.get() != null) {
            includeSamplesR.compareAndSet(null, new CopyOnWriteArrayList<>(includeSamplesInputR.get()));
        }

        ProgressLogger progressLogger = new ProgressLogger("Variants processed:").setBatchSize(50);

        int numTasks = Runtime.getRuntime().availableProcessors();
        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder()
                .setNumTasks(numTasks)
                .setBatchSize(1)
                .setCapacity(20)
                .build();
        ParallelTaskRunner<Variant, Variant> ptr = new ParallelTaskRunner<>(
                new VariantDBReaderWithShortcut(query, includeSamplesR, node, samples),
                new ResolveQuerySamplesDataTask(includeSamplesR, includeSamplesInputR, samples, progressLogger, genotypes), null, config);

        ptr.run();

        if (includeSamplesR.get() != null && includeSamplesR.get().isEmpty()) {
            logger.info("Shortcut at node '{}' after finding {} samples", node, samples.size());
        }

        return samples;
    }

    private List<String> getAllSamplesIfDone() throws InterruptedException, ExecutionException {
        if (allSamplesFuture.isDone()) {
            return getAllSamplesForce();
        }
        return null;
    }

    private List<String> getAllSamplesForce() throws InterruptedException, ExecutionException {
        return new LinkedList<>(allSamplesFuture.get());
    }

    private Set<String> resolveQueryDirect(TreeQuery.QueryNode node, Query baseQuery, List<String> includeSamples)
            throws CatalogException, StorageEngineException {
        Query query = new Query(baseQuery);
        query.putAll(node.getQuery());
        query.put(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSamples);
        query.put(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT");
        query.put(VariantQueryParam.INCLUDE_SAMPLE_ID.key(), true);
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
            for (SampleEntry sampleEntry : next.getStudies().get(0).getSamples()) {
                String genotype = sampleEntry.getData().get(0);
                if (GenotypeClass.MAIN_ALT.test(genotype) && genotypeFilter.test(genotype)) {
                    samples.add(sampleEntry.getData().get(1));
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

    private class VariantDBReaderWithShortcut extends VariantDBReader {
        private final AtomicReference<CopyOnWriteArrayList<String>> includeSamplesR;
        private final TreeQuery.QueryNode node;
        private final Set<String> samples;

        public VariantDBReaderWithShortcut(Query query, AtomicReference<CopyOnWriteArrayList<String>> includeSamplesR, TreeQuery.QueryNode node, Set<String> samples) {
            super(SampleEligibilityAnalysis.this.getVariantStorageManager().iterable(SampleEligibilityAnalysis.this.getToken()), new Query(query), new QueryOptions(QueryOptions.INCLUDE, VariantField.ID));
            this.includeSamplesR = includeSamplesR;
            this.node = node;
            this.samples = samples;
        }

        @Override
        public List<Variant> read(int batchSize) {
            if (includeSamplesR.get() != null && includeSamplesR.get().isEmpty()) {
                logger.info("Shortcut at node '{}' after finding {} samples", node, samples.size());
                return Collections.emptyList();
            } else {
                return super.read(batchSize);
            }
        }
    }

    private class ResolveQuerySamplesDataTask implements Task<Variant, Variant> {
        private final AtomicReference<CopyOnWriteArrayList<String>> includeSamplesR;
        private final AtomicReference<List<String>> includeSamplesInputR;
        private final Set<String> samples;
        private final ProgressLogger progressLogger;
        private final String genotypes;

        public ResolveQuerySamplesDataTask(AtomicReference<CopyOnWriteArrayList<String>> includeSamplesR,
                                           AtomicReference<List<String>> includeSamplesInputR,
                                           Set<String> samples,
                                           ProgressLogger progressLogger,
                                           String genotypes) {
            this.includeSamplesR = includeSamplesR;
            this.includeSamplesInputR = includeSamplesInputR;
            this.samples = samples;
            this.progressLogger = progressLogger;
            this.genotypes = genotypes;
        }

        @Override
        public List<Variant> apply(List<Variant> variants) throws Exception {
            List<String> thisVariantSamples = includeSamplesR.get() == null
                    ? new LinkedList<>()
                    : new ArrayList<>(includeSamplesR.get().size());
            for (Variant next : variants) {
                StopWatch stopWatch = StopWatch.createStarted();

                List<String> includeSamples;
                if (includeSamplesR.get() != null && includeSamplesR.get().size() < MAX_INCLUDE_SAMPLE_SIZE) {
                    includeSamples = new ArrayList<>(includeSamplesR.get());
                    if (includeSamples.isEmpty()) {
                        // Shortcut
                        break;
                    }
                } else {
                    includeSamples = null;
                }
                if (includeSamplesR.get() != null && includeSamplesR.get().isEmpty()) {
                    // Shortcut
                    break;
                }

                // Asynchronous check to get all samples
                if (includeSamplesInputR.get() == null) {
                    includeSamplesInputR.compareAndSet(null, SampleEligibilityAnalysis.this.getAllSamplesIfDone());
                    if (includeSamplesInputR.get() != null) {
                        includeSamplesR.compareAndSet(null, new CopyOnWriteArrayList<>(includeSamplesInputR.get()));
                    }
                }
                if (includeSamplesR.get() != null) {
                    synchronized (includeSamplesR) {
                        includeSamplesR.get().removeAll(samples);
                    }
                }
                logger.debug("[{}] start processing. Include {} samples{}",
                        next,
                        includeSamplesR.get() == null ? "?" : includeSamplesR.get().size(),
                        includeSamples == null ? " (skip includeSample param)" : "");
                progressLogger.increment(1, () ->
                        ". Selected " + samples.size() + "/"
                                + (includeSamplesInputR.get() == null ? "?" : includeSamplesInputR.get().size()) + " samples "
                                + "(" + (includeSamplesR.get() == null ? "?" : includeSamplesR.get().size()) + " pending"
                                + (includeSamples == null ? ", skip includeSample param)" : ")"));

                int limit = 5000;
                int skip = 0;
                int numSamples;

                do {
                    QueryOptions queryOptions = new QueryOptions();
                    if (includeSamples != null) {
                        queryOptions.put(VariantQueryParam.INCLUDE_SAMPLE.key(), new ArrayList<>(includeSamples));
                    }
                    queryOptions.put(QueryOptions.LIMIT, limit);
                    queryOptions.put(QueryOptions.SKIP, skip);
                    queryOptions.put(VariantQueryParam.GENOTYPE.key(), genotypes);
                    queryOptions.put(VariantQueryParam.INCLUDE_GENOTYPE.key(), true);
                    queryOptions.put(QueryOptions.EXCLUDE, Arrays.asList(VariantField.ANNOTATION, VariantField.STUDIES_STATS));

                    Variant variant = SampleEligibilityAnalysis.this.getVariantStorageManager()
                            .getSampleData(next.toString(), studyFqn, queryOptions, SampleEligibilityAnalysis.this.getToken()).first();

                    StudyEntry studyEntry = variant.getStudies().get(0);
                    numSamples = studyEntry.getSamples().size();
                    skip += numSamples;

                    for (SampleEntry sampleEntry : studyEntry.getSamples()) {
                        if (GenotypeClass.MAIN_ALT.test(sampleEntry.getData().get(0))) {
                            String sampleId = sampleEntry.getSampleId();
                            thisVariantSamples.add(sampleId);
                        }
                    }
                } while (numSamples == limit);

                if (stopWatch.getTime(TimeUnit.MILLISECONDS) > 2000) {
                    logger.info("Slow response for variant {}. Found {} samples in {}",
                            next, thisVariantSamples.size(), TimeUtils.durationToString(stopWatch));
                }
                logger.debug("[{}] found {} samples in {}", next, thisVariantSamples.size(), TimeUtils.durationToString(stopWatch));
                synchronized (samples) {
                    samples.addAll(thisVariantSamples);
                }
                thisVariantSamples.clear();
            }
            return variants;
        }
    }

}
