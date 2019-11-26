/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.analysis.variant.stats;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.tools.variant.stats.AggregationUtils;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.variant.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.analysis.variant.operations.StorageOperation;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.analysis.result.FileResult;
import org.opencb.opencga.core.analysis.variant.VariantStatsAnalysisExecutor;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Created by jacobo on 06/03/15.
 */
@Analysis(id = VariantStatsAnalysis.ID, type = Analysis.AnalysisType.VARIANT,
        description = "Compute variant stats for any cohort and any set of variants.")
public class VariantStatsAnalysis extends OpenCgaAnalysis {

    public final static String ID = "variant-stats";

    public static final String STATS_AGGREGATION_CATALOG = VariantStorageOptions.STATS_AGGREGATION.key().replace(".", "_");
    private List<String> cohorts;
    private String studyFqn;

    private Query samplesQuery;
    private Query variantsQuery;
    private boolean index;
    private Aggregation aggregation;
    private DataStore dataStore;
    private Map<String, List<String>> cohortsMap;
    private Path outputFile;
    private Properties mappingFile;
    private boolean dynamicCohort;


    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public VariantStatsAnalysis setStudy(String study) {
        this.studyFqn = study;
        return this;
    }

    public VariantStatsAnalysis setCohorts(List<String> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public VariantStatsAnalysis setIndex(boolean index) {
        this.index = index;
        return this;
    }

    /**
     * Samples query selecting samples of the cohort.
     * Optional if provided {@link #cohorts}.
     *
     * @param samplesQuery sample query
     * @return this
     */
    public VariantStatsAnalysis setSamplesQuery(Query samplesQuery) {
        this.samplesQuery = samplesQuery;
        return this;
    }

    /**
     * Name of the cohort.
     * Optional if provided {@link #samplesQuery}.
     * When used without {@link #samplesQuery}, the cohort must be defined in catalog.
     * It's samples will be used to calculate the variant stats.
     * When used together with {@link #samplesQuery}, this name will be just an alias to be used in the output file.
     *
     * @param cohortName cohort name
     * @return this
     */
    public VariantStatsAnalysis setCohortName(String cohortName) {
        this.cohorts = Collections.singletonList(cohortName);
        return this;
    }

    /**
     * Variants query. If not provided, all variants from the study will be used.
     * @param variantsQuery variants query.
     * @return this
     */
    public VariantStatsAnalysis setVariantsQuery(Query variantsQuery) {
        this.variantsQuery = variantsQuery;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, userId);
        studyFqn = study.getFqn();

        setUpStorageEngineExecutor(studyFqn);

        if (samplesQuery == null) {
            samplesQuery = new Query();
        }
        if (cohorts == null) {
            cohorts = new ArrayList<>();
        }

        if (variantsQuery == null) {
            variantsQuery = new Query();
        }
        variantsQuery.putIfAbsent(VariantQueryParam.STUDY.key(), studyFqn);
        String region = variantsQuery.getString(VariantQueryParam.REGION.key());
        if (StringUtils.isEmpty(region)) {
            region = params.getString(VariantQueryParam.REGION.key());
            if (StringUtils.isEmpty(region)) {
                variantsQuery.put(VariantQueryParam.REGION.key(), region);
            }
        }

        params.put("variantsQuery", variantsQuery);


        aggregation = getAggregation(catalogManager, studyFqn, params, token);

        // if the study is aggregated and a mapping file is provided, pass it to storage
        // and create in catalog the cohorts described in the mapping file
        String aggregationMappingFile = params.getString(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key());
        if (AggregationUtils.isAggregated(aggregation) && isNotEmpty(aggregationMappingFile)) {
            mappingFile = readAggregationMappingFile(aggregationMappingFile);
        }

        if (samplesQuery.isEmpty() && cohorts.isEmpty()) {
            if (AggregationUtils.isAggregated(aggregation)) {
                if (mappingFile != null) {
                    cohorts = new ArrayList<>(VariantAggregatedStatsCalculator.getCohorts(mappingFile));
                } else if (aggregation.equals(Aggregation.BASIC)) {
                    cohorts = Collections.singletonList(StudyEntry.DEFAULT_COHORT);
                } else {
                    throw missingAggregationMappingFile(aggregation);
                }
            }
            if (cohorts.isEmpty()) {
                throw new AnalysisException("Unspecified cohort or list of samples");
            }
        }

        Path outdir;
        if (index) {
            cohorts = checkCohorts(studyFqn, aggregation, cohorts, token);

            // Do not save intermediate files
            outdir = getScratchDir();
        } else {
            if (!samplesQuery.isEmpty()) {
                dynamicCohort = true;
                if (cohorts.isEmpty()) {
                    cohorts.add("COHORT");
                } else if (cohorts.size() > 1) {
                    throw new AnalysisException("Only one cohort name is accepted when using dynamic cohorts.");
                }
            }

            // Preserve intermediate files
            outdir = getOutDir();
        }

        params.put("cohorts", cohorts);

        outputFile = buildOutputFileName(cohorts, region, outdir);

        executorParams.putAll(params);
        executorParams.append(VariantStorageOptions.STATS_AGGREGATION.key(), aggregation)
                .append(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), mappingFile)
                .append(DefaultVariantStatisticsManager.OUTPUT, outputFile);

        dataStore = VariantStorageManager.getDataStore(catalogManager, studyFqn, File.Bioformat.VARIANT, token);
    }

    @Override
    public List<String> getSteps() {
        return Arrays.asList("prepare-cohorts", getId());
    }

    @Override
    protected void run() throws Exception {
        step("prepare-cohorts", () -> {
            cohortsMap = new LinkedHashMap<>(cohorts.size());

            if (index) {
                boolean updateStats = params.getBoolean(VariantStorageOptions.STATS_UPDATE.key(), false);
                boolean resume = params.getBoolean(VariantStorageOptions.RESUME.key(), VariantStorageOptions.RESUME.defaultValue());

                // Synchronize catalog with storage
                VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
                CatalogStorageMetadataSynchronizer synchronizer =
                        new CatalogStorageMetadataSynchronizer(catalogManager, variantStorageEngine.getMetadataManager());
                synchronizer.synchronizeCatalogStudyFromStorage(studyFqn, token);

                cohortsMap = checkCanCalculateCohorts(studyFqn, cohorts, updateStats, resume, token);
            } else {

                // Don't need to synchronize storage metadata
                if (dynamicCohort) {
                    String cohortName = cohorts.get(0);

                    List<Sample> samples = catalogManager.getSampleManager()
                            .search(studyFqn, new Query(samplesQuery), new QueryOptions(QueryOptions.INCLUDE, "id"), token)
                            .getResults();
                    List<String> sampleNames = samples.stream().map(Sample::getId).collect(Collectors.toList());

                    cohortsMap.put(cohortName, sampleNames);
                    params.put("dynamicCohort", true);
                    params.put("samplesQuery", samplesQuery);
                    params.put("samples", sampleNames);
                } else {
                    for (String cohortName : cohorts) {
                        Cohort cohort = catalogManager.getCohortManager().get(studyFqn, cohortName, new QueryOptions(), token).first();
                        cohortsMap.put(cohortName, cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
                    }
                }
            }

            if (!AggregationUtils.isAggregated(aggregation)) {
                // Remove non-indexed samples
                Set<String> indexedSamples = variantStorageManager.getIndexedSamples(studyFqn, token);
                cohortsMap.values().forEach(samples -> samples.removeIf(s -> !indexedSamples.contains(s)));

                List<String> emptyCohorts = new ArrayList<>();
                Set<String> sampleNames = new HashSet<>();
                cohortsMap.forEach((cohortName, samples) -> {
                    if (samples.size() <= 1) {
                        emptyCohorts.add(cohortName);
                    }
                    sampleNames.addAll(samples);
                });

                if (!emptyCohorts.isEmpty()) {
                    if (dynamicCohort) {
                        throw new AnalysisException("Missing cohort or samples. "
                                + "Use cohort " + StudyEntry.DEFAULT_COHORT + " to compute stats for all indexed samples");
                    } else {
                        throw new AnalysisException("Unable to compute variant stats for cohorts " + emptyCohorts);
                    }
                }

                // check read permission
                try {
                    variantStorageManager.checkQueryPermissions(
                            new Query(variantsQuery)
                                    .append(VariantQueryParam.STUDY.key(), studyFqn)
                                    .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleNames),
                            new QueryOptions(),
                            token);
                } catch (CatalogException | StorageEngineException e) {
                    throw new AnalysisException(e);
                }
            }
        });

        if (index) {
            step(() -> {
                try {
                    // Modify cohort status to "CALCULATING"
                    updateCohorts(studyFqn, cohortsMap.keySet(), getToken(), Cohort.CohortStatus.CALCULATING, "Start calculating stats");

                    getAnalysisExecutor(VariantStatsAnalysisExecutor.class)
                            .setStudy(studyFqn)
                            .setCohorts(cohortsMap)
                            .setOutputFile(outputFile)
                            .setVariantsQuery(variantsQuery)
                            .setIndex(index)
                            .execute();

                    // Modify cohort status to "READY"
                    updateCohorts(studyFqn, cohortsMap.keySet(), getToken(), Cohort.CohortStatus.READY, "");
                } catch (Exception e) {
                    // Error!
                    logger.error("Error executing stats. Set cohorts status to " + Cohort.CohortStatus.INVALID, e);
                    // Modify to "INVALID"
                    try {
                        updateCohorts(studyFqn, cohortsMap.keySet(), getToken(), Cohort.CohortStatus.INVALID,
                                "Error calculating stats: " + e.getMessage());
                    } catch (CatalogException ex) {
                        addError(ex);
                    }
                    throw new AnalysisException("Error calculating statistics.", e);
                }
            });
        } else {
            step(() -> {
                getAnalysisExecutor(VariantStatsAnalysisExecutor.class)
                        .setStudy(studyFqn)
                        .setCohorts(cohortsMap)
                        .setOutputFile(outputFile)
                        .setVariantsQuery(variantsQuery)
                        .setIndex(index)
                        .execute();

                addFile(outputFile, FileResult.FileType.TAB_SEPARATED);
            });
        }
    }

    @Override
    protected void onShutdown() {
        try {
            if (index) {
                updateCohorts(studyFqn, cohortsMap.keySet(), token, Cohort.CohortStatus.INVALID, "");
            }
        } catch (CatalogException e) {
            logger.error("Error updating cohorts " + cohortsMap + " to status " + Cohort.CohortStatus.INVALID, e);
        }
    }


    private VariantStorageEngine getVariantStorageEngine(DataStore dataStore) throws StorageEngineException {
        return StorageEngineFactory.get(variantStorageManager.getStorageConfiguration())
                .getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
    }

    protected Path buildOutputFileName(Collection<String> cohortIds, String region, Path outdir) {
        final String outputFileName;
        if (isNotEmpty(params.getString(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME))) {
            outputFileName = params.getString(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME);
        } else {
            StringBuilder outputFileNameBuilder;
            outputFileNameBuilder = new StringBuilder("variant_stats_");
            if (isNotEmpty(region)) {
                outputFileNameBuilder.append(region).append('_');
            }
            for (Iterator<String> iterator = cohortIds.iterator(); iterator.hasNext();) {
                String cohortId = iterator.next();
                outputFileNameBuilder.append(cohortId);
                if (iterator.hasNext()) {
                    outputFileNameBuilder.append('_');
                }
            }
            if (!index) {
                outputFileNameBuilder.append(".tsv");
            }
            outputFileName = outputFileNameBuilder.toString();
        }
        return outdir.resolve(outputFileName);
    }

    /**
     * Must provide a list of cohorts or a aggregation_mapping_properties file.
     * @param studyId   Study
     * @param aggregation Aggregation type for this study. {@link #getAggregation}
     * @param cohorts   List of cohorts
     * @param sessionId User's sessionId
     * @return          Checked list of cohorts
     * @throws CatalogException if an error on Catalog
     * @throws IOException if an IO error reading the aggregation map file (if any)
     */
    protected List<String> checkCohorts(String studyId, Aggregation aggregation, List<String> cohorts, String sessionId)
            throws AnalysisException, CatalogException, IOException {
        List<String> cohortIds;

        // Check aggregation mapping properties
        String tagMap = params.getString(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key());
        List<String> cohortsByAggregationMapFile = Collections.emptyList();
        if (!isBlank(tagMap)) {
            if (!AggregationUtils.isAggregated(aggregation)) {
                throw nonAggregatedWithMappingFile();
            }
            cohortsByAggregationMapFile = createCohortsByAggregationMapFile(studyId, tagMap, sessionId);
        } else if (AggregationUtils.isAggregated(aggregation)) {
            if (aggregation.equals(Aggregation.BASIC)) {
                cohortsByAggregationMapFile = createCohortsIfNeeded(studyId, Collections.singleton(StudyEntry.DEFAULT_COHORT), sessionId);
            } else {
                throw missingAggregationMappingFile(aggregation);
            }
        }

        if (cohorts == null || cohorts.isEmpty()) {
            // If no aggregation map file provided
            if (cohortsByAggregationMapFile.isEmpty()) {
                throw missingCohorts();
            } else {
                cohortIds = cohortsByAggregationMapFile;
            }
        } else {
            cohortIds = new ArrayList<>(cohorts.size());
            for (String cohort : cohorts) {
                String cohortId = catalogManager.getCohortManager().get(studyId, cohort,
                        new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ID.key()), sessionId).first().getId();
                cohortIds.add(cohortId);
            }
            if (!cohortsByAggregationMapFile.isEmpty()) {
                if (cohortIds.size() != cohortsByAggregationMapFile.size() || !cohortIds.containsAll(cohortsByAggregationMapFile)) {
                    throw differentCohortsThanMappingFile();
                }
            }
        }
        return cohortIds;
    }

    private List<String> createCohortsByAggregationMapFile(String studyId, String aggregationMapFile, String sessionId)
            throws IOException, CatalogException {
        Properties tagmap = readAggregationMappingFile(aggregationMapFile);
        Set<String> cohortNames = VariantAggregatedStatsCalculator.getCohorts(tagmap);
        return createCohortsIfNeeded(studyId, cohortNames, sessionId);
    }

    private Properties readAggregationMappingFile(String aggregationMapFile) throws IOException {
        Properties tagmap = new Properties();
        try (InputStream is = FileUtils.newInputStream(Paths.get(aggregationMapFile))) {
            tagmap.load(is);
        }
        return tagmap;
    }

    private List<String> createCohortsIfNeeded(String studyId, Set<String> cohortNames, String sessionId) throws CatalogException {
        List<String> cohorts = new ArrayList<>();
        // Silent query, so it does not fail for missing cohorts
        Set<String> catalogCohorts = catalogManager.getCohortManager().get(studyId, new ArrayList<>(cohortNames),
                new QueryOptions(QueryOptions.INCLUDE, "name,id"), true, sessionId)
                .getResults()
                .stream()
                .filter(Objects::nonNull)
                .map(Cohort::getId)
                .collect(Collectors.toSet());
        for (String cohortName : cohortNames) {
            if (!catalogCohorts.contains(cohortName)) {
                DataResult<Cohort> cohort = catalogManager.getCohortManager().create(studyId, cohortName, Study.Type.COLLECTION, "",
                        Collections.emptyList(), null, null, sessionId);
                logger.info("Creating cohort {}", cohortName);
                cohorts.add(cohort.first().getId());
            } else {
                logger.debug("cohort {} was already created", cohortName);
                cohorts.add(cohortName);
            }
        }
        return cohorts;
    }

    /**
     * Check if a set of given cohorts are available to calculate statistics.
     *
     * @param studyFqn      Study fqn
     * @param cohortIds     Set of cohorts
     * @param updateStats   Update already existing stats
     * @param resume        Resume statistics calculation
     * @param sessionId     User's sessionId
     * @return Map from cohortId to Cohort
     * @throws CatalogException if an error on Catalog
     */
    protected Map<String, List<String>> checkCanCalculateCohorts(String studyFqn, List<String> cohortIds,
                                                                 boolean updateStats, boolean resume, String sessionId)
            throws CatalogException, AnalysisException {
        Map<String, List<String>> cohortMap = new HashMap<>(cohortIds.size());
        for (String cohortId : cohortIds) {
            Cohort cohort = catalogManager.getCohortManager().get(studyFqn, cohortId, null, sessionId).first();
            switch (cohort.getStatus().getName()) {
                case Cohort.CohortStatus.NONE:
                case Cohort.CohortStatus.INVALID:
                    break;
                case Cohort.CohortStatus.READY:
                    if (updateStats) {
                        catalogManager.getCohortManager().setStatus(studyFqn, cohortId, Cohort.CohortStatus.INVALID, "", sessionId);
                        break;
                    } else {
                        // If not updating the stats or resuming, can't calculate statistics for a cohort READY
                        if (!resume) {
                            throw unableToCalculateCohortReady(cohort);
                        }
                    }
                    break;
                case Cohort.CohortStatus.CALCULATING:
                    if (!resume) {
                        throw unableToCalculateCohortCalculating(cohort);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown status " + cohort.getStatus().getName());
            }
            cohortMap.put(cohort.getId(), cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
        }
        return cohortMap;
    }

    protected void updateCohorts(String studyId, Collection<String> cohortIds, String sessionId, String status, String message)
            throws CatalogException {
        for (String cohortId : cohortIds) {
            catalogManager.getCohortManager().setStatus(studyId, cohortId, status, message, sessionId);
        }
    }

    /**
     * If the study is aggregated and a mapping file is provided, pass it to
     * and create in catalog the cohorts described in the mapping file.
     *
     * If the study aggregation was not defined, updateStudy with the provided aggregation type
     *
     * @param catalogManager CatalogManager
     * @param studyId   StudyId where calculate stats
     * @param options   Options
     * @param sessionId Users sessionId
     * @return          Effective study aggregation type
     * @throws CatalogException if something is wrong with catalog
     */
    public static Aggregation getAggregation(CatalogManager catalogManager, String studyId, ObjectMap options, String sessionId)
            throws CatalogException {
        QueryOptions include = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.ATTRIBUTES.key());
        Study study = catalogManager.getStudyManager().get(studyId, include, sessionId).first();
        Aggregation argsAggregation = options.get(VariantStorageOptions.STATS_AGGREGATION.key(), Aggregation.class, Aggregation.NONE);
        Object studyAggregationObj = study.getAttributes().get(STATS_AGGREGATION_CATALOG);
        Aggregation studyAggregation = null;
        if (studyAggregationObj != null) {
            studyAggregation = AggregationUtils.valueOf(studyAggregationObj.toString());
        }

        final Aggregation aggregation;
        if (AggregationUtils.isAggregated(argsAggregation)) {
            if (studyAggregation != null && !studyAggregation.equals(argsAggregation)) {
                // FIXME: Throw an exception?
                LoggerFactory.getLogger(StorageOperation.class)
                        .warn("Calculating statistics with aggregation " + argsAggregation + " instead of " + studyAggregation);
            }
            aggregation = argsAggregation;
            // If studyAggregation is not define, update study aggregation
            if (studyAggregation == null) {
                //update study aggregation
                Map<String, Aggregation> attributes = Collections.singletonMap(STATS_AGGREGATION_CATALOG,
                        argsAggregation);
                ObjectMap parameters = new ObjectMap("attributes", attributes);
                catalogManager.getStudyManager().update(studyId, parameters, null, sessionId);
            }
        } else {
            if (studyAggregation == null) {
                aggregation = Aggregation.NONE;
            } else {
                aggregation = studyAggregation;
            }
        }
        return aggregation;
    }

    public static AnalysisException differentCohortsThanMappingFile() {
        return new AnalysisException("Given cohorts (if any) must match with cohorts in the aggregation mapping file.");
    }

    public static AnalysisException missingCohorts() {
        return new AnalysisException("Unable to index stats if no cohort is specified.");
    }

    public static IllegalArgumentException missingAggregationMappingFile(Aggregation aggregation) {
        return new IllegalArgumentException("Unable to calculate statistics for an aggregated study of type "
                + "\"" + aggregation + "\" without an aggregation mapping file.");
    }

    public static IllegalArgumentException nonAggregatedWithMappingFile() {
        return new IllegalArgumentException("Unable to use an aggregation mapping file for non aggregated study");
    }


    public static AnalysisException unableToCalculateCohortReady(Cohort cohort) {
        return new AnalysisException("Unable to calculate stats for cohort "
                + "{ uid: " + cohort.getUid() + " id: \"" + cohort.getId() + "\" }"
                + " with status \"" + cohort.getStatus().getName() + "\". "
                + "Resume or update stats for continue calculation");
    }

    public static AnalysisException unableToCalculateCohortCalculating(Cohort cohort) {
        return new AnalysisException("Unable to calculate stats for cohort "
                + "{ uid: " + cohort.getUid() + " id: \"" + cohort.getId() + "\" }"
                + " with status \"" + cohort.getStatus().getName() + "\". "
                + "Resume for continue calculation.");
    }

}
