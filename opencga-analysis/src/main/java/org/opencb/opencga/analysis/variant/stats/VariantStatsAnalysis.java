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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyUpdateParams;
import org.opencb.opencga.core.models.variant.VariantStatsAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.VariantStatsAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Created by jacobo on 06/03/15.
 */
@Tool(id = VariantStatsAnalysis.ID, resource = Enums.Resource.VARIANT, description = VariantStatsAnalysis.DESCRIPTION)
public class VariantStatsAnalysis extends OpenCgaTool {

    public final static String ID = "variant-stats";
    public static final String DESCRIPTION = "Compute variant stats for any cohort and any set of variants."
            + " Optionally, index the result in the variant storage database.";

    public static final String STATS_AGGREGATION_CATALOG = VariantStorageOptions.STATS_AGGREGATION.key().replace(".", "_");

    private String studyFqn;

    private VariantStatsAnalysisParams toolParams = new VariantStatsAnalysisParams();
    private List<String> cohorts;

    private Query samplesQuery;
    private Query variantsQuery;
    private Aggregation aggregation;
    private Map<String, List<String>> cohortsMap;
    private Path outputFile;
    private Properties mappingFile;
    private Path mappingFilePath;
    private boolean dynamicCohort;


    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public VariantStatsAnalysis setStudy(String study) {
        params.put(ParamConstants.STUDY_PARAM, study);
        return this;
    }

    public VariantStatsAnalysis setCohort(List<String> cohort) {
        toolParams.setCohort(cohort);
        return this;
    }

    public VariantStatsAnalysis setIndex(boolean index) {
        toolParams.setIndex(index);
        return this;
    }

    /**
     * Samples of the cohort.
     * Optional if provided {@link #setCohort(List)}.
     *
     * @param samples samples list
     * @return this
     */
    public VariantStatsAnalysis setSamples(List<String> samples) {
        toolParams.setSamples(samples);
        return this;
    }

//    /**
//     * Samples query selecting samples of the cohort.
//     * Optional if provided {@link #cohorts}.
//     *
//     * @param samplesQuery sample query
//     * @return this
//     */
    public VariantStatsAnalysis setSamplesQuery(Query samplesQuery) {
        this.samplesQuery = samplesQuery;
        return this;
    }

    /**catalogManager.getStudyManager().resolveId(studyFqn, userId);
        studyFqn = study.getFqn();
     * Name of the cohort.
     * Optional if provided {@link #setSamples(List)}.
     * When used without {@link #setSamples(List)}, the cohort must be defined in catalog.
     * It's samples will be used to calculate the variant stats.
     * When used together with {@link #setSamples(List)}, this name will be just an alias to be used in the output file.
     *
     * @param cohortName cohort name
     * @return this
     */
    public VariantStatsAnalysis setCohortName(String cohortName) {
        return setCohort(Collections.singletonList(cohortName));
    }

    /**
     * Variants region query. If not provided, all variants from the study will be used.
     * @param region region filter
     * @return this
     */
    public VariantStatsAnalysis setRegion(String region) {
        toolParams.setRegion(region);
        return this;
    }

    /**
     * Variants gene query. If not provided, all variants from the study will be used.
     * @param gene gene filter
     * @return this
     */
    public VariantStatsAnalysis setGene(String gene) {
        toolParams.setGene(gene);
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(params.getString(ParamConstants.STUDY_PARAM), userId);
        studyFqn = study.getFqn();

        toolParams.updateParams(params);

        params.put(VariantStorageOptions.STATS_OVERWRITE.key(), toolParams.isOverwriteStats());
        params.put(VariantStorageOptions.STATS_UPDATE.key(), toolParams.isUpdateStats());
        params.put(VariantStorageOptions.STATS_AGGREGATION.key(), toolParams.getAggregated());
        params.put(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), toolParams.getAggregationMappingFile());
        params.put(VariantStorageOptions.RESUME.key(), toolParams.isResume());


        setUpStorageEngineExecutor(studyFqn);

        if (samplesQuery == null) {
            samplesQuery = new Query();
        }
        if (CollectionUtils.isNotEmpty(toolParams.getSamples())) {
            samplesQuery.put(SampleDBAdaptor.QueryParams.ID.key(), toolParams.getSamples());
        }

        cohorts = toolParams.getCohort();
        if (cohorts == null) {
            cohorts = new ArrayList<>();
        }

        if (variantsQuery == null) {
            variantsQuery = new Query();
        }

        variantsQuery.putIfAbsent(VariantQueryParam.STUDY.key(), studyFqn);
        if (StringUtils.isNotEmpty(toolParams.getRegion())) {
            variantsQuery.put(VariantQueryParam.REGION.key(), toolParams.getRegion());
        }
        if (StringUtils.isNotEmpty(toolParams.getGene())) {
            variantsQuery.put(VariantQueryParam.REGION.key(), toolParams.getGene());
        }

        aggregation = getAggregation(catalogManager, studyFqn, params, token);

        // if the study is aggregated and a mapping file is provided, pass it to storage
        // and create in catalog the cohorts described in the mapping file
        String aggregationMappingFile = params.getString(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key());
        if (AggregationUtils.isAggregated(aggregation) && isNotEmpty(aggregationMappingFile)) {
            mappingFilePath = getFilePath(aggregationMappingFile);
            mappingFile = readAggregationMappingFile(mappingFilePath);
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
                throw new ToolException("Unspecified cohort or list of samples");
            }
        }

        Path outdir;
        if (toolParams.isIndex()) {
            // Do not save intermediate files
            outdir = getScratchDir();
        } else {
            if (!samplesQuery.isEmpty()) {
                dynamicCohort = true;
                if (cohorts.isEmpty()) {
                    cohorts.add("COHORT");
                } else if (cohorts.size() > 1) {
                    throw new ToolException("Only one cohort name is accepted when using dynamic cohorts.");
                }
            }

            // Preserve intermediate files
            outdir = getOutDir();
        }

        outputFile = buildOutputFileName(cohorts, toolParams.getRegion(), outdir);

        executorParams.putAll(params);
        executorParams.append(VariantStorageOptions.STATS_AGGREGATION.key(), aggregation)
                .append(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), mappingFilePath)
                .append(DefaultVariantStatisticsManager.OUTPUT, outputFile);
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList("prepare-cohorts", getId());
    }

    @Override
    protected void run() throws Exception {
        step("prepare-cohorts", () -> {
            cohortsMap = new LinkedHashMap<>(cohorts.size());
            if (!toolParams.isIndex()) {
                // Don't need to synchronize storage metadata
                if (dynamicCohort) {
                    String cohortName = cohorts.get(0);

                    List<Sample> samples = catalogManager.getSampleManager()
                            .search(studyFqn, new Query(samplesQuery), new QueryOptions(QueryOptions.INCLUDE, "id"), token)
                            .getResults();
                    List<String> sampleNames = samples.stream().map(Sample::getId).collect(Collectors.toList());

                    cohortsMap.put(cohortName, sampleNames);
                    addAttribute("dynamicCohort", true);
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
                        throw new ToolException("Missing cohort or samples. "
                                + "Use cohort " + StudyEntry.DEFAULT_COHORT + " to compute stats for all indexed samples");
                    } else {
                        throw new ToolException("Unable to compute variant stats for cohorts " + emptyCohorts);
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
                    throw new ToolException(e);
                }
            }
        });

        step(() -> {
            if (toolParams.isIndex()) {
                variantStorageManager.stats(
                        studyFqn,
                        cohorts,
                        variantsQuery.getString(VariantQueryParam.REGION.key()),
                        executorParams,
                        token);
            } else {
                getToolExecutor(VariantStatsAnalysisExecutor.class)
                        .setStudy(studyFqn)
                        .setCohorts(cohortsMap)
                        .setOutputFile(outputFile)
                        .setVariantsQuery(variantsQuery)
                        .execute();
            }
        });
    }

    @Override
    protected void onShutdown() {
        try {
            if (toolParams.isIndex()) {
                updateCohorts(studyFqn, cohortsMap.keySet(), token, CohortStatus.INVALID, "");
            }
        } catch (CatalogException e) {
            logger.error("Error updating cohorts " + cohortsMap + " to status " + CohortStatus.INVALID, e);
        }
    }

    private Properties readAggregationMappingFile(Path aggregationMapFile) throws IOException {
        try (InputStream is = FileUtils.newInputStream(aggregationMapFile)) {
            Properties tagmap = new Properties();
            tagmap.load(is);
            return tagmap;
        }
    }

    private Path getFilePath(String aggregationMapFile) throws CatalogException {
        if (Files.exists(Paths.get(aggregationMapFile))) {
            return Paths.get(aggregationMapFile).toAbsolutePath();
        } else {
            return Paths.get(getCatalogManager().getFileManager()
                    .get(studyFqn, aggregationMapFile, QueryOptions.empty(), getToken()).first().getUri());
        }
    }

    protected Path buildOutputFileName(Collection<String> cohortIds, String region, Path outdir) {
        final String outputFileName;
        if (isNotEmpty(toolParams.getOutputFileName())) {
            outputFileName = toolParams.getOutputFileName();
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
            if (!toolParams.isIndex()) {
                outputFileNameBuilder.append(".tsv");
            }
            outputFileName = outputFileNameBuilder.toString();
        }
        return outdir.resolve(outputFileName);
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
                LoggerFactory.getLogger(VariantStatsAnalysis.class)
                        .warn("Calculating statistics with aggregation " + argsAggregation + " instead of " + studyAggregation);
            }
            aggregation = argsAggregation;
            // If studyAggregation is not define, update study aggregation
            if (studyAggregation == null) {
                //update study aggregation
                Map<String, Object> attributes = Collections.singletonMap(STATS_AGGREGATION_CATALOG, argsAggregation);
                StudyUpdateParams updateParams = new StudyUpdateParams()
                        .setAttributes(attributes);
                catalogManager.getStudyManager().update(studyId, updateParams, null, sessionId);
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

    public static IllegalArgumentException missingAggregationMappingFile(Aggregation aggregation) {
        return new IllegalArgumentException("Unable to calculate statistics for an aggregated study of type "
                + "\"" + aggregation + "\" without an aggregation mapping file.");
    }

}
