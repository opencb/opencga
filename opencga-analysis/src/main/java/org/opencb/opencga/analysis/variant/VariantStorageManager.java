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

package org.opencb.opencga.analysis.variant;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.tools.variant.converters.ga4gh.Ga4ghVariantConverter;
import org.opencb.biodata.tools.variant.converters.ga4gh.factories.AvroGa4GhVariantFactory;
import org.opencb.biodata.tools.variant.converters.ga4gh.factories.ProtoGa4GhVariantFactory;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.analysis.models.StudyInfo;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.analysis.variant.metadata.CatalogVariantMetadataFactory;
import org.opencb.opencga.analysis.variant.operations.*;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.analysis.result.AnalysisResult;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.BeaconResponse;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleData;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.commons.datastore.core.QueryOptions.empty;
import static org.opencb.opencga.catalog.db.api.StudyDBAdaptor.QueryParams.FQN;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.NONE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.addDefaultLimit;

public class VariantStorageManager extends StorageManager {

    private final VariantCatalogQueryUtils catalogUtils;

    public VariantStorageManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);
        catalogUtils = new VariantCatalogQueryUtils(catalogManager);
    }

    public void clearCache(String studyId, String type, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);

    }

    // -------------------------//
    //   Import/Export methods  //
    // -------------------------//

    /**
     * Loads the given file into an empty study.
     * <p>
     * The input file should have, in the same directory, a metadata file, with the same name ended with
     * {@link org.opencb.opencga.storage.core.variant.io.VariantExporter#METADATA_FILE_EXTENSION}
     *
     * @param inputUri  Variants input file in avro format.
     * @param study     Study where to load the variants
     * @param sessionId User's session id
     * @throws CatalogException       if there is any error with Catalog
     * @throws IOException            if there is any I/O error
     * @throws StorageEngineException if there si any error loading the variants
     */
    public void importData(URI inputUri, String study, String sessionId)
            throws CatalogException, IOException, StorageEngineException {

        VariantImportStorageOperation op = new VariantImportStorageOperation(catalogManager, storageEngineFactory);
        StudyInfo studyInfo = getStudyInfo(study, Collections.emptyList(), sessionId);
        op.importData(studyInfo, inputUri, sessionId);

    }

    /**
     * Exports the result of the given query and the associated metadata.
     *
     * @param outputFile   Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat Output format.
     * @param study        Study to export
     * @param sessionId    User's session id
     * @return List of generated files
     * @throws CatalogException       if there is any error with Catalog
     * @throws IOException            If there is any IO error
     * @throws StorageEngineException If there is any error exporting variants
     */
    public List<URI> exportData(String outputFile, VariantOutputFormat outputFormat, String study, String sessionId)
            throws StorageEngineException, CatalogException, IOException, AnalysisException {
        Query query = new Query(VariantQueryParam.INCLUDE_STUDY.key(), study)
                .append(VariantQueryParam.STUDY.key(), study);
        return exportData(outputFile, outputFormat, null, query, new QueryOptions(), sessionId);
    }

    /**
     * Exports the result of the given query and the associated metadata.
     * @param outputFile    Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat  Variant Output format.
     * @param variantsFile  Optional variants file.
     * @param query         Query with the variants to export
     * @param queryOptions  Query options
     * @param sessionId     User's session id
     * @return              List of generated files
     * @throws CatalogException if there is any error with Catalog
     * @throws IOException  If there is any IO error
     * @throws StorageEngineException  If there is any error exporting variants
     */
    public List<URI> exportData(String outputFile, VariantOutputFormat outputFormat, String variantsFile,
                                Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, IOException, StorageEngineException, AnalysisException {
        if (query == null) {
            query = new Query();
        }

        catalogUtils.parseQuery(query, sessionId);
        checkSamplesPermissions(query, queryOptions, sessionId);

        Path outDir;
        if (StringUtils.isEmpty(outputFile)) {
            outDir = Paths.get("");
        } else {
            URI uri = URI.create(outputFile);
            if (StringUtils.isEmpty(uri.getScheme()) || uri.getScheme().equals("file")) {
                java.io.File file = Paths.get(outputFile).toFile();
                if (file.exists() && file.isDirectory()) {
                    outDir = Paths.get(outputFile).toAbsolutePath();
                } else {
                    outDir = Paths.get(outputFile).toAbsolutePath().getParent();
                }
            } else {
                outDir = Paths.get("");
            }
        }
        VariantExportStorageOperation operation = new VariantExportStorageOperation()
                .setQuery(query)
                .setOutputFormat(outputFormat)
                .setVariantsFile(variantsFile)
                .setOutputFile(outputFile);
        operation.setUp(null, catalogManager, this, queryOptions, outDir, sessionId);

        AnalysisResult result = operation.start();

        return result.getOutputFiles()
                .stream()
                .map(fileResult -> outDir.resolve(fileResult.getPath()).toUri())
                .collect(Collectors.toList());
    }

    // --------------------------//
    //   Data Operation methods  //
    // --------------------------//

    public List<StoragePipelineResult> index(String study, String fileId, String outDir, ObjectMap config, String sessionId)
            throws AnalysisException {
        return index(study, Arrays.asList(fileId.split(",")), outDir, config, sessionId);
    }

    public List<StoragePipelineResult> index(String study, List<String> files, String outDir, ObjectMap config, String sessionId)
            throws AnalysisException {
        VariantFileIndexerStorageOperation operation = new VariantFileIndexerStorageOperation()
                .setStudyId(study)
                .setFiles(files);
        operation.setUp(null, catalogManager, this, config, Paths.get(outDir), sessionId);
        operation.start();

        return operation.getStoragePipelineResults();
    }

    public void searchIndexSamples(String study, List<String> samples, QueryOptions queryOptions, String sessionId)
            throws StorageEngineException, IOException, VariantSearchException, CatalogException {
        DataStore dataStore = getDataStore(study, sessionId);
        VariantStorageEngine variantStorageEngine =
                getVariantStorageEngine(dataStore);
        variantStorageEngine.getOptions().putAll(queryOptions);

        variantStorageEngine.secondaryIndexSamples(study, samples);
    }

    public void removeSearchIndexSamples(String study, List<String> samples, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, VariantSearchException {
        DataStore dataStore = getDataStore(study, sessionId);
        VariantStorageEngine variantStorageEngine =
                getVariantStorageEngine(dataStore);
        variantStorageEngine.getOptions().putAll(queryOptions);

        variantStorageEngine.removeSecondaryIndexSamples(study, samples);

    }

    public void searchIndex(String study, String sessionId)
            throws StorageEngineException, IOException, VariantSearchException, CatalogException {
        searchIndex(new Query(STUDY.key(), study), new QueryOptions(), false, sessionId);
    }

    public void searchIndex(Query query, QueryOptions queryOptions, boolean overwrite, String sessionId) throws StorageEngineException,
            IOException, VariantSearchException, CatalogException {
//        String userId = catalogManager.getUserManager().getUserId(sessionId);
//        long studyId = catalogManager.getStudyManager().getId(userId, study);
        String study = catalogUtils.getAnyStudy(query, sessionId);
        DataStore dataStore = getDataStore(study, sessionId);
        VariantStorageEngine variantStorageEngine =
                getVariantStorageEngine(dataStore);
        catalogUtils.parseQuery(query, sessionId);
        variantStorageEngine.searchIndex(query, queryOptions, overwrite);
    }

    public void removeStudy(String study, String sessionId, QueryOptions options)
            throws CatalogException, IOException, StorageEngineException {
        VariantRemoveStorageOperation removeOperation = new VariantRemoveStorageOperation(catalogManager, storageEngineFactory);

        removeOperation.removeStudy(study, options, sessionId);
    }

    public List<File> removeFile(List<String> files, String study, String sessionId, QueryOptions options)
            throws CatalogException, IOException, StorageEngineException {
        VariantRemoveStorageOperation removeOperation = new VariantRemoveStorageOperation(catalogManager, storageEngineFactory);

        return removeOperation.removeFiles(study, files, options, sessionId);
    }

    public List<File> annotate(String study, Query query, String outDir, ObjectMap config, String sessionId)
            throws StorageEngineException, URISyntaxException, CatalogException, IOException {
        return annotate(null, study, query, outDir, config, sessionId);
    }

    public List<File> annotate(String project, String studies, Query query, String outDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException, IOException, URISyntaxException {
        VariantAnnotationStorageOperation annotOperation = new VariantAnnotationStorageOperation(catalogManager, storageConfiguration);

        List<String> studyIds;
        if (StringUtils.isNotEmpty(studies)) {
            studyIds = Arrays.asList(studies.split(","));
        } else if (StringUtils.isEmpty(studies) && StringUtils.isEmpty(project)) {
            // If non study or project is given, read all the studies from the user
            String userId = catalogManager.getUserManager().getUserId(sessionId);
            studyIds = catalogManager.getStudyManager().resolveIds(Collections.emptyList(), userId).stream()
                    .map(Study::getFqn)
                    .collect(Collectors.toList());
        } else {
            // If project is present, no study information is needed
            studyIds = Collections.emptyList();
        }
        List<StudyInfo> studiesList = new ArrayList<>(studyIds.size());
        for (String studyId : studyIds) {
            studiesList.add(getStudyInfo(studyId, Collections.emptyList(), sessionId));
        }
        return annotOperation.annotateVariants(project, studiesList, query, outDir, sessionId, config);
    }

    public void saveAnnotation(String project, String annotationName, ObjectMap params, String sessionId)
            throws StorageEngineException, VariantAnnotatorException, CatalogException {

        DataStore dataStore = getDataStoreByProjectId(project, sessionId);
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);

        CatalogStorageMetadataSynchronizer.updateProjectMetadata(catalogManager, variantStorageEngine.getMetadataManager(), project, sessionId);

        variantStorageEngine.saveAnnotation(annotationName, params);
    }

    public void deleteAnnotation(String project, String annotationName, ObjectMap params, String sessionId)
            throws StorageEngineException, VariantAnnotatorException, CatalogException {

        DataStore dataStore = getDataStoreByProjectId(project, sessionId);
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);

        CatalogStorageMetadataSynchronizer.updateProjectMetadata(catalogManager, variantStorageEngine.getMetadataManager(), project, sessionId);

        variantStorageEngine.deleteAnnotation(annotationName, params);
    }

    public DataResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options, String sessionId)
            throws StorageEngineException, CatalogException {
        QueryOptions finalOptions = VariantQueryUtils.validateAnnotationQueryOptions(options);
        return secure(query, finalOptions, sessionId, (engine) -> engine.getAnnotation(name, query, finalOptions));
    }

    public DataResult<ProjectMetadata.VariantAnnotationMetadata> getAnnotationMetadata(String name, String project, String sessionId)
            throws StorageEngineException, CatalogException {
        Query query = new Query(VariantCatalogQueryUtils.PROJECT.key(), project);
        return secure(query, empty(), sessionId, (engine) -> engine.getAnnotationMetadata(name));
    }

    public void stats(String study, List<String> cohorts, List<String> samples,
                      String outDir, boolean index, ObjectMap config, String sessionId)
            throws AnalysisException {
        stats(study, cohorts, new Query(SampleDBAdaptor.QueryParams.ID.key(), samples), outDir, index, config, sessionId);
    }

    public void stats(String study, List<String> cohorts, Query samplesQuery,
                      String outDir, boolean index, ObjectMap config, String sessionId)
            throws AnalysisException {
        VariantStatsAnalysis variantStatsAnalysis = new VariantStatsAnalysis()
                .setStudy(study)
                .setCohorts(cohorts)
                .setVariantsQuery(new Query(REGION.key(), config.getString(REGION.key())))
                .setSamplesQuery(samplesQuery)
                .setIndex(index);
        variantStatsAnalysis.setUp(null, catalogManager, this, config, Paths.get(outDir), sessionId);
        variantStatsAnalysis.start();
    }

    public void deleteStats(List<String> cohorts, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public List<VariantScoreMetadata> listVariantScores(String study, String sessionId) throws CatalogException, StorageEngineException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        String studyFqn = catalogManager.getStudyManager().resolveId(study, userId).getFqn();
        DataStore dataStore = getDataStore(studyFqn, sessionId);
        VariantStorageEngine engine = getVariantStorageEngine(dataStore);

        return engine.getMetadataManager().getStudyMetadata(studyFqn).getVariantScores();
    }

    public void loadVariantScore(String study, URI scoreFile, String scoreName, String cohort1, String cohort2,
                                 VariantScoreFormatDescriptor descriptor, ObjectMap options, String sessionId)
            throws CatalogException, StorageEngineException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        String studyFqn = catalogManager.getStudyManager().resolveId(study, userId).getFqn();

        DataStore dataStore = getDataStore(studyFqn, sessionId);
        VariantStorageEngine engine = getVariantStorageEngine(dataStore);

        Cohort cohort1Obj = catalogManager.getCohortManager().get(study, cohort1, new QueryOptions(), sessionId).first();
        List<String> cohort1Samples = cohort1Obj.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        engine.getMetadataManager().registerCohort(studyFqn, cohort1Obj.getId(), cohort1Samples);
        cohort1 = cohort1Obj.getId();

        if (StringUtils.isNotEmpty(cohort2)) {
            Cohort cohort2Obj = catalogManager.getCohortManager().get(study, cohort2, new QueryOptions(), sessionId).first();
            List<String> cohort2Samples = cohort2Obj.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
            engine.getMetadataManager().registerCohort(studyFqn, cohort2Obj.getId(), cohort2Samples);
            cohort2 = cohort2Obj.getId();
        }

        engine.loadVariantScore(scoreFile, study, scoreName, cohort1, cohort2, descriptor, options);
    }

    public void removeVariantScore(String study, String scoreName, ObjectMap options, String sessionId)
            throws CatalogException, StorageEngineException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        String studyFqn = catalogManager.getStudyManager().resolveId(study, userId).getFqn();

        DataStore dataStore = getDataStore(studyFqn, sessionId);
        VariantStorageEngine engine = getVariantStorageEngine(dataStore);


        engine.removeVariantScore(study, scoreName, options);
    }

    public void sampleIndex(String studyStr, List<String> samples, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException {

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        DataStore dataStore = getDataStore(study.getFqn(), sessionId);
        VariantStorageEngine engine = getVariantStorageEngine(dataStore);

        if (CollectionUtils.isEmpty(samples)) {
            throw new IllegalArgumentException("Empty list of samples");
        }

        engine.sampleIndex(study.getFqn(), samples, config);
    }

    public void sampleIndexAnnotate(String studyStr, List<String> samples, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException {

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        DataStore dataStore = getDataStore(study.getFqn(), sessionId);
        VariantStorageEngine engine = getVariantStorageEngine(dataStore);

        if (CollectionUtils.isEmpty(samples)) {
            throw new IllegalArgumentException("Empty list of samples");
        }

        engine.sampleIndexAnnotate(study.getFqn(), samples, config);
    }

    public void familyIndex(String studyStr, List<String> familiesStr, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException {

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        DataStore dataStore = getDataStore(study.getFqn(), sessionId);
        VariantStorageEngine engine = getVariantStorageEngine(dataStore);

        if (CollectionUtils.isEmpty(familiesStr)) {
            throw new IllegalArgumentException("Empty list of families");
        }

        List<List<String>> trios = new LinkedList<>();

        VariantStorageMetadataManager metadataManager = engine.getMetadataManager();

        if (familiesStr.size() == 1 && familiesStr.get(0).equals(VariantQueryUtils.ALL)) {
            DBIterator<Family> iterator = catalogManager.getFamilyManager().iterator(studyStr, new Query(), new QueryOptions(), sessionId);
            while (iterator.hasNext()) {
                Family family = iterator.next();
                trios.addAll(catalogUtils.getTriosFromFamily(study.getFqn(), family, metadataManager, true, sessionId));
            }
        } else {
            boolean skipIncompleteFamily = config.getBoolean("skipIncompleteFamily", false);
            for (String familyId : familiesStr) {
                Family family = catalogManager.getFamilyManager().get(studyStr, familyId, null, sessionId).first();
                trios.addAll(catalogUtils.getTriosFromFamily(study.getFqn(), family, metadataManager, skipIncompleteFamily, sessionId));
            }
        }

        engine.familyIndex(study.getFqn(), trios, config);
    }

    public void fillGaps(String studyStr, List<String> samples, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException {

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        DataStore dataStore = getDataStore(study.getFqn(), sessionId);
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);

        if (samples == null || samples.size() < 2) {
            throw new IllegalArgumentException("Fill gaps operation requires at least two samples!");
        }
        variantStorageEngine.fillGaps(study.getFqn(), samples, config);
    }

    public void fillMissing(String studyStr, boolean overwrite, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        DataStore dataStore = getDataStore(study.getFqn(), sessionId);
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);

        variantStorageEngine.fillMissing(study.getFqn(), config, overwrite);
    }

    // ---------------------//
    //   Query methods      //
    // ---------------------//

    public VariantQueryResult<Variant> get(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        return secure(query, queryOptions, sessionId, Enums.Action.SEARCH, engine -> {
            logger.debug("getVariants {}, {}", query, queryOptions);
            VariantQueryResult<Variant> result = engine.get(query, queryOptions);
            logger.debug("gotVariants {}, {}, in {}ms", result.getNumResults(), result.getNumTotalResults(), result.getTime());
            return result;
        });
    }

    @SuppressWarnings("unchecked")
    public <T> VariantQueryResult<T> get(Query query, QueryOptions queryOptions, String sessionId, Class<T> clazz)
            throws CatalogException, IOException, StorageEngineException {
        VariantQueryResult<Variant> result = get(query, queryOptions, sessionId);
        List<T> variants;
        if (clazz == Variant.class) {
            return (VariantQueryResult<T>) result;
        } else if (clazz == org.ga4gh.models.Variant.class) {
            Ga4ghVariantConverter<org.ga4gh.models.Variant> converter = new Ga4ghVariantConverter<>(new AvroGa4GhVariantFactory());
            variants = (List<T>) converter.apply(result.getResults());
        } else if (clazz == ga4gh.Variants.Variant.class) {
            Ga4ghVariantConverter<ga4gh.Variants.Variant> converter = new Ga4ghVariantConverter<>(new ProtoGa4GhVariantFactory());
            variants = (List<T>) converter.apply(result.getResults());
        } else {
            throw new IllegalArgumentException("Unknown variant format " + clazz);
        }
        return new VariantQueryResult<>(
                result.getTime(),
                result.getNumResults(),
                result.getNumMatches(),
                result.getEvents(),
                variants,
                result.getSamples(),
                result.getSource(),
                result.getApproximateCount(),
                result.getApproximateCountSamplingSize(), null);

    }

    public DataResult<VariantMetadata> getMetadata(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        return secure(query, queryOptions, sessionId, engine -> {
            StopWatch watch = StopWatch.createStarted();
            VariantMetadataFactory metadataFactory = new CatalogVariantMetadataFactory(catalogManager, engine.getDBAdaptor(), sessionId);
            VariantMetadata metadata = metadataFactory.makeVariantMetadata(query, queryOptions);
            return new DataResult<>(((int) watch.getTime()), Collections.emptyList(), 1, Collections.singletonList(metadata), 1);
        });
    }

    //TODO: GroupByFieldEnum
    public DataResult groupBy(String field, Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        return (DataResult) secure(query, queryOptions, sessionId, engine -> engine.groupBy(query, field, queryOptions));
    }

    public DataResult rank(Query query, String field, int limit, boolean asc, String sessionId)
            throws StorageEngineException, CatalogException, IOException {
        int limitMax = 30;
        int limitDefault = 10;
        return (DataResult) secure(query, null, sessionId,
                engine -> engine.rank(query, field, (limit > 0) ? Math.min(limit, limitMax) : limitDefault, asc));
    }

    public DataResult<Long> count(Query query, String sessionId) throws CatalogException, StorageEngineException, IOException {
        return secure(query, new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES), sessionId,
                engine -> engine.count(query));
    }

    public DataResult distinct(Query query, String field, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        return (DataResult) secure(query, new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES), sessionId,
                engine -> engine.distinct(query, field));
    }

    public VariantQueryResult<Variant> getPhased(Variant variant, String study, String sample, String sessionId, QueryOptions options)
            throws CatalogException, IOException, StorageEngineException {
        return secure(new Query(VariantQueryParam.STUDY.key(), study), options, sessionId,
                engine -> engine.getPhased(variant.toString(), study, sample, options, 5000));
    }

    public DataResult getFrequency(Query query, int interval, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        return (DataResult) secure(query, null, sessionId, engine -> {
            String[] regions = getRegions(query);
            if (regions.length != 1) {
                throw new IllegalArgumentException("Unable to calculate histogram with " + regions.length + " regions.");
            }
            return engine.getFrequency(query, Region.parseRegion(regions[0]), interval);
        });
    }

    public VariantIterable iterable(String sessionId) throws CatalogException, StorageEngineException {
        return (query, options) -> {
            try {
                return iterator(query, options, sessionId);
            } catch (CatalogException | StorageEngineException e) {
                throw new VariantQueryException("Error getting variant iterator", e);
            }
        };
    }

    public VariantDBIterator iterator(String sessionId) throws CatalogException, StorageEngineException {
        return iterator(null, null, sessionId);
    }

    public VariantDBIterator iterator(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException {
        String study = catalogUtils.getAnyStudy(query, sessionId);

        DataStore dataStore = getDataStore(study, sessionId);
        VariantStorageEngine storageEngine = getVariantStorageEngine(dataStore);
        catalogUtils.parseQuery(query, sessionId);
        checkSamplesPermissions(query, queryOptions, storageEngine.getMetadataManager(), sessionId);
        return storageEngine.iterator(query, queryOptions);
    }

//    public <T> VariantDBIterator<T> iterator(Query query, QueryOptions queryOptions, Class<T> clazz, String sessionId) {
//        return null;
//    }

    public VariantQueryResult<Variant> intersect(Query query, QueryOptions queryOptions, List<String> studyIds, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        Query intersectQuery = new Query(query);
        intersectQuery.put(VariantQueryParam.STUDY.key(), String.join(VariantQueryUtils.AND, studyIds));
        return get(intersectQuery, queryOptions, sessionId);
    }

    public DataResult<VariantSampleData> getSampleData(String variant, String study, QueryOptions inputOptions, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        Query query = new Query(VariantQueryParam.STUDY.key(), study);
        QueryOptions options = inputOptions == null ? new QueryOptions() : new QueryOptions(inputOptions);
        options.remove(QueryOptions.INCLUDE);
        options.remove(QueryOptions.EXCLUDE);
        options.remove(VariantField.SUMMARY);
        return secure(query, options, sessionId, Enums.Action.SAMPLE_DATA, engine -> {
            String studyFqn = query.getString(STUDY.key());
            options.putAll(query);
            return engine.getSampleData(variant, studyFqn, options);
        });
    }

    public SampleMetadata getSampleMetadata(String study, String sample, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        Query query = new Query(STUDY.key(), study)
                .append(SAMPLE.key(), sample)
                .append(INCLUDE_FILE.key(), VariantQueryUtils.NONE);
        return secure(query, new QueryOptions(), sessionId, engine -> {

            VariantStorageMetadataManager metadataManager = engine.getMetadataManager();
            int studyId = metadataManager.getStudyId(study);
            Integer sampleId = metadataManager.getSampleId(studyId, sample);
            if (sampleId == null) {
                // Sample does not exist in storage!
                throw VariantQueryException.sampleNotFound(sample, study);
            } else {
                return metadataManager.getSampleMetadata(studyId, sampleId);
            }
        });
    }

    public DataStore getDataStore(String study, String sessionId) throws CatalogException {
        return getDataStore(catalogManager, study, File.Bioformat.VARIANT, sessionId);
    }

    public DataStore getDataStoreByProjectId(String project, String sessionId) throws CatalogException {
        return getDataStoreByProjectId(catalogManager, project, File.Bioformat.VARIANT, sessionId);
    }

    protected VariantStorageEngine getVariantStorageEngine(Query query, String sessionId) throws CatalogException, StorageEngineException {
        String study = catalogUtils.getAnyStudy(query, sessionId);

        catalogUtils.parseQuery(query, sessionId);
        DataStore dataStore = getDataStore(study, sessionId);
        return getVariantStorageEngine(dataStore);
    }

    protected VariantStorageEngine getVariantStorageEngine(DataStore dataStore) throws StorageEngineException {
        return storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
    }

    public boolean isSolrAvailable() {
        SolrManager solrManager = null;
        try {
            solrManager = new SolrManager(
                    storageConfiguration.getSearch().getHosts(),
                    storageConfiguration.getSearch().getMode(),
                    storageConfiguration.getSearch().getTimeout());
            String collectionName = "test_connection";
            if (!solrManager.exists(collectionName)) {
                solrManager.create(collectionName, VariantSearchManager.CONF_SET);
            }
        } catch (Exception e) {
            logger.warn("Ignore exception checking if Solr is available", e);
            return false;
        } finally {
            if (solrManager != null) {
                try {
                    solrManager.close();
                } catch (IOException e) {
                    logger.warn("Ignore exception closing Solr", e);
                    return false;
                }
            }
        }
        return true;
    }

    public void checkQueryPermissions(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(query, sessionId);
        checkSamplesPermissions(query, queryOptions, variantStorageEngine.getMetadataManager(), sessionId);
    }

    public Set<String> getIndexedSamples(String study, String sessionId) throws CatalogException {
        return catalogManager
                .getCohortManager()
                .get(study, StudyEntry.DEFAULT_COHORT, new QueryOptions(), sessionId)
                .first()
                .getSamples()
                .stream()
                .map(Sample::getId)
                .collect(Collectors.toSet());
    }

    // Permission related methods

    private interface VariantReadOperation<R> {
        R apply(VariantStorageEngine engine) throws StorageEngineException;
    }

    private <R> R secure(Query query, QueryOptions queryOptions, String sessionId, VariantReadOperation<R> supplier)
            throws CatalogException, StorageEngineException {
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(query, sessionId);
        checkSamplesPermissions(query, queryOptions, variantStorageEngine.getMetadataManager(), sessionId);
        return supplier.apply(variantStorageEngine);
    }

    private <R> R secure(Query query, QueryOptions queryOptions, String sessionId, Enums.Action auditAction,
                         VariantReadOperation<R> supplier)
            throws CatalogException, StorageEngineException, IOException {
        ObjectMap auditAttributes = new ObjectMap()
                .append("query", new Query(query))
                .append("queryOptions", new QueryOptions(queryOptions));
        R result = null;
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        String dbName = null;
        Exception exception = null;
        StopWatch totalStopWatch = StopWatch.createStarted();
        StopWatch storageStopWatch = null;
        try {
            String study = catalogUtils.getAnyStudy(query, sessionId);

            StopWatch stopWatch = StopWatch.createStarted();
            catalogUtils.parseQuery(query, sessionId);
            auditAttributes.append("catalogParseQueryTimeMillis", stopWatch.getTime(TimeUnit.MILLISECONDS));
            DataStore dataStore = getDataStore(study, sessionId);
            dbName = dataStore.getDbName();
            VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);

            stopWatch.reset();
            checkSamplesPermissions(query, queryOptions, variantStorageEngine.getMetadataManager(), sessionId);
            auditAttributes.append("checkPermissionsTimeMillis", stopWatch.getTime(TimeUnit.MILLISECONDS));

            storageStopWatch = StopWatch.createStarted();
            result = supplier.apply(variantStorageEngine);
            return result;
        } catch (Exception e) {
            exception = e;
            throw e;
        } finally {
            auditAttributes.append("storageTimeMillis", storageStopWatch == null
                    ? -1
                    : storageStopWatch.getTime(TimeUnit.MILLISECONDS));
            if (result instanceof DataResult) {
                auditAttributes.append("dbTime", ((DataResult) result).getTime());
                auditAttributes.append("numResults", ((DataResult) result).getResults().size());
            }
            auditAttributes.append("totalTimeMillis", totalStopWatch.getTime(TimeUnit.MILLISECONDS));
            auditAttributes.append("error", result == null);
            if (exception != null) {
                auditAttributes.append("errorType", exception.getClass());
                auditAttributes.append("errorMessage", exception.getMessage());
            }
            logger.debug("catalogParseQueryTimeMillis = " + auditAttributes.getInt("catalogParseQueryTimeMillis"));
            logger.debug("checkPermissionsTimeMillis = " + auditAttributes.getInt("checkPermissionsTimeMillis"));
            logger.debug("storageTimeMillis = " + auditAttributes.getInt("storageTimeMillis"));
            logger.debug("dbTime = " + auditAttributes.getInt("dbTime"));
            logger.debug("totalTimeMillis = " + auditAttributes.getInt("totalTimeMillis"));
            catalogManager.getAuditManager().audit(userId, auditAction, Enums.Resource.VARIANT, "", "", "", "", new ObjectMap(),
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), auditAttributes);
        }
    }

    private Map<String, List<Sample>> checkSamplesPermissions(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        String study = catalogUtils.getAnyStudy(query, sessionId);
        DataStore dataStore = getDataStore(study, sessionId);
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
        return checkSamplesPermissions(query, queryOptions, variantStorageEngine.getMetadataManager(), sessionId);
    }

    // package protected for test visibility
    Map<String, List<Sample>> checkSamplesPermissions(Query query, QueryOptions queryOptions, VariantStorageMetadataManager mm,
                                                      String sessionId)
            throws CatalogException {
        final Map<String, List<Sample>> samplesMap = new HashMap<>();
        Set<VariantField> returnedFields = VariantField.getIncludeFields(queryOptions);
        if (!returnedFields.contains(VariantField.STUDIES)) {
            // FIXME: What if filtering by fields with no permissions?
            return Collections.emptyMap();
        }

        if (VariantQueryUtils.isIncludeSamplesDefined(query, returnedFields)) {
            Map<String, List<String>> samplesToReturn = VariantQueryUtils.getSamplesMetadata(query, queryOptions, mm);
            for (Map.Entry<String, List<String>> entry : samplesToReturn.entrySet()) {
                String studyId = entry.getKey();
                if (!entry.getValue().isEmpty()) {
                    DataResult<Sample> samplesQueryResult = catalogManager.getSampleManager().get(studyId, entry.getValue(),
                            new QueryOptions(INCLUDE, SampleDBAdaptor.QueryParams.ID.key()), sessionId);
                    if (samplesQueryResult.getNumResults() != entry.getValue().size()) {
                        throw new CatalogAuthorizationException("Permission denied. User "
                                + catalogManager.getUserManager().getUserId(sessionId) + " can't read all the requested samples");
                    }
                    samplesMap.put(studyId, samplesQueryResult.getResults());
                } else {
                    samplesMap.put(studyId, Collections.emptyList());
                }
            }
        } else {
            logger.debug("Missing include samples! Obtaining samples to include from catalog.");
            List<String> includeStudies = VariantQueryUtils.getIncludeStudies(query, queryOptions, mm)
                    .stream()
                    .map(mm::getStudyName)
                    .collect(Collectors.toList());
            List<Study> studies = catalogManager.getStudyManager().get(includeStudies,
                    new QueryOptions(INCLUDE, FQN.key()), false, sessionId).getResults();
            if (!returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA)) {
                for (String returnedStudy : includeStudies) {
                    samplesMap.put(returnedStudy, Collections.emptyList());
                }
            } else {
                List<String> includeSamples = new LinkedList<>();
                for (Study study : studies) {
                    DataResult<Sample> samplesQueryResult = catalogManager.getSampleManager().search(study.getFqn(), new Query(),
                            new QueryOptions(INCLUDE, SampleDBAdaptor.QueryParams.ID.key()).append("lazy", true), sessionId);
                    samplesQueryResult.getResults().sort(Comparator.comparing(Sample::getId));
                    samplesMap.put(study.getFqn(), samplesQueryResult.getResults());
                    int studyId = mm.getStudyId(study.getFqn());
                    for (Iterator<Sample> iterator = samplesQueryResult.getResults().iterator(); iterator.hasNext();) {
                        Sample sample = iterator.next();
                        if (mm.getSampleId(studyId, sample.getId(), true) != null) {
                            includeSamples.add(sample.getId());
                        } else {
                            iterator.remove();
                        }
                    }
                }
                if (includeSamples.isEmpty()) {
                    query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), NONE);
                } else {
                    query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSamples);
                }
            }
        }
        return samplesMap;
    }

    // Some aux methods

    private String[] getRegions(Query query) {
        String[] regions;
        String regionStr = query.getString(VariantQueryParam.REGION.key());
        if (!StringUtils.isEmpty(regionStr)) {
            regions = regionStr.split(",");
        } else {
            regions = new String[0];
        }
        return regions;
    }

    public static Query getVariantQuery(Map<String, ?> queryOptions) {
        Query query = new Query();

        for (VariantQueryParam queryParams : VariantQueryParam.values()) {
            if (queryOptions.containsKey(queryParams.key())) {
                query.put(queryParams.key(), queryOptions.get(queryParams.key()));
            }
        }
        if (queryOptions.containsKey(VariantCatalogQueryUtils.SAMPLE_ANNOTATION.key())) {
            query.put(VariantCatalogQueryUtils.SAMPLE_ANNOTATION.key(), queryOptions.get(VariantCatalogQueryUtils.SAMPLE_ANNOTATION.key()));
        }
        if (queryOptions.containsKey(VariantCatalogQueryUtils.PROJECT.key())) {
            query.put(VariantCatalogQueryUtils.PROJECT.key(), queryOptions.get(VariantCatalogQueryUtils.PROJECT.key()));
        }
        if (queryOptions.containsKey(VariantCatalogQueryUtils.FAMILY.key())) {
            query.put(VariantCatalogQueryUtils.FAMILY.key(), queryOptions.get(VariantCatalogQueryUtils.FAMILY.key()));
        }
        if (queryOptions.containsKey(VariantCatalogQueryUtils.FAMILY_DISORDER.key())) {
            query.put(VariantCatalogQueryUtils.FAMILY_DISORDER.key(), queryOptions.get(VariantCatalogQueryUtils.FAMILY_DISORDER.key()));
        }
        if (queryOptions.containsKey(VariantCatalogQueryUtils.FAMILY_PROBAND.key())) {
            query.put(VariantCatalogQueryUtils.FAMILY_PROBAND.key(), queryOptions.get(VariantCatalogQueryUtils.FAMILY_PROBAND.key()));
        }
        if (queryOptions.containsKey(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key())) {
            query.put(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key(),
                    queryOptions.get(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key()));
        }
        if (queryOptions.containsKey(VariantCatalogQueryUtils.FAMILY_MEMBERS.key())) {
            query.put(VariantCatalogQueryUtils.FAMILY_MEMBERS.key(),
                    queryOptions.get(VariantCatalogQueryUtils.FAMILY_MEMBERS.key()));
        }
        if (queryOptions.containsKey(VariantCatalogQueryUtils.PANEL.key())) {
            query.put(VariantCatalogQueryUtils.PANEL.key(), queryOptions.get(VariantCatalogQueryUtils.PANEL.key()));
        }

        return query;
    }

    @Override
    public void testConnection() throws StorageEngineException {

    }

    // ---------------------//
    //   Facet methods      //
    // ---------------------//

    public DataResult<FacetField> facet(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        return secure(query, queryOptions, sessionId, Enums.Action.FACET, engine -> {
            addDefaultLimit(queryOptions, engine.getOptions());
            logger.debug("getFacets {}, {}", query, queryOptions);
            DataResult<FacetField> result = engine.facet(query, queryOptions);
            logger.debug("getFacets in {}ms", result.getTime());
            return result;
        });
    }

    public List<BeaconResponse> beacon(String beaconsStr, BeaconResponse.Query beaconQuery, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        if (beaconsStr.startsWith("[")) {
            beaconsStr = beaconsStr.substring(1);
        }
        if (beaconsStr.endsWith("]")) {
            beaconsStr = beaconsStr.substring(0, beaconsStr.length() - 1);
        }

        List<String> beaconsList = Arrays.asList(beaconsStr.split(","));

        List<BeaconResponse.Beacon> beacons = new ArrayList<>(beaconsList.size());
        for (String studyStr : beaconsList) {
            beacons.add(new BeaconResponse.Beacon(studyStr, null, null, null));
        }

        List<BeaconResponse> responses = new ArrayList<>(beacons.size());

        for (BeaconResponse.Beacon beacon : beacons) {
            Query query = new Query();
            query.put(STUDY.key(), beacon.getId());
            int position1based = beaconQuery.getPosition() + 1;
            query.put(REGION.key(), new Region(beaconQuery.getChromosome(), position1based, position1based));
            query.putIfNotEmpty(REFERENCE.key(), beaconQuery.getReference());
            switch (beaconQuery.getAllele().toUpperCase()) {
                case "D":
                case "DEL":
                    query.put(TYPE.key(), VariantType.DELETION);
                    break;
                case "I":
                case "INS":
                    query.put(TYPE.key(), VariantType.INSERTION);
                    break;
                default:
                    query.put(ALTERNATE.key(), beaconQuery.getAllele());
                    break;
            }

            Long count = count(query, sessionId).first();
            if (count > 1) {
                throw new VariantQueryException("Unexpected beacon count for query " + query + ". Got " + count + " results!");
            }
            BeaconResponse beaconResponse = new BeaconResponse(beacon, beaconQuery, count == 1, null);

            responses.add(beaconResponse);
        }
        return responses;
    }

    public static DataStore getDataStore(CatalogManager catalogManager, String studyStr, File.Bioformat bioformat, String sessionId)
            throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyStr, new QueryOptions(), sessionId).first();
        return getDataStore(catalogManager, study, bioformat, sessionId);
    }

    private static DataStore getDataStore(CatalogManager catalogManager, Study study, File.Bioformat bioformat, String sessionId)
            throws CatalogException {
        DataStore dataStore;
        if (study.getDataStores() != null && study.getDataStores().containsKey(bioformat)) {
            dataStore = study.getDataStores().get(bioformat);
        } else {
            String projectId = catalogManager.getStudyManager().getProjectFqn(study.getFqn());
            dataStore = getDataStoreByProjectId(catalogManager, projectId, bioformat, sessionId);
        }
        return dataStore;
    }

    public static DataStore getDataStoreByProjectId(CatalogManager catalogManager, String projectStr, File.Bioformat bioformat,
                                                    String sessionId)
            throws CatalogException {
        DataStore dataStore;
        QueryOptions queryOptions = new QueryOptions(INCLUDE,
                Arrays.asList(ProjectDBAdaptor.QueryParams.ID.key(), ProjectDBAdaptor.QueryParams.DATASTORES.key()));
        Project project = catalogManager.getProjectManager().get(projectStr, queryOptions, sessionId).first();
        if (project.getDataStores() != null && project.getDataStores().containsKey(bioformat)) {
            dataStore = project.getDataStores().get(bioformat);
        } else { //get default datastore
            //Must use the UserByStudyId instead of the file owner.
            String userId = catalogManager.getProjectManager().getOwner(project.getUid());
            // Replace possible dots at the userId. Usually a special character in almost all databases. See #532
            userId = userId.replace('.', '_');

            String databasePrefix = catalogManager.getConfiguration().getDatabasePrefix();

            String dbName = buildDatabaseName(databasePrefix, userId, project.getId());
            dataStore = new DataStore(StorageEngineFactory.get().getDefaultStorageEngineId(), dbName);
        }
        return dataStore;
    }

    public static String buildDatabaseName(String databasePrefix, String userId, String alias) {
        String prefix;
        if (StringUtils.isNotEmpty(databasePrefix)) {
            prefix = databasePrefix;
            if (!prefix.endsWith("_")) {
                prefix += "_";
            }
        } else {
            prefix = "opencga_";
        }
        // Project alias contains the userId:
        // userId@projectAlias
        int idx = alias.indexOf('@');
        if (idx >= 0) {
            alias = alias.substring(idx + 1);
        }

        return prefix + userId + '_' + alias;
    }

}
