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

package org.opencb.opencga.storage.core.manager.variant;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.tools.variant.converters.ga4gh.Ga4ghVariantConverter;
import org.opencb.biodata.tools.variant.converters.ga4gh.factories.AvroGa4GhVariantFactory;
import org.opencb.biodata.tools.variant.converters.ga4gh.factories.ProtoGa4GhVariantFactory;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.manager.StorageManager;
import org.opencb.opencga.storage.core.manager.models.StudyInfo;
import org.opencb.opencga.storage.core.manager.variant.metadata.CatalogVariantMetadataFactory;
import org.opencb.opencga.storage.core.manager.variant.operations.*;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.BeaconResponse;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.opencga.catalog.db.api.StudyDBAdaptor.QueryParams.FQN;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

public class VariantStorageManager extends StorageManager {

    public static final int LIMIT_DEFAULT = 1000;
    public static final int LIMIT_MAX = 5000;

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
     *
     * The input file should have, in the same directory, a metadata file, with the same name ended with
     * {@link org.opencb.opencga.storage.core.variant.io.VariantExporter#METADATA_FILE_EXTENSION}
     *
     *
     * @param inputUri      Variants input file in avro format.
     * @param study         Study where to load the variants
     * @param sessionId     User's session id
     * @throws CatalogException if there is any error with Catalog
     * @throws IOException      if there is any I/O error
     * @throws StorageEngineException  if there si any error loading the variants
     */
    public void importData(URI inputUri, String study, String sessionId)
            throws CatalogException, IOException, StorageEngineException {

        VariantExportStorageOperation op = new VariantExportStorageOperation(catalogManager, storageConfiguration);
        StudyInfo studyInfo = getStudyInfo(study, Collections.emptyList(), sessionId);
        op.importData(studyInfo, inputUri, sessionId);

    }

    /**
     * Exports the result of the given query and the associated metadata.
     * @param outputFile    Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat  Output format.
     * @param study         Study to export
     * @param sessionId     User's session id
     * @return              List of generated files
     * @throws CatalogException if there is any error with Catalog
     * @throws IOException  If there is any IO error
     * @throws StorageEngineException  If there is any error exporting variants
     */
    public List<URI> exportData(String outputFile, VariantOutputFormat outputFormat, String study, String sessionId)
            throws StorageEngineException, CatalogException, IOException {
        Query query = new Query(VariantQueryParam.INCLUDE_STUDY.key(), study)
                .append(VariantQueryParam.STUDY.key(), study);
        return exportData(outputFile, outputFormat, query, new QueryOptions(), sessionId);
    }

    /**
     * Exports the result of the given query and the associated metadata.
     * @param outputFile    Optional output file. If null or empty, will print into the Standard output. Won't export any metadata.
     * @param outputFormat  Variant Output format.
     * @param query         Query with the variants to export
     * @param queryOptions  Query options
     * @param sessionId     User's session id
     * @return              List of generated files
     * @throws CatalogException if there is any error with Catalog
     * @throws IOException  If there is any IO error
     * @throws StorageEngineException  If there is any error exporting variants
     */
    public List<URI> exportData(String outputFile, VariantOutputFormat outputFormat, Query query, QueryOptions queryOptions,
                                String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        if (query == null) {
            query = new Query();
        }
        VariantExportStorageOperation op = new VariantExportStorageOperation(catalogManager, storageConfiguration);

        catalogUtils.parseQuery(query, sessionId);
        Collection<String> studies = checkSamplesPermissions(query, queryOptions, sessionId).keySet();
        if (studies.isEmpty()) {
            studies = catalogUtils.getStudies(query, sessionId);
        }
        List<StudyInfo> studyInfos = new ArrayList<>(studies.size());
        for (String study : studies) {
            studyInfos.add(getStudyInfo(study, Collections.emptyList(), sessionId));
        }

        return op.exportData(studyInfos, query, outputFormat, outputFile, sessionId, queryOptions);
    }

    // --------------------------//
    //   Data Operation methods  //
    // --------------------------//

    public List<StoragePipelineResult> index(String study, String fileId, String outDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException, IOException, URISyntaxException {
        return index(study, Arrays.asList(fileId.split(",")), outDir, config, sessionId);
    }

    public List<StoragePipelineResult> index(String study, List<String> files, String outDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException, IOException, URISyntaxException {
        VariantFileIndexerStorageOperation indexOperation = new VariantFileIndexerStorageOperation(catalogManager, storageConfiguration);

        QueryOptions options = new QueryOptions(config);
        StudyInfo studyInfo = getStudyInfo(study, files, sessionId);
        return indexOperation.index(studyInfo, outDir, options, sessionId);
    }


    public void searchIndex(String study, String sessionId) throws StorageEngineException, IOException, VariantSearchException,
            IllegalAccessException, ClassNotFoundException, InstantiationException, CatalogException {
        searchIndex(new Query(STUDY.key(), study), new QueryOptions(), sessionId);
    }

    public void searchIndex(Query query, QueryOptions queryOptions, String sessionId) throws StorageEngineException,
            IOException, VariantSearchException, IllegalAccessException, InstantiationException, ClassNotFoundException, CatalogException {
//        String userId = catalogManager.getUserManager().getUserId(sessionId);
//        long studyId = catalogManager.getStudyManager().getId(userId, study);
        String study = catalogUtils.getAnyStudy(query, sessionId);
        DataStore dataStore = getDataStore(study, sessionId);
        VariantStorageEngine variantStorageEngine =
                storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
        catalogUtils.parseQuery(query, sessionId);
        variantStorageEngine.searchIndex(query, queryOptions);
    }

    public void removeStudy(String study, String sessionId, QueryOptions options)
            throws CatalogException, IOException, StorageEngineException {
        VariantRemoveStorageOperation removeOperation = new VariantRemoveStorageOperation(catalogManager, storageEngineFactory);

        StudyInfo studyInfo = getStudyInfo(study, Collections.emptyList(), sessionId);
        removeOperation.removeStudy(studyInfo, options, sessionId);
    }

    public List<File> removeFile(List<String> files, String study, String sessionId, QueryOptions options)
            throws CatalogException, IOException, StorageEngineException {
        VariantRemoveStorageOperation removeOperation = new VariantRemoveStorageOperation(catalogManager, storageEngineFactory);

        StudyInfo studyInfo = getStudyInfo(study, files, sessionId);
        return removeOperation.removeFiles(studyInfo, options, sessionId);
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

    public void deleteAnnotation(String annotationId, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public void createAnnotationSnapshot(String project, String annotationName, ObjectMap params, String sessionId)
            throws StorageEngineException, VariantAnnotatorException, CatalogException, IllegalAccessException, InstantiationException,
            ClassNotFoundException {

        DataStore dataStore = getDataStoreByProjectId(project, sessionId);
        VariantStorageEngine variantStorageEngine =
                storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());

        StorageOperation.updateProjectMetadata(catalogManager, variantStorageEngine.getStudyConfigurationManager(), project, sessionId);

        variantStorageEngine.createAnnotationSnapshot(annotationName, params);
    }

    public void deleteAnnotationSnapshot(String project, String annotationName, ObjectMap params, String sessionId)
            throws StorageEngineException, VariantAnnotatorException, CatalogException, IllegalAccessException,
            InstantiationException, ClassNotFoundException {

        DataStore dataStore = getDataStoreByProjectId(project, sessionId);
        VariantStorageEngine variantStorageEngine =
                storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());

        StorageOperation.updateProjectMetadata(catalogManager, variantStorageEngine.getStudyConfigurationManager(), project, sessionId);

        variantStorageEngine.deleteAnnotationSnapshot(annotationName, params);
    }

    public QueryResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options, String sessionId)
            throws StorageEngineException, CatalogException, IOException {
        QueryOptions finalOptions = VariantQueryUtils.validateAnnotationQueryOptions(options);
        return secure(query, finalOptions, sessionId, (engine) -> engine.getAnnotation(name, query, finalOptions));
    }

    public void stats(String study, List<String> cohorts, String outDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException, IOException, URISyntaxException {
        VariantStatsStorageOperation statsOperation = new VariantStatsStorageOperation(catalogManager, storageConfiguration);

        statsOperation.calculateStats(study, cohorts, outDir, new QueryOptions(config), sessionId);
    }

    public void deleteStats(List<String> cohorts, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public void fillGaps(String studyStr, List<String> samples, ObjectMap config, String sessionId)
            throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException, StorageEngineException {

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        DataStore dataStore = getDataStore(study.getFqn(), sessionId);
        VariantStorageEngine variantStorageEngine =
                storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());

        if (samples == null || samples.size() < 2) {
            throw new IllegalArgumentException("Fill gaps operation requires at least two samples!");
        }
        variantStorageEngine.fillGaps(study.getFqn(), samples, config);
    }

    public void fillMissing(String studyStr, boolean overwrite, ObjectMap config, String sessionId)
            throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException, StorageEngineException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        DataStore dataStore = getDataStore(study.getFqn(), sessionId);
        VariantStorageEngine variantStorageEngine =
                storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());

        variantStorageEngine.fillMissing(study.getFqn(), config, overwrite);
    }

    // ---------------------//
    //   Query methods      //
    // ---------------------//

    public VariantQueryResult<Variant> get(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        return secure(query, queryOptions, sessionId, engine -> {
            addDefaultLimit(queryOptions);
            logger.debug("getVariants {}, {}", query, queryOptions);
            VariantQueryResult<Variant> result = engine.get(query, queryOptions);
            logger.debug("gotVariants {}, {}, in {}ms", result.getNumResults(), result.getNumTotalResults(), result.getDbTime());
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
            variants = (List<T>) converter.apply(result.getResult());
        } else if (clazz == ga4gh.Variants.Variant.class) {
            Ga4ghVariantConverter<ga4gh.Variants.Variant> converter = new Ga4ghVariantConverter<>(new ProtoGa4GhVariantFactory());
            variants = (List<T>) converter.apply(result.getResult());
        } else {
            throw new IllegalArgumentException("Unknown variant format " + clazz);
        }
        return new VariantQueryResult<>(
                result.getId(),
                result.getDbTime(),
                result.getNumResults(),
                result.getNumTotalResults(),
                result.getWarningMsg(),
                result.getErrorMsg(),
                variants,
                result.getSamples(),
                result.getSource(),
                result.getApproximateCount(),
                result.getApproximateCountSamplingSize());

    }

    public QueryResult<VariantMetadata> getMetadata(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        return secure(query, queryOptions, sessionId, engine -> {
            StopWatch watch = StopWatch.createStarted();
            VariantMetadataFactory metadataFactory = new CatalogVariantMetadataFactory(catalogManager, engine.getDBAdaptor(), sessionId);
            VariantMetadata metadata = metadataFactory.makeVariantMetadata(query, queryOptions);
            return new QueryResult<>("getMetadata", ((int) watch.getTime()), 1, 1, "", "", Collections.singletonList(metadata));
        });
    }

    //TODO: GroupByFieldEnum
    public QueryResult groupBy(String field, Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        return (QueryResult) secure(query, queryOptions, sessionId, engine -> engine.groupBy(query, field, queryOptions));
    }

    public QueryResult rank(Query query, String field, int limit, boolean asc, String sessionId)
            throws StorageEngineException, CatalogException, IOException {
        getDefaultLimit(limit, 30, 10);
        return (QueryResult) secure(query, null, sessionId, engine -> engine.rank(query, field, limit, asc));
    }

    public QueryResult<Long> count(Query query, String sessionId) throws CatalogException, StorageEngineException, IOException {
        return secure(query, new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES), sessionId,
                engine -> engine.count(query));
    }

    public QueryResult distinct(Query query, String field, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        return (QueryResult) secure(query, new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES), sessionId,
                engine -> engine.distinct(query, field));
    }

    public void facet() {
        throw new UnsupportedOperationException();
    }

    public VariantQueryResult<Variant> getPhased(Variant variant, String study, String sample, String sessionId, QueryOptions options)
            throws CatalogException, IOException, StorageEngineException {
        return secure(new Query(VariantQueryParam.STUDY.key(), study), options, sessionId,
                engine -> engine.getPhased(variant.toString(), study, sample, options, 5000));
    }

    public QueryResult getFrequency(Query query, int interval, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        return (QueryResult) secure(query, null, sessionId, engine -> {
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
        checkSamplesPermissions(query, queryOptions, storageEngine.getStudyConfigurationManager(), sessionId);
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

    private DataStore getDataStore(String study, String sessionId) throws CatalogException {
        return StorageOperation.getDataStore(catalogManager, study, File.Bioformat.VARIANT, sessionId);
    }

    private DataStore getDataStoreByProjectId(String project, String sessionId) throws CatalogException {
        return StorageOperation.getDataStoreByProjectId(catalogManager, project, File.Bioformat.VARIANT, sessionId);
    }

    protected VariantStorageEngine getVariantStorageEngine(DataStore dataStore) throws CatalogException, StorageEngineException {
        try {
            return storageEngineFactory.getVariantStorageEngine(dataStore.getStorageEngine(), dataStore.getDbName());
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new StorageEngineException("Unable to get VariantDBAdaptor", e);
        }
    }

    // Permission related methods

    private interface VariantReadOperation<R> {
        R apply(VariantStorageEngine engine) throws StorageEngineException;
    }

    private <R> R secure(Query query, QueryOptions queryOptions, String sessionId, VariantReadOperation<R> supplier)
            throws CatalogException, StorageEngineException, IOException {
        String study = catalogUtils.getAnyStudy(query, sessionId);

        catalogUtils.parseQuery(query, sessionId);
        DataStore dataStore = getDataStore(study, sessionId);
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);

        checkSamplesPermissions(query, queryOptions, variantStorageEngine.getStudyConfigurationManager(), sessionId);
        return supplier.apply(variantStorageEngine);
    }
    private <R> R secure(Query facetedQuery, Query query, QueryOptions queryOptions,
                         String sessionId, VariantReadOperation<R> supplier)
            throws CatalogException, StorageEngineException, IOException {
        return secure(query, queryOptions, sessionId, supplier);
    }

    private Map<String, List<Sample>> checkSamplesPermissions(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        String study = catalogUtils.getAnyStudy(query, sessionId);
        DataStore dataStore = getDataStore(study, sessionId);
        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);
        return checkSamplesPermissions(query, queryOptions, variantStorageEngine.getStudyConfigurationManager(), sessionId);
    }

    // package protected for test visibility
    Map<String, List<Sample>> checkSamplesPermissions(Query query, QueryOptions queryOptions, StudyConfigurationManager scm,
                                                      String sessionId)
            throws CatalogException {
        final Map<String, List<Sample>> samplesMap = new HashMap<>();
        Set<VariantField> returnedFields = VariantField.getIncludeFields(queryOptions);
        if (!returnedFields.contains(VariantField.STUDIES)) {
            // FIXME: What if filtering by fields with no permissions?
            return Collections.emptyMap();
        }

        BiMap<Integer, String> studyIdMap = HashBiMap.create(scm.getStudies(null)).inverse();
        if (VariantQueryUtils.isIncludeSamplesDefined(query, returnedFields)) {
            Map<Integer, List<Integer>> samplesToReturn = VariantQueryUtils.getIncludeSamples(query, queryOptions, scm);
            for (Map.Entry<Integer, List<Integer>> entry : samplesToReturn.entrySet()) {
                int studyId = entry.getKey();
                String study = studyIdMap.get(studyId);
                if (!entry.getValue().isEmpty()) {
                    QueryResult<Sample> samplesQueryResult = catalogManager.getSampleManager().get(study,
                            new Query(SampleDBAdaptor.QueryParams.UID.key(), entry.getValue()), new QueryOptions("exclude",
                                    Arrays.asList("projects.studies.samples.annotationSets", "projects.studies.samples.attributes")),
                            sessionId);
                    if (samplesQueryResult.getNumResults() != entry.getValue().size()) {
                        throw new CatalogAuthorizationException("Permission denied. User "
                                + catalogManager.getUserManager().getUserId(sessionId) + " can't read all the requested samples");
                    }
                    samplesMap.put(study, samplesQueryResult.getResult());
                } else {
                    samplesMap.put(study, Collections.emptyList());
                }
            }
        } else {
            logger.debug("Missing returned samples! Obtaining returned samples from catalog.");
            List<Integer> returnedStudies = VariantQueryUtils.getIncludeStudies(query, queryOptions, scm);
            List<Study> studies = catalogManager.getStudyManager().get(new Query(StudyDBAdaptor.QueryParams.UID.key(), returnedStudies),
                    new QueryOptions(INCLUDE, FQN.key()), sessionId).getResult();
            if (!returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA)) {
                for (Integer returnedStudy : returnedStudies) {
                    samplesMap.put(studyIdMap.get(returnedStudy), Collections.emptyList());
                }
            } else {
                List<Long> returnedSamples = new LinkedList<>();
                for (Study study : studies) {
                    QueryResult<Sample> samplesQueryResult = catalogManager.getSampleManager().get(study.getFqn(),
                            new Query(), new QueryOptions("exclude",
                                    Arrays.asList("projects.studies.samples.annotationSets", "projects.studies.samples.attributes")),
                            sessionId);
                    samplesQueryResult.getResult().sort(Comparator.comparingLong(PrivateFields::getUid));
                    samplesMap.put(study.getFqn(), samplesQueryResult.getResult());
                    samplesQueryResult.getResult().stream().map(Sample::getUid).forEach(returnedSamples::add);
                }
                query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), returnedSamples);
            }
        }
        return samplesMap;
    }

    // Some aux methods

    private int addDefaultLimit(QueryOptions queryOptions) {
        return addDefaultLimit(queryOptions, LIMIT_MAX, LIMIT_DEFAULT);
    }

    private int addDefaultLimit(QueryOptions queryOptions, int limitMax, int limitDefault) {
        // Add default limit
        int limit = getDefaultLimit(queryOptions.getInt(QueryOptions.LIMIT, -1), limitMax, limitDefault);
        queryOptions.put(QueryOptions.LIMIT,  limit);
        return limit;
    }

    private int getDefaultLimit(int limit, int limitMax, int limitDefault) {
        if (limit > limitMax) {
            logger.info("Unable to return more than {} variants. Change limit from {} to {}", limitMax, limit, limitMax);
        }
        limit = (limit > 0) ? Math.min(limit, limitMax) : limitDefault;
        return limit;
    }

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

        return query;
    }

    @Override
    public void testConnection() throws StorageEngineException {

    }

    // ---------------------//
    //   Facet methods      //
    // ---------------------//

    public FacetedQueryResult facet(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        return secure(query, queryOptions, sessionId, dbAdaptor -> {
            addDefaultLimit(queryOptions);
            logger.debug("getFacets {}, {}", query, queryOptions);
            FacetedQueryResult result = dbAdaptor.facet(query, queryOptions);
            logger.debug("getFacets in {}ms", result.getDbTime());
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
}
