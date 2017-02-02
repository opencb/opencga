/*
 * Copyright 2015-2016 OpenCB
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

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converters.ga4gh.Ga4ghVariantConverter;
import org.opencb.biodata.tools.variant.converters.ga4gh.factories.AvroGa4GhVariantFactory;
import org.opencb.biodata.tools.variant.converters.ga4gh.factories.ProtoGa4GhVariantFactory;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.StorageManager;
import org.opencb.opencga.storage.core.manager.models.StudyInfo;
import org.opencb.opencga.storage.core.manager.variant.operations.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Function;

public class VariantStorageManager extends StorageManager {

    public static final int LIMIT_DEFAULT = 1000;
    public static final int LIMIT_MAX = 5000;

    public VariantStorageManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);
    }

    public void clearCache(String studyId, String type, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

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
        Query query = new Query(VariantQueryParams.RETURNED_STUDIES.key(), study)
                .append(VariantQueryParams.STUDIES.key(), study);
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
        Set<Long> studies = checkSamplesPermissions(query, queryOptions, sessionId).keySet();

        List<StudyInfo> studyInfos = new ArrayList<>(studies.size());
        for (Long study : studies) {
            studyInfos.add(getStudyInfo(String.valueOf(study), Collections.emptyList(), sessionId));
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

    public void deleteStudy(String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public void deleteFile(String fileId, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public List<File> annotate(String study, Query query, String outDir, ObjectMap config, String sessionId)
            throws StorageEngineException, URISyntaxException, CatalogException, IOException {
        return annotate(null, study, query, outDir, config, sessionId);
    }

    public List<File> annotate(String project, String studies, Query query, String outDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException, IOException, URISyntaxException {
        VariantAnnotationStorageOperation annotOperation = new VariantAnnotationStorageOperation(catalogManager, storageConfiguration);

        List<Long> studyIds;
        if (StringUtils.isNotEmpty(studies) || StringUtils.isEmpty(project)) {
            // Only get specific studies if project is missing, or if some study is given
            studyIds = catalogManager.getStudyIds(studies, sessionId);
        } else {
            studyIds = Collections.emptyList();
        }
        List<StudyInfo> studiesList = new ArrayList<>(studyIds.size());
        for (Long studyId : studyIds) {
            studiesList.add(getStudyInfo(studyId.toString(), Collections.emptyList(), sessionId));
        }
        return annotOperation.annotateVariants(project, studiesList, query, outDir, sessionId, config);
    }

    public void deleteAnnotation(String annotationId, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public void stats(String study, List<String> cohorts, String outDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageEngineException, IOException, URISyntaxException {
        VariantStatsStorageOperation statsOperation = new VariantStatsStorageOperation(catalogManager, storageConfiguration);

        long studyId = catalogManager.getStudyId(study, sessionId);
        statsOperation.calculateStats(studyId, cohorts, outDir, new QueryOptions(config), sessionId);
    }

    public void deleteStats(List<String> cohorts, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    // ---------------------//
    //   Query methods      //
    // ---------------------//

    public QueryResult<Variant> get(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        return secure(query, queryOptions, sessionId, dbAdaptor -> {
            addDefaultLimit(queryOptions);
            logger.debug("getVariants {}, {}", query, queryOptions);
            QueryResult<Variant> result = dbAdaptor.get(query, queryOptions);
            logger.debug("gotVariants {}, {}, in {}ms", result.getNumResults(), result.getNumTotalResults(), result.getDbTime());
            return result;
        });
    }

    @SuppressWarnings("unchecked")
    public <T> QueryResult<T> get(Query query, QueryOptions queryOptions, String sessionId, Class<T> clazz)
            throws CatalogException, IOException, StorageEngineException {
        QueryResult<Variant> result = get(query, queryOptions, sessionId);
        List<T> variants;
        if (clazz == Variant.class) {
            return (QueryResult<T>) result;
        } else if (clazz == org.ga4gh.models.Variant.class) {
            Ga4ghVariantConverter<org.ga4gh.models.Variant> converter = new Ga4ghVariantConverter<>(new AvroGa4GhVariantFactory());
            variants = (List<T>) converter.apply(result.getResult());
        } else if (clazz == ga4gh.Variants.Variant.class) {
            Ga4ghVariantConverter<ga4gh.Variants.Variant> converter = new Ga4ghVariantConverter<>(new ProtoGa4GhVariantFactory());
            variants = (List<T>) converter.apply(result.getResult());
        } else {
            throw new IllegalArgumentException("Unknown variant format " + clazz);
        }
        return new QueryResult<>(
                result.getId(),
                result.getDbTime(),
                result.getNumResults(),
                result.getNumTotalResults(),
                result.getWarningMsg(),
                result.getErrorMsg(), variants);

    }

    //TODO: GroupByFieldEnum
    public QueryResult groupBy(String field, Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        return (QueryResult) secure(query, queryOptions, sessionId, dbAdaptor -> dbAdaptor.groupBy(query, field, queryOptions));
    }

    public QueryResult rank(Query query, String field, int limit, boolean asc, String sessionId)
            throws StorageEngineException, CatalogException, IOException {
        getDefaultLimit(limit, 30, 10);
        return (QueryResult) secure(query, null, sessionId, dbAdaptor -> dbAdaptor.rank(query, field, limit, asc));
    }

    public QueryResult<Long> count(Query query, String sessionId) throws CatalogException, StorageEngineException, IOException {
        return secure(query, new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES), sessionId,
                dbAdaptor -> dbAdaptor.count(query));
    }

    public QueryResult distinct(Query query, String field, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        return (QueryResult) secure(query, new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES), sessionId,
                dbAdaptor -> dbAdaptor.distinct(query, field));
    }

    public void facet() {
        throw new UnsupportedOperationException();
    }

    public QueryResult<Variant> getPhased(Variant variant, String study, String sample, String sessionId, QueryOptions options)
            throws CatalogException, IOException, StorageEngineException {
        return secure(new Query(VariantQueryParams.STUDIES.key(), study), options, sessionId,
                dbAdaptor -> dbAdaptor.getPhased(variant.toString(), study, sample, options, 5000));
    }

    public QueryResult getFrequency(Query query, int interval, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        return (QueryResult) secure(query, null, sessionId, dbAdaptor -> {
            String[] regions = getRegions(query);
            if (regions.length != 1) {
                throw new IllegalArgumentException("Unable to calculate histogram with " + regions.length + " regions.");
            }
            return dbAdaptor.getFrequency(query, Region.parseRegion(regions[0]), interval);
        });
    }

    public VariantDBIterator iterator(String sessionId) throws CatalogException, StorageEngineException {
        return iterator(null, null, sessionId);
    }

    public VariantDBIterator iterator(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException {
        long studyId = catalogUtils.getAnyStudyId(query, sessionId);

        VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId);
        catalogUtils.parseQuery(query, sessionId);
        checkSamplesPermissions(query, queryOptions, dbAdaptor, sessionId);
        VariantDBIterator iterator = dbAdaptor.iterator(query, queryOptions);
        iterator.addCloseable(dbAdaptor);
        return iterator;
    }

//    public <T> VariantDBIterator<T> iterator(Query query, QueryOptions queryOptions, Class<T> clazz, String sessionId) {
//        return null;
//    }

    public QueryResult<Variant> intersect(Query query, QueryOptions queryOptions, List<String> studyIds, String sessionId)
            throws CatalogException, IOException, StorageEngineException {
        Query intersectQuery = new Query(query);
        intersectQuery.put(VariantQueryParams.STUDIES.key(), String.join(VariantDBAdaptorUtils.AND, studyIds));
        return get(intersectQuery, queryOptions, sessionId);
    }


    public Map<Long, List<Sample>> getSamplesMetadata(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        long studyId = catalogUtils.getAnyStudyId(query, sessionId);
        catalogUtils.parseQuery(query, sessionId);
        try (VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(studyId, sessionId)) {
            return checkSamplesPermissions(query, queryOptions, variantDBAdaptor, sessionId);
        }
    }

    protected VariantDBAdaptor getVariantDBAdaptor(long studyId, String sessionId) throws CatalogException, StorageEngineException {
        DataStore dataStore = StorageOperation.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);

        String storageEngine = dataStore.getStorageEngine();
        String dbName = dataStore.getDbName();
        try {
            return storageEngineFactory.getVariantStorageEngine(storageEngine).getDBAdaptor(dbName);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new StorageEngineException("Unable to get VariantDBAdaptor", e);
        }
    }

    // Permission related methods

    private <R> R secure(Query query, QueryOptions queryOptions, String sessionId, Function<VariantDBAdaptor, R> supplier)
            throws CatalogException, StorageEngineException, IOException {
        long studyId = catalogUtils.getAnyStudyId(query, sessionId);

        catalogUtils.parseQuery(query, sessionId);

        try (VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId)) {
            checkSamplesPermissions(query, queryOptions, dbAdaptor, sessionId);

            return supplier.apply(dbAdaptor);
        }
    }

    private Map<Long, List<Sample>> checkSamplesPermissions(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        long studyId = catalogUtils.getAnyStudyId(query, sessionId);
        try (VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId)) {
            return checkSamplesPermissions(query, queryOptions, dbAdaptor, sessionId);
        }
    }

    // package protected for test visibility
    Map<Long, List<Sample>> checkSamplesPermissions(Query query, QueryOptions queryOptions, VariantDBAdaptor dbAdaptor, String sessionId)
            throws CatalogException {
        final Map<Long, List<Sample>> samplesMap = new HashMap<>();
        Set<VariantField> returnedFields = VariantField.getReturnedFields(queryOptions);
        if (!returnedFields.contains(VariantField.STUDIES)) {
            return Collections.emptyMap();
        }
        if (VariantDBAdaptorUtils.isValidParam(query, VariantQueryParams.RETURNED_SAMPLES)) {
            Map<Integer, List<Integer>> samplesToReturn = dbAdaptor.getReturnedSamples(query, queryOptions);
            for (Map.Entry<Integer, List<Integer>> entry : samplesToReturn.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    QueryResult<Sample> samplesQueryResult = catalogManager.getAllSamples(entry.getKey(),
                            new Query(SampleDBAdaptor.QueryParams.ID.key(), entry.getValue()),
                            new QueryOptions("exclude", Arrays.asList("projects.studies.samples.annotationSets",
                                    "projects.studies.samples.attributes")),
                            sessionId);
                    if (samplesQueryResult.getNumResults() != entry.getValue().size()) {
                        throw new CatalogAuthorizationException("Permission denied. User " + catalogManager.getUserIdBySessionId(sessionId)
                                + " can't read all the requested samples");
                    }
                    samplesMap.put((long) entry.getKey(), samplesQueryResult.getResult());
                }
            }
        } else {
            logger.debug("Missing returned samples! Obtaining returned samples from catalog.");
            List<Integer> returnedStudies = dbAdaptor.getReturnedStudies(query, queryOptions);
            List<Study> studies = catalogManager.getAllStudies(new Query(StudyDBAdaptor.QueryParams.ID.key(), returnedStudies),
                    new QueryOptions("include", "projects.studies.id"), sessionId).getResult();
            if (!returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA)) {
                for (Integer returnedStudy : returnedStudies) {
                    samplesMap.put(returnedStudy.longValue(), Collections.emptyList());
                }
            } else {
                List<Long> returnedSamples = new LinkedList<>();
                for (Study study : studies) {
                    QueryResult<Sample> samplesQueryResult = catalogManager.getAllSamples(study.getId(),
                            new Query(),
                            new QueryOptions("exclude", Arrays.asList("projects.studies.samples.annotationSets",
                                    "projects.studies.samples.attributes")),
                            sessionId);
                    samplesQueryResult.getResult().sort((o1, o2) -> Long.compare(o1.getId(), o2.getId()));
                    samplesMap.put(study.getId(), samplesQueryResult.getResult());
                    samplesQueryResult.getResult().stream().map(Sample::getId).forEach(returnedSamples::add);
                }
                query.append(VariantQueryParams.RETURNED_SAMPLES.key(), returnedSamples);
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
        String regionStr = query.getString(VariantQueryParams.REGION.key());
        if (!StringUtils.isEmpty(regionStr)) {
            regions = regionStr.split(",");
        } else {
            regions = new String[0];
        }
        return regions;
    }

    public static <T extends ObjectMap> Query getVariantQuery(T queryOptions) {
        Query query = new Query();

        for (VariantQueryParams queryParams : VariantQueryParams.values()) {
            if (queryOptions.containsKey(queryParams.key())) {
                query.put(queryParams.key(), queryOptions.get(queryParams.key()));
            }
        }

        return query;
    }

    @Override
    public void testConnection() throws StorageEngineException {

    }
}
