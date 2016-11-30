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

package org.opencb.opencga.storage.core.local.variant;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.ga4gh.AvroGa4GhVariantFactory;
import org.opencb.biodata.tools.ga4gh.ProtoGa4GhVariantFactory;
import org.opencb.biodata.tools.variant.converter.ga4gh.Ga4ghVariantConverter;
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
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.local.StorageManager;
import org.opencb.opencga.storage.core.local.models.StudyInfo;
import org.opencb.opencga.storage.core.local.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.local.variant.operations.VariantAnnotationStorageOperation;
import org.opencb.opencga.storage.core.local.variant.operations.VariantFileIndexerStorageOperation;
import org.opencb.opencga.storage.core.local.variant.operations.VariantStatsStorageOperation;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Function;

public class VariantStorageManager extends StorageManager {

    public static final int LIMIT_DEFAULT = 1000;
    public static final int LIMIT_MAX = 5000;


    public VariantStorageManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, storageConfiguration);

    }

    public void clearCache(String studyId, String type, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

    }

    // -------------------------//
    //   Import/Export methods  //
    // -------------------------//

    public void importData(String fileId, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public void exportData(String outputFile, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public void exportData(String outputFile, String studyId, Query query, QueryOptions queryOptions, String sessionId) {
        throw new UnsupportedOperationException();
    }

    // --------------------------//
    //   Data Operation methods  //
    // --------------------------//

    public List<StorageETLResult> index(String fileId, String outDir, String catalogOutDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageManagerException, IOException, URISyntaxException {
        return index(Arrays.asList(fileId.split(",")), outDir, catalogOutDir, config, sessionId);
    }

    public List<StorageETLResult> index(List<String> files, String outDir, String catalogOutDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageManagerException, IOException, URISyntaxException {
        VariantFileIndexerStorageOperation indexOperation = new VariantFileIndexerStorageOperation(catalogManager, storageConfiguration);

        QueryOptions options = new QueryOptions(config);
        options.putIfNotNull(VariantFileIndexerStorageOperation.CATALOG_PATH, catalogOutDir);
        StudyInfo studyInfo = getStudyInfo(null, files, sessionId);
        return indexOperation.index(studyInfo, outDir, options, sessionId);
    }

    public void deleteStudy(String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public void deleteFile(String fileId, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public void annotate(String study, Query query, String outDir, String catalogOutDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageManagerException, IOException, URISyntaxException {
        VariantAnnotationStorageOperation annotOperation = new VariantAnnotationStorageOperation(catalogManager, storageConfiguration);

        long studyId = catalogManager.getStudyId(study, sessionId);
        annotOperation.annotateVariants(studyId, query, outDir, catalogOutDir, sessionId, config);
    }

    public void deleteAnnotation(String annotationId, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    public void stats(String study, List<String> cohorts, String outDir, String catalogOutDir, ObjectMap config, String sessionId)
            throws CatalogException, StorageManagerException, IOException, URISyntaxException {
        VariantStatsStorageOperation statsOperation = new VariantStatsStorageOperation(catalogManager, storageConfiguration);

        List<Long> cohortIds = new ArrayList<>(cohorts.size());
        for (String cohort : cohorts) {
            if (!cohort.contains(":")) {
                cohort = study + ":" + cohort;
            }
            long cohortId = catalogManager.getCohortId(cohort, sessionId);
            cohortIds.add(cohortId);
        }
        long studyId = catalogManager.getStudyId(study, sessionId);
        statsOperation.calculateStats(studyId, cohortIds, outDir, catalogOutDir, new QueryOptions(config), sessionId);
    }

    public void deleteStats(List<String> cohorts, String studyId, String sessionId) {
        throw new UnsupportedOperationException();
    }

    // ---------------------//
    //   Query methods      //
    // ---------------------//

    public QueryResult<Variant> get(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageManagerException, IOException {
        return secure(query, sessionId, dbAdaptor -> {
            addDefaultLimit(queryOptions);
            logger.debug("getVariants {}, {}", query, queryOptions);
            QueryResult<Variant> result = dbAdaptor.get(query, queryOptions);
            logger.debug("gotVariants {}, {}, in {}ms", result.getNumResults(), result.getNumTotalResults(), result.getDbTime());
            return result;
        });
    }

    @SuppressWarnings("unchecked")
    public <T> QueryResult<T> get(Query query, QueryOptions queryOptions, String sessionId, Class<T> clazz)
            throws CatalogException, IOException, StorageManagerException {
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
            throws CatalogException, StorageManagerException, IOException {
        return (QueryResult) secure(query, sessionId, dbAdaptor -> dbAdaptor.groupBy(query, field, queryOptions));
    }

    public QueryResult rank(Query query, String field, int limit, boolean asc, String sessionId)
            throws StorageManagerException, CatalogException, IOException {
        getDefaultLimit(limit, 30, 10);
        return (QueryResult) secure(query, sessionId, dbAdaptor -> dbAdaptor.rank(query, field, limit, asc));
    }

    public QueryResult<Long> count(Query query, String sessionId) throws CatalogException, StorageManagerException, IOException {
        return secure(query, sessionId, dbAdaptor -> dbAdaptor.count(query));
    }

    public QueryResult distinct(Query query, String field, String sessionId)
            throws CatalogException, IOException, StorageManagerException {
        return (QueryResult) secure(query, sessionId, dbAdaptor -> dbAdaptor.distinct(query, field));
    }

    public void facet() {
        throw new UnsupportedOperationException();
    }

    public QueryResult<Variant> getPhased(Variant variant, String study, String sample, String sessionId, QueryOptions options)
            throws CatalogException, IOException, StorageManagerException {
        return secure(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), study), sessionId,
                dbAdaptor -> dbAdaptor.getPhased(variant.toString(), study, sample, options, 5000));
    }

    public QueryResult getFrequency(Query query, int interval, String sessionId)
            throws CatalogException, IOException, StorageManagerException {
        return (QueryResult) secure(query, sessionId, dbAdaptor -> {
            String[] regions = getRegions(query);
            if (regions.length != 1) {
                throw new IllegalArgumentException("Unable to calculate histogram with " + regions.length + " regions.");
            }
            return dbAdaptor.getFrequency(query, Region.parseRegion(regions[0]), interval);
        });
    }

    public VariantDBIterator iterator(String sessionId) throws CatalogException, StorageManagerException {
        return iterator(null, null, sessionId);
    }

    public VariantDBIterator iterator(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageManagerException {
        long mainStudyId = getMainStudyId(query, sessionId);

        VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(mainStudyId, sessionId);
        checkSamplesPermissions(query, queryOptions, dbAdaptor, sessionId);
        VariantDBIterator iterator = dbAdaptor.iterator(query, queryOptions);
        iterator.addCloseable(dbAdaptor);
        return iterator;
    }

//    public <T> VariantDBIterator<T> iterator(Query query, QueryOptions queryOptions, Class<T> clazz, String sessionId) {
//        return null;
//    }

    public QueryResult<Variant> intersect(Query query, QueryOptions queryOptions, List<String> studyIds, String sessionId)
            throws CatalogException, IOException, StorageManagerException {
        Query intersectQuery = new Query(query);
        intersectQuery.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), String.join(VariantDBAdaptorUtils.AND, studyIds));
        return get(intersectQuery, queryOptions, sessionId);
    }


    public Map<Long, List<Sample>> getSamplesMetadata(Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageManagerException, IOException {
        long mainStudyId = getMainStudyId(query, sessionId);
        try (VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(mainStudyId, sessionId)) {
            return checkSamplesPermissions(query, queryOptions, variantDBAdaptor, sessionId);
        }
    }

    protected VariantDBAdaptor getVariantDBAdaptor(long studyId, String sessionId) throws CatalogException, StorageManagerException {
        DataStore dataStore = StorageOperation.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);

        String storageEngine = dataStore.getStorageEngine();
        String dbName = dataStore.getDbName();
        try {
            return storageManagerFactory.getVariantStorageManager(storageEngine).getDBAdaptor(dbName);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new StorageManagerException("Unable to get VariantDBAdaptor", e);
        }
    }

    // Permission related methods

    private <R> QueryResult<R> secure(Query query, String sessionId, Function<VariantDBAdaptor, QueryResult<R>> supplier)
            throws CatalogException, StorageManagerException, IOException {
        long studyId = getMainStudyId(query, sessionId);

        try (VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId)) {
            checkSamplesPermissions(query, null, dbAdaptor, sessionId);
            return getCacheOrDB(query, new QueryOptions(), studyId, sessionId, dbAdaptor, supplier);
        }
    }

    private <R> QueryResult<R> getCacheOrDB(Query query, QueryOptions options, Long studyId, String sessionId,
             VariantDBAdaptor dbAdaptor, Function<VariantDBAdaptor, QueryResult<R>> supplier) throws CatalogException {

        QueryResult<R> queryResult;

        if (isCacheEnabled(options)) {
            String redisKey = cacheManager.createKey(studyId, catalogManager.getUserIdBySessionId(sessionId), "var", query, options);
            queryResult = cacheManager.get(redisKey);
            if (queryResult.getResult() != null && queryResult.getResult().size() != 0) {
                return queryResult;
            } else {
                queryResult = supplier.apply(dbAdaptor);
                cacheManager.set(redisKey, query, (QueryResult) queryResult);
                return queryResult;
            }
        }
        return supplier.apply(dbAdaptor);
    }

    private boolean isCacheEnabled(QueryOptions options) {
        return options.getBoolean("cache") && cacheManager.isTypeAllowed("var");
    }

    private Map<Long, List<Sample>> checkSamplesPermissions(Query query, QueryOptions queryOptions, VariantDBAdaptor dbAdaptor,
                                                            String sessionId)
            throws CatalogException {
        final Map<Long, List<Sample>> samplesMap;
        if (query.containsKey(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key())) {
            Map<Integer, List<Integer>> samplesToReturn = dbAdaptor.getReturnedSamples(query, queryOptions);
            samplesMap = new HashMap<>();
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
            samplesMap = new HashMap<>();
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
            query.append(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), returnedSamples);
        }
        return samplesMap;
    }

    public long getMainStudyId(Query query, String sessionId) throws CatalogException {
        Long id = getMainStudyId(query, VariantDBAdaptor.VariantQueryParams.STUDIES.key(), sessionId);
        if (id == null) {
            id = getMainStudyId(query, VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(), sessionId);
        }
        if (id != null) {
            return id;
        } else {
            // TODO: Search in Catalog
            throw new IllegalArgumentException("Missing StudyId. Unable to get any variant!");
        }
    }

    private Long getMainStudyId(Query query, String key, String sessionId) throws CatalogException {
        if (query != null && query.containsKey(key)) {
            for (String id : query.getAsStringList(key)) {
                if (!id.startsWith("!")) {
                    long studyId = catalogManager.getStudyId(id, sessionId);
                    return studyId > 0 ? studyId : null;
                }
            }
        }
        return null;
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
        String regionStr = query.getString(VariantDBAdaptor.VariantQueryParams.REGION.key());
        if (!StringUtils.isEmpty(regionStr)) {
            regions = regionStr.split(",");
        } else {
            regions = new String[0];
        }
        return regions;
    }

    public static Query getVariantQuery(QueryOptions queryOptions) {
        Query query = new Query();

        for (VariantDBAdaptor.VariantQueryParams queryParams : VariantDBAdaptor.VariantQueryParams.values()) {
            if (queryOptions.containsKey(queryParams.key())) {
                query.put(queryParams.key(), queryOptions.get(queryParams.key()));
            }
        }

        return query;
    }

    @Override
    public void testConnection() throws StorageManagerException {

    }
}
