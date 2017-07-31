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

package org.opencb.opencga.analysis.storage.variant;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converters.ga4gh.Ga4ghVariantConverter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 *
 * Created on 18/08/15.
 */
@Deprecated
public class VariantFetcher implements AutoCloseable {

    public static final String SAMPLES_METADATA = "samplesMetadata";
    private final CatalogManager catalogManager;
    private final StorageEngineFactory storageEngineFactory;
    private final Logger logger;

    public static final int LIMIT_DEFAULT = 1000;
    public static final int LIMIT_MAX = 5000;
    private final ConcurrentHashMap<String, VariantDBAdaptor> variantDBAdaptor = new ConcurrentHashMap<>();

    public VariantFetcher(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        this.catalogManager = catalogManager;
        this.storageEngineFactory = storageEngineFactory;
        logger = LoggerFactory.getLogger(VariantFetcher.class);
    }

    public QueryResult rank(Query query, QueryOptions queryOptions, String rank, String sessionId)
            throws Exception {
        return getVariantsPerStudy(getMainStudyId(query, sessionId), query, queryOptions, false, null, rank, 0, null, sessionId);
    }

    public QueryResult groupBy(Query query, QueryOptions queryOptions, String groupBy, String sessionId)
            throws Exception {
        return getVariantsPerStudy(getMainStudyId(query, sessionId), query, queryOptions, false, groupBy, null, 0, null, sessionId);
    }

    public QueryResult<Variant> get(Query query, QueryOptions queryOptions, String sessionId)
            throws Exception {
        queryOptions.remove("model");
        return getVariantsPerStudy(getMainStudyId(query, sessionId), query, queryOptions, false, null, null, 0, null, sessionId);
    }

    public QueryResult<org.ga4gh.models.Variant> getGa4gh(Query query, QueryOptions queryOptions, String sessionId)
            throws Exception {
        queryOptions.put("model", "ga4gh");
        return getVariantsPerStudy(getMainStudyId(query, sessionId), query, queryOptions, false, null, null, 0, null, sessionId);
    }

    public Map<Long, List<Sample>> getSamplesMetadata(long studyId, Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(studyId, sessionId);
        return checkSamplesPermissions(query, queryOptions, variantDBAdaptor, sessionId);
    }

    public StudyConfiguration getStudyConfiguration(long studyId, QueryOptions options, String sessionId)
            throws CatalogException, StorageEngineException, IOException {
        VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(studyId, sessionId); // DB con closed by VariantFetcher
        return variantDBAdaptor.getStudyConfigurationManager().getStudyConfiguration((int) studyId, options).first();
    }

    public QueryResult getVariantsPerFile(String region, boolean histogram, String groupBy, int interval, String fileId, String sessionId, QueryOptions queryOptions)
            throws Exception {
        QueryResult result;
        long fileIdNum;

        fileIdNum = catalogManager.getFileId(fileId, null, sessionId);
        File file = catalogManager.getFile(fileIdNum, sessionId).first();

        if (file.getIndex() == null || !file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
            throw new Exception("File {id:" + file.getId() + " name:'" + file.getName() + "'} " +
                    " is not an indexed file.");
        }
        if (!file.getBioformat().equals(File.Bioformat.VARIANT)) {
            throw new Exception("File {id:" + file.getId() + " name:'" + file.getName() + "'} " +
                    " is not a Variant file.");
        }

        long studyId = catalogManager.getStudyIdByFileId(file.getId());
        result = getVariantsPerStudy(studyId, region, histogram, groupBy, interval, fileIdNum, sessionId, queryOptions);
        return result;
    }

    public QueryResult getVariantsPerStudy(long studyId, String region, boolean histogram, String groupBy, int interval, String sessionId, QueryOptions queryOptions) throws Exception {
        return getVariantsPerStudy(studyId, region, histogram, groupBy, interval, null, sessionId, queryOptions);
    }

    public QueryResult getVariantsPerStudy(long studyId, String regionStr, boolean histogram, String groupBy, int interval, Long fileIdNum, String sessionId, QueryOptions queryOptions)
            throws Exception {
        queryOptions.add(VariantQueryParam.REGION.key(), regionStr);
        return getVariantsPerStudy(studyId, getVariantQuery(queryOptions), queryOptions, histogram, groupBy, null, interval, fileIdNum, sessionId);
    }

    public QueryResult getVariantsPerStudy(long studyId, Query query, QueryOptions queryOptions,
                                           boolean histogram, String groupBy, String rank, int interval, Long fileIdNum, String sessionId)
            throws Exception {
        QueryResult result;
        logger.debug("queryVariants = {}", query.toJson());

        //TODO: Check files and studies exists

        if (fileIdNum != null) {
            query.put(VariantQueryParam.FILES.key(), fileIdNum);
        }
        if (!query.containsKey(VariantQueryParam.STUDIES.key())) {
            query.put(VariantQueryParam.STUDIES.key(), studyId);
        }

        // TODO: Check returned files
        try (VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId)) {

            final Map<Long, List<Sample>> samplesMap = checkSamplesPermissions(query, queryOptions, dbAdaptor, sessionId);

            String[] regions = getRegions(query);

            if (histogram) {
                if (regions.length != 1) {
                    throw new IllegalArgumentException("Unable to calculate histogram with " + regions.length + " regions.");
                }
                result = dbAdaptor.getFrequency(query, Region.parseRegion(regions[0]), interval);
            } else if (StringUtils.isNotEmpty(groupBy)) {
                result = dbAdaptor.groupBy(query, groupBy, queryOptions);
            } else if (StringUtils.isNotEmpty(rank)) {
                int limit = addDefaultLimit(queryOptions, LIMIT_MAX, 10);
                boolean asc = false;
                if (rank.contains(":")) {  //  eg. gene:-1
                    String[] arr = rank.split(":");
                    rank = arr[0];
                    if (arr[1].endsWith("-1")) {
                        asc = true;
                    }
                }
                result = dbAdaptor.rank(query, rank, limit, asc);
            } else if (queryOptions.getBoolean(SAMPLES_METADATA)) {
                List<ObjectMap> list = samplesMap.entrySet().stream()
                        .map(entry -> new ObjectMap("id", entry.getKey()).append("samples", entry.getValue()))
                        .collect(Collectors.toList());
                result = new QueryResult("getVariantSamples", 0, list.size(), list.size(), "", "", list);
            } else {
                addDefaultLimit(queryOptions);
                logger.debug("getVariants {}, {}", query, queryOptions);
                result = dbAdaptor.get(query, queryOptions);
                logger.debug("gotVariants {}, {}, in {}ms", result.getNumResults(), result.getNumTotalResults(), result.getDbTime());
                if (queryOptions.getString("model", "opencb").equalsIgnoreCase("ga4gh")) {
                    result = convertToGA4GH(result);
                }
            }
            return result;
        }
    }

    public VariantDBIterator iterator(Query query, QueryOptions queryOptions, String sessionId) throws CatalogException, StorageEngineException {

        long studyId = getMainStudyId(query, sessionId);

        VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId);
        checkSamplesPermissions(query, queryOptions, dbAdaptor, sessionId);
        // TODO: Check returned files

        return dbAdaptor.iterator(query, queryOptions);
    }

    public QueryResult<Long> countByFile(long fileId, QueryOptions params, String sessionId) throws CatalogException, StorageEngineException, IOException {
        Query query = getVariantQuery(params);
        if (getMainStudyId(query, VariantQueryParam.STUDIES.key(), sessionId) == null) {
            long studyId = catalogManager.getStudyIdByFileId(fileId);
            query.put(VariantQueryParam.STUDIES.key(), studyId);
        }
        query.put(VariantQueryParam.FILES.key(), fileId);
        return count(query, sessionId);
    }

    public QueryResult<Long> count(long studyId, QueryOptions params, String sessionId) throws CatalogException, StorageEngineException, IOException {
        Query query = getVariantQuery(params);
        if (getMainStudyId(query, VariantQueryParam.STUDIES.key(), sessionId) == null) {
            query.put(VariantQueryParam.STUDIES.key(), studyId);
        }
        return count(query, sessionId);
    }

    public QueryResult<Long> count(Query query, String sessionId) throws CatalogException, StorageEngineException, IOException {

        long studyId = getMainStudyId(query, sessionId);

        // Closed by Variant Fetcher
        VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId);
            // TODO: Check permissions?
        return dbAdaptor.count(query);
    }

    protected int addDefaultLimit(QueryOptions queryOptions) {
        return addDefaultLimit(queryOptions, LIMIT_MAX, LIMIT_DEFAULT);
    }

    protected int addDefaultLimit(QueryOptions queryOptions, int limitMax, int limitDefault) {
        // Add default limit
        int limit = queryOptions.getInt("limit", -1);
        if (limit > limitMax) {
            logger.info("Unable to return more than {} variants. Change limit from {} to {}", limitMax, limit, limitMax);
        }
        limit = (limit > 0) ? Math.min(limit, limitMax) : limitDefault;
        queryOptions.put("limit",  limit);
        return limit;
    }

    protected String[] getRegions(Query query) {
        String[] regions;
        String regionStr = query.getString(VariantQueryParam.REGION.key());
        if (!StringUtils.isEmpty(regionStr)) {
            regions = regionStr.split(",");
        } else {
            regions = new String[0];
        }
        return regions;
    }

    public Long getMainStudyId(Query query, String sessionId) throws CatalogException {
        Long id = getMainStudyId(query, VariantQueryParam.STUDIES.key(), sessionId);
        if (id == null) {
            id = getMainStudyId(query, VariantQueryParam.RETURNED_STUDIES.key(), sessionId);
        }
        if (id != null) {
            return id;
        } else {
            throw new IllegalArgumentException("Missing StudyId. Unable to get any variant!");
        }
    }

    private Long getMainStudyId(Query query, String key, String sessionId) throws CatalogException {
        if (query.containsKey(key)) {
            for (String id : query.getAsStringList(key)) {
                if (!id.startsWith("!")) {
                    long studyId = catalogManager.getStudyId(id, sessionId);
                    return studyId > 0 ? studyId : null;
                }
            }
        }
        return null;
    }

    protected Map<Long, List<Sample>> checkSamplesPermissions(Query query, QueryOptions queryOptions, VariantDBAdaptor dbAdaptor, String sessionId) throws CatalogException {
        final Map<Long, List<Sample>> samplesMap;
        if (query.containsKey(VariantQueryParam.RETURNED_SAMPLES.key())) {
            Map<Integer, List<Integer>> samplesToReturn = dbAdaptor.getReturnedSamples(query, queryOptions);
            samplesMap = new HashMap<>();
            for (Map.Entry<Integer, List<Integer>> entry : samplesToReturn.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    QueryResult<Sample> samplesQueryResult = catalogManager.getAllSamples(entry.getKey(),
                            new Query(SampleDBAdaptor.QueryParams.ID.key(), entry.getValue()),
                            new QueryOptions("exclude", Arrays.asList("projects.studies.samples.annotationSets",
                                    "projects.studies.samples.attributes"))
                            , sessionId);
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
                                "projects.studies.samples.attributes"))
                        , sessionId);
                samplesQueryResult.getResult().sort((o1, o2) -> Long.compare(o1.getId(), o2.getId()));
                samplesMap.put(study.getId(), samplesQueryResult.getResult());
                samplesQueryResult.getResult().stream().map(Sample::getId).forEach(returnedSamples::add);
            }
            query.append(VariantQueryParam.RETURNED_SAMPLES.key(), returnedSamples);
        }
        return samplesMap;
    }

    @Override
    public void close() throws Exception {
        while (!this.variantDBAdaptor.isEmpty()) {
            String key = this.variantDBAdaptor.keys().nextElement();
            VariantDBAdaptor adaptor = this.variantDBAdaptor.remove(key);
            if (adaptor != null) {
                try{
                    adaptor.close();
                } catch (Exception e) {
                    logger.error("Issue closing VariantDBadaptor", e);
                }
            }
        }
    }

    protected VariantDBAdaptor getVariantDBAdaptor(long studyId, String sessionId) throws CatalogException, StorageEngineException {
        String key = studyId + "_" + sessionId;
        if (!this.variantDBAdaptor.containsKey(key)) {
            // Set new key
            DataStore dataStore = StorageOperation.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
            String storageEngine = dataStore.getStorageEngine();
            String dbName = dataStore.getDbName();
            try {
                this.variantDBAdaptor.computeIfAbsent(key, (str) -> {
                    try {
                        return storageEngineFactory.getVariantStorageEngine(storageEngine, dbName).getDBAdaptor();
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageEngineException e) {
                        throw new IllegalStateException("Unable to get VariantDBAdaptor", e);
                    }
                });
            } catch (IllegalStateException e) {
                throw new StorageEngineException("Problems creating VariantDBAdaptor", e);
            }
        }
        return variantDBAdaptor.get(key);
    }

    protected QueryResult<org.ga4gh.models.Variant> convertToGA4GH(QueryResult<Variant> result) {
        Ga4ghVariantConverter<org.ga4gh.models.Variant> converter = Ga4ghVariantConverter.newAvroConverter(false, null);
        List<org.ga4gh.models.Variant> gaVariants = converter.apply(result.getResult());
        QueryResult<org.ga4gh.models.Variant> gaResult = new QueryResult<>(result.getId(), result.getDbTime(), result.getNumResults(), result.getNumTotalResults(), result.getWarningMsg(), result.getErrorMsg(), gaVariants);
        return gaResult;
    }

    public static Query getVariantQuery(QueryOptions queryOptions) {
        Query query = new Query();

        for (VariantQueryParam queryParams : VariantQueryParam.values()) {
            if (queryOptions.containsKey(queryParams.key())) {
                query.put(queryParams.key(), queryOptions.get(queryParams.key()));
            }
        }

        return query;
    }

    public VariantSourceDBAdaptor getSourceDBAdaptor(int studyId, String sessionId) throws CatalogException, StorageEngineException {
        return getVariantDBAdaptor(studyId, sessionId).getVariantSourceDBAdaptor();
    }

}
