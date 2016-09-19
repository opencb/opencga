package org.opencb.opencga.analysis.storage.variant;

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converter.ga4gh.GAVariantFactory;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.variant.AbstractFileIndexer;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 *
 * Created on 18/08/15.
 */
public class VariantFetcher {

    public static final String SAMPLES_METADATA = "samplesMetadata";
    private final CatalogManager catalogManager;
    private final StorageManagerFactory storageManagerFactory;
    private final Logger logger;

    public static final int LIMIT_DEFAULT = 1000;
    public static final int LIMIT_MAX = 5000;

    public VariantFetcher(CatalogManager catalogManager, StorageManagerFactory storageManagerFactory) {
        this.catalogManager = catalogManager;
        this.storageManagerFactory = storageManagerFactory;
        logger = LoggerFactory.getLogger(VariantFetcher.class);
    }

    public QueryResult rank(Query query, QueryOptions queryOptions, String rank, String sessionId)
            throws Exception {
        return getVariantsPerStudy(getMainStudyId(query), query, queryOptions, false, null, rank, 0, null, sessionId);
    }

    public QueryResult groupBy(Query query, QueryOptions queryOptions, String groupBy, String sessionId)
            throws Exception {
        return getVariantsPerStudy(getMainStudyId(query), query, queryOptions, false, groupBy, null, 0, null, sessionId);
    }

    public QueryResult<Variant> get(Query query, QueryOptions queryOptions, String sessionId)
            throws Exception {
        queryOptions.remove("model");
        return getVariantsPerStudy(getMainStudyId(query), query, queryOptions, false, null, null, 0, null, sessionId);
    }

    public QueryResult<org.ga4gh.models.Variant> getGa4gh(Query query, QueryOptions queryOptions, String sessionId)
            throws Exception {
        queryOptions.put("model", "ga4gh");
        return getVariantsPerStudy(getMainStudyId(query), query, queryOptions, false, null, null, 0, null, sessionId);
    }

    public Map<Long, List<Sample>> getSamplesMetadata(long studyId, Query query, QueryOptions queryOptions, String sessionId)
            throws CatalogException, StorageManagerException, IOException {
        try (VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(studyId, sessionId)) {
            return checkSamplesPermissions(query, queryOptions, variantDBAdaptor, sessionId);
        }
    }

    public StudyConfiguration getStudyConfiguration(long studyId, QueryOptions options, String sessionId)
            throws CatalogException, StorageManagerException, IOException {
        try (VariantDBAdaptor variantDBAdaptor = getVariantDBAdaptor(studyId, sessionId)) {
            return variantDBAdaptor.getStudyConfigurationManager().getStudyConfiguration((int) studyId, options).first();
        }
    }

    public QueryResult getVariantsPerFile(String region, boolean histogram, String groupBy, int interval, String fileId, String sessionId, QueryOptions queryOptions)
            throws Exception {
        QueryResult result;
        long fileIdNum;

        fileIdNum = catalogManager.getFileId(fileId, sessionId);
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
        queryOptions.add(VariantDBAdaptor.VariantQueryParams.REGION.key(), regionStr);
        return getVariantsPerStudy(studyId, getVariantQuery(queryOptions), queryOptions, histogram, groupBy, null, interval, fileIdNum, sessionId);
    }

    public QueryResult getVariantsPerStudy(long studyId, Query query, QueryOptions queryOptions, boolean histogram, String groupBy, String rank, int interval, Long fileIdNum, String sessionId)
            throws Exception {
        QueryResult result;
        logger.debug("queryVariants = {}", query.toJson());

        //TODO: Check files and studies exists

        if (fileIdNum != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileIdNum);
        }
        if (!query.containsKey(VariantDBAdaptor.VariantQueryParams.STUDIES.key())) {
            query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyId);
        }

        VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId);
        // TODO: Check returned files

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

    public VariantDBIterator iterator(Query query, QueryOptions queryOptions, String sessionId) throws CatalogException, StorageManagerException {

        long studyId = getMainStudyId(query);

        VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId);

        checkSamplesPermissions(query, queryOptions, dbAdaptor, sessionId);
        // TODO: Check returned files

        return dbAdaptor.iterator(query, queryOptions);
    }
    public QueryResult<Long> count(Query query, String sessionId) throws CatalogException, StorageManagerException, IOException {

        long studyId = getMainStudyId(query);

        try (VariantDBAdaptor dbAdaptor = getVariantDBAdaptor(studyId, sessionId)) {

            // TODO: Check permissions?

            return dbAdaptor.count(query);
        }
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
        String regionStr = query.getString(VariantDBAdaptor.VariantQueryParams.REGION.key());
        if (!StringUtils.isEmpty(regionStr)) {
            regions = regionStr.split(",");
        } else {
            regions = new String[0];
        }
        return regions;
    }

    public Long getMainStudyId(Query query) throws CatalogException {
        Long id = getMainStudyId(query, VariantDBAdaptor.VariantQueryParams.STUDIES.key());
        if (id == null) {
            id = getMainStudyId(query, VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key());
        }
        if (id != null) {
            return id;
        } else {
            throw new IllegalArgumentException("Missing StudyId. Unable to get any variant!");
        }
    }

    private Long getMainStudyId(Query query, String key) throws CatalogException {
        if (query.containsKey(key)) {
            for (String id : query.getAsStringList(key)) {
                if (!id.startsWith("!")) {
                    return catalogManager.getStudyId(id);
                }
            }
        }
        return null;
    }

    protected Map<Long, List<Sample>> checkSamplesPermissions(Query query, QueryOptions queryOptions, VariantDBAdaptor dbAdaptor, String sessionId) throws CatalogException {
        final Map<Long, List<Sample>> samplesMap;
        if (query.containsKey(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key())) {
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
            List<Study> studies = catalogManager.getAllStudies(new Query(StudyDBAdaptor.StudyFilterOptions.id.toString(), returnedStudies),
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
            query.append(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), returnedSamples);
        }
        return samplesMap;
    }

    protected VariantDBAdaptor getVariantDBAdaptor(long studyId, String sessionId) throws CatalogException, StorageManagerException {
        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);

        String storageEngine = dataStore.getStorageEngine();
        String dbName = dataStore.getDbName();

//        dbAdaptor.setStudyConfigurationManager(new CatalogStudyConfigurationManager(catalogManager, sessionId));
        try {
            return storageManagerFactory.getVariantStorageManager(storageEngine).getDBAdaptor(dbName);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new StorageManagerException("Unable to get VariantDBAdaptor", e);
        }
    }

    protected QueryResult<org.ga4gh.models.Variant> convertToGA4GH(QueryResult<Variant> result) {
        GAVariantFactory factory = new GAVariantFactory();
        List<org.ga4gh.models.Variant> gaVariants = factory.create(result.getResult());
        QueryResult<org.ga4gh.models.Variant> gaResult = new QueryResult<>(result.getId(), result.getDbTime(), result.getNumResults(), result.getNumTotalResults(), result.getWarningMsg(), result.getErrorMsg(), gaVariants);
        return gaResult;
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

    public VariantSourceDBAdaptor getSourceDBAdaptor(int studyId, String sessionId) throws CatalogException, StorageManagerException {
        return getVariantDBAdaptor(studyId, sessionId).getVariantSourceDBAdaptor();
    }

}
