package org.opencb.opencga.server.utils;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converter.ga4gh.GAVariantFactory;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Index;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 *
 * Created on 18/08/15.
 */
public class VariantFetcher {

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

    public org.opencb.datastore.core.QueryResult variantsFile(String region, boolean histogram, String groupBy, int interval, String fileId, String sessionId, QueryOptions queryOptions)
            throws Exception {
        org.opencb.datastore.core.QueryResult result;
        long fileIdNum;

        fileIdNum = catalogManager.getFileId(fileId);
        File file = catalogManager.getFile(fileIdNum, sessionId).first();

        if (file.getIndex() == null || file.getIndex().getStatus() != Index.Status.READY) {
            throw new Exception("File {id:" + file.getId() + " name:'" + file.getName() + "'} " +
                    " is not an indexed file.");
        }
        if (!file.getBioformat().equals(File.Bioformat.VARIANT)) {
            throw new Exception("File {id:" + file.getId() + " name:'" + file.getName() + "'} " +
                    " is not a Variant file.");
        }

        long studyId = catalogManager.getStudyIdByFileId(file.getId());
        result = variantsStudy(studyId, region, histogram, groupBy, interval, fileIdNum, sessionId, queryOptions);
        return result;
    }

    public org.opencb.datastore.core.QueryResult variantsStudy(long studyId, String region, boolean histogram, String groupBy, int interval, String sessionId, QueryOptions queryOptions) throws Exception {
        return variantsStudy(studyId, region, histogram, groupBy, interval, null, sessionId, queryOptions);
    }

    public org.opencb.datastore.core.QueryResult variantsStudy(long studyId, String regionStr, boolean histogram, String groupBy, int interval, Long fileIdNum, String sessionId, QueryOptions queryOptions)
            throws Exception {
//        QueryResult result;
        org.opencb.datastore.core.QueryResult result;
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);

        String storageEngine = dataStore.getStorageEngine();
        String dbName = dataStore.getDbName();

        int limit = queryOptions.getInt("limit", -1);
        if (limit > LIMIT_MAX) {
            logger.info("Unable to return more than {} variants. Change limit from {} to {}", LIMIT_MAX, limit, LIMIT_MAX);
        }
        queryOptions.put("limit", (limit > 0) ? Math.min(limit, LIMIT_MAX) : LIMIT_DEFAULT);

        Query query = getVariantQuery(queryOptions);

        if (fileIdNum != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileIdNum);
        }
        if (!query.containsKey(VariantDBAdaptor.VariantQueryParams.STUDIES.key())) {
            query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyId);
        }

        //TODO: Check files and studies exists

        logger.debug("queryVariants = {}", query.toJson());
        VariantDBAdaptor dbAdaptor = storageManagerFactory.getVariantStorageManager(storageEngine).getDBAdaptor(dbName);
//        dbAdaptor.setStudyConfigurationManager(new CatalogStudyConfigurationManager(catalogManager, sessionId));
        Map<Integer, List<Integer>> samplesToReturn = dbAdaptor.getReturnedSamples(new org.opencb.datastore.core.Query(query), new org.opencb.datastore.core.QueryOptions(queryOptions));

        Map<Object, List<Sample>> samplesMap = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : samplesToReturn.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                QueryResult<Sample> samplesQueryResult = catalogManager.getAllSamples(entry.getKey(),
                        new Query(CatalogSampleDBAdaptor.QueryParams.ID.key(), entry.getValue()),
                        new QueryOptions("exclude", Arrays.asList("projects.studies.samples.annotationSets",
                                        "projects.studies.samples.attributes"))
                        , sessionId);
                if (samplesQueryResult.getNumResults() != entry.getValue().size()) {
                    throw new CatalogAuthorizationException("Permission denied. User " + catalogManager.getUserIdBySessionId(sessionId)
                            + " can't read all the requested samples");
                }
                samplesMap.put(entry.getKey(), samplesQueryResult.getResult());
            }
        }

        String[] regions;
        if (regionStr != null) {
            regions = regionStr.split(",");
        } else {
            regions = new String[0];
        }

        if (histogram) {
            if (regions.length != 1) {
                throw new IllegalArgumentException("Unable to calculate histogram with " + regions.length + " regions.");
            }
            result = dbAdaptor.getFrequency(new org.opencb.datastore.core.Query(query), Region.parseRegion(regions[0]), interval);
        } else if (groupBy != null && !groupBy.isEmpty()) {
            result = dbAdaptor.groupBy(new org.opencb.datastore.core.Query(query), groupBy, new org.opencb.datastore.core.QueryOptions(queryOptions));
        } else if (queryOptions.getBoolean("samplesMetadata")) {
            List<ObjectMap> list = samplesMap.entrySet().stream()
                    .map(entry -> new ObjectMap("id", entry.getKey()).append("samples", entry.getValue()))
                    .collect(Collectors.toList());
            result = new org.opencb.datastore.core.QueryResult("getVariantSamples", 0, list.size(), list.size(), "", "", list);
        } else {
            logger.debug("getVariants {}, {}", query, queryOptions);
            result = dbAdaptor.get(new org.opencb.datastore.core.Query(query), new org.opencb.datastore.core.QueryOptions(queryOptions));
            logger.debug("gotVariants {}, {}, in {}ms", result.getNumResults(), result.getNumTotalResults(), result.getDbTime());
            if (queryOptions.getString("model", "opencb").equalsIgnoreCase("ga4gh")) {
                result = convertToGA4GH(result);
            }
        }
        return result;
    }

    private org.opencb.datastore.core.QueryResult<org.ga4gh.models.Variant> convertToGA4GH(org.opencb.datastore.core.QueryResult<Variant> result) {
        GAVariantFactory factory = new GAVariantFactory();
        List<org.ga4gh.models.Variant> gaVariants = factory.create(result.getResult());
        org.opencb.datastore.core.QueryResult<org.ga4gh.models.Variant> gaResult = new org.opencb.datastore.core.QueryResult<>(result.getId(), result.getDbTime(), result.getNumResults(), result.getNumTotalResults(), result.getWarningMsg(), result.getErrorMsg(), gaVariants);
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

}
