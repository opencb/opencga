package org.opencb.opencga.server.ws;

import org.opencb.biodata.models.core.Region;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.CatalogStudyConfigurationManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Index;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by hpccoll1 on 18/08/15.
 */
public class VariantFetcher {

    private final OpenCGAWSServer wsServer;
    private final CatalogManager catalogManager;
    private final StorageManagerFactory storageManagerFactory;
    private final Logger logger;

    public static final int LIMIT_DEFAULT = 1000;
    public static final int LIMIT_MAX = 5000;

    public VariantFetcher(OpenCGAWSServer wsServer) {
        this.wsServer = wsServer;
        catalogManager = OpenCGAWSServer.catalogManager;
        storageManagerFactory = OpenCGAWSServer.storageManagerFactory;
        logger = LoggerFactory.getLogger(VariantFetcher.class);
    }

    public QueryResult variantsFile(String region, boolean histogram, String groupBy, int interval, String fileId)
            throws Exception {
        QueryResult result;
        int fileIdNum;

        fileIdNum = catalogManager.getFileId(fileId);
        File file = catalogManager.getFile(fileIdNum, wsServer.sessionId).first();

        if (file.getIndex() == null || file.getIndex().getStatus() != Index.Status.READY) {
            throw new Exception("File {id:" + file.getId() + " name:'" + file.getName() + "'} " +
                    " is not an indexed file.");
        }
        if (!file.getBioformat().equals(File.Bioformat.VARIANT)) {
            throw new Exception("File {id:" + file.getId() + " name:'" + file.getName() + "'} " +
                    " is not a Variant file.");
        }

        int studyId = catalogManager.getStudyIdByFileId(file.getId());
        result = variantsStudy(studyId, region, histogram, groupBy, interval, fileIdNum);
        return result;
    }

    public QueryResult variantsStudy(int studyId, String region, boolean histogram, String groupBy, int interval) throws Exception {
        return variantsStudy(studyId, region, histogram, groupBy, interval, null);
    }

    public QueryResult variantsStudy(int studyId, String regionStr, boolean histogram, String groupBy, int interval, Integer fileIdNum)
            throws Exception {
        QueryResult result;
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, wsServer.sessionId);

        String storageEngine = dataStore.getStorageEngine();
        String dbName = dataStore.getDbName();

        QueryOptions queryOptions = wsServer.queryOptions;
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


        logger.debug("queryVariants = {}", query.toJson());
        VariantDBAdaptor dbAdaptor = storageManagerFactory.getVariantStorageManager(storageEngine).getDBAdaptor(dbName);
        dbAdaptor.setStudyConfigurationManager(new CatalogStudyConfigurationManager(catalogManager, wsServer.sessionId));

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
            result = dbAdaptor.getFrequency(query, Region.parseRegion(regions[0]), interval);
        } else if (!groupBy.isEmpty()) {
            result = dbAdaptor.groupBy(query, groupBy, queryOptions);
        } else {
            result = dbAdaptor.get(query, queryOptions);
        }
        return result;
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
