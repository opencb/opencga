package org.opencb.opencga.server.ws;

import org.opencb.biodata.models.feature.Region;
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

    public VariantFetcher(OpenCGAWSServer wsServer) {
        this.wsServer = wsServer;
        catalogManager = OpenCGAWSServer.catalogManager;
        storageManagerFactory = OpenCGAWSServer.storageManagerFactory;
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

        Query query = getVariantQuery(wsServer.queryOptions);

        if (fileIdNum != null) {
            query.put(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileIdNum);
        }
        System.out.println("queryVariants = " + query.toJson());
        VariantDBAdaptor dbAdaptor = storageManagerFactory.getVariantStorageManager(storageEngine).getDBAdaptor(dbName);
        dbAdaptor.setStudyConfigurationManager(new CatalogStudyConfigurationManager(catalogManager, wsServer.sessionId));

        List<Region> regions = Region.parseRegions(regionStr);

        long size = 0;
        if (regions == null) {
            regions = Collections.emptyList();
        }
        for (Region region : regions) {
            size += region.getEnd() - region.getStart();
        }
        if (size > 1_000_000) {
            OpenCGAWSServer.logger.warn("Really big query");
        }
        if (histogram) {
            if (regions.size() != 1) {
                throw new IllegalArgumentException("Unable to calculate histogram with " + regions.size() + " regions.");
            }
            result = dbAdaptor.getFrequency(query, regions.get(0), interval);
        } else if (!groupBy.isEmpty()) {
            result = dbAdaptor.groupBy(query, groupBy, wsServer.queryOptions);
        } else {
            result = dbAdaptor.get(query, wsServer.queryOptions);
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
