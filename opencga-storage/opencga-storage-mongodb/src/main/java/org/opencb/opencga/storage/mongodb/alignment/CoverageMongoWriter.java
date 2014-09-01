package org.opencb.opencga.storage.mongodb.alignment;

import com.mongodb.*;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.commons.io.DataWriter;

import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jacobo on 26/08/14.
 */
public class CoverageMongoWriter implements DataWriter<AlignmentRegion> {

    public static final String COVERAGE_COLLECTION_NAME = "alignment";

    public static final String ID_FIELD = "_id";
    public static final String COVERAGE_FIELD = "cov";
    public static final String FILES_FIELD = "files";
    public static final String FILE_ID_FIELD = "id";
    public static final String AVERAGE_FIELD = "avg";

    private final MongoDataStoreManager mongoManager;
    private final String fileId;
    private MongoDataStore db;
    private final DBObjectToRegionCoverageConverter coverageConverter;
    private final DBObjectToMeanCoverageConverter meanCoverageConverter;
    private final MongoCredentials credentials;
    private final String collectionName;
    private MongoDBCollection collection;

    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(CoverageMongoWriter.class);

    public CoverageMongoWriter(MongoCredentials credentials, String fileId) {
        this.collectionName = COVERAGE_COLLECTION_NAME;
        this.credentials = credentials;
        mongoManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        coverageConverter = new DBObjectToRegionCoverageConverter();
        meanCoverageConverter = new DBObjectToMeanCoverageConverter();
        this.fileId = fileId;
    }

    @Override
    public boolean open() {
        //MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder().add("username", "biouser").add("password", "*******").build();
        db = mongoManager.get(credentials.getMongoDbName());

        return true;
    }

    @Override
    public boolean close() {
        mongoManager.close(db.getDatabaseName());
        return true;
    }

    @Override
    public boolean pre() {
        collection = db.createCollection(collectionName);
        return true;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public boolean write(AlignmentRegion elem) {
        List<MeanCoverage> meanCoverageList = elem.getMeanCoverage();
        RegionCoverage regionCoverage = elem.getCoverage();

        if(regionCoverage != null){
            DBObject coverageQuery = coverageConverter.getIdObject(regionCoverage);
            DBObject coverageObject = coverageConverter.convertToStorageType(regionCoverage);
            secureInsert(coverageQuery, coverageObject);
        }

        if(meanCoverageList != null) {
            for (MeanCoverage meanCoverage : meanCoverageList) {
                DBObject query = this.meanCoverageConverter.getIdObject(meanCoverage);  //{_id:"20_2354_1k"}
                DBObject object = meanCoverageConverter.convertToStorageType(meanCoverage);  //{avg:4.5662}
                secureInsert(query, object);
            }
        }
        return true;
    }

    private void secureInsert(DBObject query, DBObject object) {
        boolean documentExists = true;
        boolean fileExists = true;

        //Check if the document exists
        {
            QueryResult countId = collection.count(query);
            if (countId.getNumResults() == 1 && countId.getResultType().equals(Long.class.getCanonicalName())) {
                if ((Long) countId.getResult().get(0) < 1) {
                    DBObject document = new BasicDBObject(FILES_FIELD, new BasicDBList());
                    document.putAll(query);             //{_id:<chunkId>, files:[]}
                    collection.insert(document);        //Insert a document with empty files array.
                    fileExists = false;
                }
            } else {
                logger.error(countId.getErrorMsg(), countId);
            }
        }

        if(documentExists){
            //Check if the file exists
            BasicDBObject fileQuery = new BasicDBObject(FILES_FIELD + "." + FILE_ID_FIELD, fileId);
            fileQuery.putAll(query);
            QueryResult countFile = collection.count(fileQuery);
            if(countFile.getNumResults() == 1 && countFile.getResultType().equals(Long.class.getCanonicalName())) {
                if((Long) countFile.getResult().get(0) < 1){
                    fileExists = false;
                }
            } else {
                logger.error(countFile.getErrorMsg(), countFile);
            }
        }

        if(fileExists){
            BasicDBObject fileQuery = new BasicDBObject(FILES_FIELD + "." + FILE_ID_FIELD, fileId);
            fileQuery.putAll(query);

            BasicDBObject fileObject = new BasicDBObject();
            for(String key : object.keySet()){
                fileObject.put(FILES_FIELD+".$."+key,object.get(key));
            }
//            DBObject update = new BasicDBObject("$set", new BasicDBObject(FILES_FIELD, fileObject));
            DBObject update = new BasicDBObject("$set", fileObject);

            //db.<collectionName>.update({_id:<chunkId>  , "files.id":<fileId>}, {$set:{"files.$.<objKey>":<objValue>}})
            collection.update(fileQuery, update, true, false);
        } else {
            BasicDBObject fileObject = new BasicDBObject(FILE_ID_FIELD, fileId);
            fileObject.putAll(object);
            DBObject update = new BasicDBObject("$addToSet", new BasicDBObject(FILES_FIELD, fileObject));

            //db.<collectionName>.update({_id:<chunkId>} , {$addToSet:{files:{id:<fileId>, <object>}}})
            collection.update(query, update, true, false);
        }
    }

    @Override
    public boolean write(List<AlignmentRegion> batch) {
        for(AlignmentRegion region : batch){
            if(region != null){
                if(!write(region)){
                    return false;
                }
            }
        }
        return false;
    }
}
