package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DuplicateKeyException;
import org.bson.Document;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.getMongoDBDocument;

/**
 * Created by pfurio on 08/01/16.
 */
public class CatalogMongoDBAdaptorFactory implements CatalogDBAdaptorFactory {

    static final String METADATA_OBJECT_ID = "METADATA";
    private static final String USER_COLLECTION = "user";
    private static final String STUDY_COLLECTION = "study";
    private static final String FILE_COLLECTION = "file";
    private static final String JOB_COLLECTION = "job";
    private static final String SAMPLE_COLLECTION = "sample";
    private static final String INDIVIDUAL_COLLECTION = "individual";
    private static final String METADATA_COLLECTION = "metadata";
    private static final String AUDIT_COLLECTION = "audit";
    private final MongoDataStoreManager mongoManager;
    private final MongoDBConfiguration configuration;
    private final String database;
    //    private final DataStoreServerAddress dataStoreServerAddress;
    private MongoDataStore db;

    private MongoDBCollection metaCollection;
    private MongoDBCollection userCollection;
    private MongoDBCollection studyCollection;
    private MongoDBCollection fileCollection;
    private MongoDBCollection sampleCollection;
    private MongoDBCollection individualCollection;
    private MongoDBCollection jobCollection;
    private MongoDBCollection auditCollection;
    private Map<String, MongoDBCollection> collections;
    private CatalogMongoUserDBAdaptor userDBAdaptor;
    private CatalogMongoStudyDBAdaptor studyDBAdaptor;
    private CatalogMongoIndividualDBAdaptor individualDBAdaptor;
    private CatalogMongoSampleDBAdaptor sampleDBAdaptor;
    private CatalogMongoFileDBAdaptor fileDBAdaptor;
    private CatalogMongoJobDBAdaptor jobDBAdaptor;
    private CatalogMongoProjectDBAdaptor projectDBAdaptor;
    private CatalogMongoAuditDBAdaptor auditDBAdaptor;
    private CatalogMongoMetaDBAdaptor metaDBAdaptor;

    private Logger logger;

    public CatalogMongoDBAdaptorFactory(List<DataStoreServerAddress> dataStoreServerAddressList, MongoDBConfiguration configuration,
                                        String database) throws CatalogDBException {
//        super(LoggerFactory.getLogger(CatalogMongoDBAdaptor.class));
        this.mongoManager = new MongoDataStoreManager(dataStoreServerAddressList);
        this.configuration = configuration;
        this.database = database;

        logger = LoggerFactory.getLogger(this.getClass());
        connect();
    }

    @Override
    public void initializeCatalogDB() throws CatalogDBException {
        //If "metadata" document doesn't exist, create.
        if (!isCatalogDBReady()) {

            /* Check all collections are empty */
            for (Map.Entry<String, MongoDBCollection> entry : collections.entrySet()) {
                if (entry.getValue().count().first() != 0L) {
                    throw new CatalogDBException("Fail to initialize Catalog Database in MongoDB. Collection " + entry.getKey() + " is " +
                            "not empty.");
                }
            }

            try {
//                DBObject metadataObject = getDbObject(new Metadata(), "Metadata");
                Document metadataObject = getMongoDBDocument(new Metadata(), "Metadata");
                metadataObject.put("_id", METADATA_OBJECT_ID);
                metaCollection.insert(metadataObject, null);

            } catch (DuplicateKeyException e) {
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
            }
            //Set indexes
//            BasicDBObject unique = new BasicDBObject("unique", true);
//            nativeUserCollection.createIndex(new BasicDBObject("id", 1), unique);
//            nativeFileCollection.createIndex(BasicDBObjectBuilder.start("studyId", 1).append("path", 1).get(), unique);
//            nativeJobCollection.createIndex(new BasicDBObject("id", 1), unique);
        } else {
            throw new CatalogDBException("Catalog already initialized");
        }
    }

    @Override
    public boolean isCatalogDBReady() {
        QueryResult<Long> queryResult = metaCollection.count(new BasicDBObject("_id", METADATA_OBJECT_ID));
        return queryResult.getResult().get(0) == 1;
    }

    @Override
    public void close() {
        mongoManager.close(db.getDatabaseName());
    }

    @Override
    public CatalogMongoUserDBAdaptor getCatalogUserDBAdaptor() {
        return userDBAdaptor;
    }

    @Override
    public CatalogMongoProjectDBAdaptor getCatalogProjectDbAdaptor() {
        return projectDBAdaptor;
    }

    @Override
    public CatalogMongoStudyDBAdaptor getCatalogStudyDBAdaptor() {
        return studyDBAdaptor;
    }

    @Override
    public CatalogMongoSampleDBAdaptor getCatalogSampleDBAdaptor() {
        return sampleDBAdaptor;
    }

    @Override
    public CatalogMongoIndividualDBAdaptor getCatalogIndividualDBAdaptor() {
        return individualDBAdaptor;
    }

    @Override
    public CatalogMongoFileDBAdaptor getCatalogFileDBAdaptor() {
        return fileDBAdaptor;
    }

    @Override
    public CatalogMongoJobDBAdaptor getCatalogJobDBAdaptor() {
        return jobDBAdaptor;
    }

    public CatalogMongoMetaDBAdaptor getCatalogMetaDBAdaptor() {
        return metaDBAdaptor;
    }

    @Override
    public CatalogMongoAuditDBAdaptor getCatalogAuditDbAdaptor() {
        return auditDBAdaptor;
    }

    private void connect() throws CatalogDBException {
        db = mongoManager.get(database, configuration);
        if (db == null) {
            throw new CatalogDBException("Unable to connect to MongoDB");
        }

        collections = new HashMap<>();
        collections.put(METADATA_COLLECTION, metaCollection = db.getCollection(METADATA_COLLECTION));
        collections.put(USER_COLLECTION, userCollection = db.getCollection(USER_COLLECTION));
        collections.put(STUDY_COLLECTION, studyCollection = db.getCollection(STUDY_COLLECTION));
        collections.put(FILE_COLLECTION, fileCollection = db.getCollection(FILE_COLLECTION));
        collections.put(SAMPLE_COLLECTION, sampleCollection = db.getCollection(SAMPLE_COLLECTION));
        collections.put(INDIVIDUAL_COLLECTION, individualCollection = db.getCollection(INDIVIDUAL_COLLECTION));
        collections.put(JOB_COLLECTION, jobCollection = db.getCollection(JOB_COLLECTION));
        collections.put(AUDIT_COLLECTION, auditCollection = db.getCollection(AUDIT_COLLECTION));

        /*
        userDBAdaptor = new CatalogMongoUserDBAdaptor(this, metaCollection, userCollection);
        studyDBAdaptor = new CatalogMongoStudyDBAdaptor(this, metaCollection, studyCollection, fileCollection);
        individualDBAdaptor = new CatalogMongoIndividualDBAdaptor(this, metaCollection, individualCollection);
        sampleDBAdaptor = new CatalogMongoSampleDBAdaptor(this, metaCollection, sampleCollection, studyCollection);
        auditDBAdaptor = new CatalogMongoAuditDBAdaptor(auditCollection);
        */

        fileDBAdaptor = new CatalogMongoFileDBAdaptor(fileCollection, this);
        individualDBAdaptor = new CatalogMongoIndividualDBAdaptor(individualCollection, this);
        jobDBAdaptor = new CatalogMongoJobDBAdaptor(jobCollection, this);
        projectDBAdaptor = new CatalogMongoProjectDBAdaptor(userCollection, this);
        sampleDBAdaptor = new CatalogMongoSampleDBAdaptor(sampleCollection, this);
        studyDBAdaptor = new CatalogMongoStudyDBAdaptor(studyCollection, this);
        userDBAdaptor = new CatalogMongoUserDBAdaptor(userCollection, this);
        auditDBAdaptor = new CatalogMongoAuditDBAdaptor(auditCollection);

    }

}
