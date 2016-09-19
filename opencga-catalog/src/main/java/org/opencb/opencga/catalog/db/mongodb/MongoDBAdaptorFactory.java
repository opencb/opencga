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
import org.opencb.opencga.catalog.config.Admin;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.getMongoDBDocument;

/**
 * Created by pfurio on 08/01/16.
 */
public class MongoDBAdaptorFactory implements DBAdaptorFactory {

    private final List<String> COLLECTIONS_LIST = Arrays.asList(
            "user",
            "study",
            "file",
            "job",
            "sample",
            "individual",
            "cohort",
            "dataset",
            "panel",
            "metadata",
            "audit"
    );

    protected static final String USER_COLLECTION = "user";
    protected static final String STUDY_COLLECTION = "study";
    protected static final String FILE_COLLECTION = "file";
    protected static final String JOB_COLLECTION = "job";
    protected static final String SAMPLE_COLLECTION = "sample";
    protected static final String INDIVIDUAL_COLLECTION = "individual";
    protected static final String COHORT_COLLECTION = "cohort";
    protected static final String DATASET_COLLECTION = "dataset";
    protected static final String PANEL_COLLECTION = "panel";
    protected static final String METADATA_COLLECTION = "metadata";
    protected static final String AUDIT_COLLECTION = "audit";
    static final String METADATA_OBJECT_ID = "METADATA";
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
    private MongoDBCollection cohortCollection;
    private MongoDBCollection datasetCollection;
    private MongoDBCollection panelCollection;
    private MongoDBCollection auditCollection;
    private Map<String, MongoDBCollection> collections;
    private UserMongoDBAdaptor userDBAdaptor;
    private StudyMongoDBAdaptor studyDBAdaptor;
    private IndividualMongoDBAdaptor individualDBAdaptor;
    private SampleMongoDBAdaptor sampleDBAdaptor;
    private FileMongoDBAdaptor fileDBAdaptor;
    private JobMongoDBAdaptor jobDBAdaptor;
    private ProjectMongoDBAdaptor projectDBAdaptor;
    private CohortMongoDBAdaptor cohortDBAdaptor;
    private DatasetMongoDBAdaptor datasetDBAdaptor;
    private PanelMongoDBAdaptor panelDBAdaptor;
    private AuditMongoDBAdaptor auditDBAdaptor;
    private MetaMongoDBAdaptor metaDBAdaptor;

    private Logger logger;

    public MongoDBAdaptorFactory(List<DataStoreServerAddress> dataStoreServerAddressList, MongoDBConfiguration configuration,
                                 String database) throws CatalogDBException {
//        super(LoggerFactory.getLogger(CatalogMongoDBAdaptor.class));
        this.mongoManager = new MongoDataStoreManager(dataStoreServerAddressList);
        this.configuration = configuration;
        this.database = database;

        logger = LoggerFactory.getLogger(this.getClass());
        connect();
    }

    @Override
    public void initializeCatalogDB(Admin admin) throws CatalogDBException {
        //If "metadata" document doesn't exist, create.
        if (!isCatalogDBReady()) {

            /* Check all collections are empty */
            for (Map.Entry<String, MongoDBCollection> entry : collections.entrySet()) {
                if (entry.getValue().count().first() != 0L) {
                    throw new CatalogDBException("Fail to initialize Catalog Database in MongoDB. Collection " + entry.getKey() + " is "
                            + "not empty.");
                }
            }

            try {
//                DBObject metadataObject = getDbObject(new Metadata(), "Metadata");
                Document metadataObject = getMongoDBDocument(new Metadata(), "Metadata");
                metadataObject.put("_id", METADATA_OBJECT_ID);
                metadataObject.put("admin", getMongoDBDocument(admin, "Admin"));

                metaCollection.insert(metadataObject, null);

            } catch (DuplicateKeyException e) {
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
            }
            //Set indexes
//            BasicDBObject unique = new BasicDBObject("unique", true);
//            nativeUserCollection.createIndexes(new BasicDBObject("id", 1), unique);
//            nativeFileCollection.createIndexes(BasicDBObjectBuilder.start("studyId", 1).append("path", 1).get(), unique);
//            nativeJobCollection.createIndexes(new BasicDBObject("id", 1), unique);
        } else {
            throw new CatalogDBException("Catalog already initialized");
        }
    }

    @Override
    public void installCatalogDB(CatalogConfiguration catalogConfiguration) throws CatalogException {
        // TODO: Check META object does not exist. Use {@link isCatalogDBReady}
        // TODO: Check all collections do not exists, or are empty
        // TODO: Catch DuplicatedKeyException while inserting META object
        MongoDataStore mongoDataStore = mongoManager.get(database, configuration);
        COLLECTIONS_LIST.forEach(mongoDataStore::createCollection);
        metaDBAdaptor.createIndexes();
        metaDBAdaptor.initializeMetaCollection(catalogConfiguration);
    }

    @Override
    public void createIndexes() throws CatalogDBException {
        metaDBAdaptor.createIndexes();
//        InputStream resourceAsStream = getClass().getResourceAsStream("/catalog-indexes.txt");
//        ObjectMapper objectMapper = new ObjectMapper();
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resourceAsStream));
//        bufferedReader.lines().filter(s -> !s.trim().isEmpty()).forEach(s -> {
//            try {
//                System.out.println(s);
//                HashMap hashMap = objectMapper.readValue(s, HashMap.class);
//                System.out.println(hashMap);
//                QueryResult<Document> index = getCatalogUserDBAdaptor().getUserCollection().getIndex();
//                System.out.println(index);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//        try {
//            bufferedReader.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void deleteCatalogDB() throws CatalogDBException {
        mongoManager.drop(database);
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
    public MetaMongoDBAdaptor getCatalogMetaDBAdaptor() {
        return metaDBAdaptor;
    }

    @Override
    public UserMongoDBAdaptor getCatalogUserDBAdaptor() {
        return userDBAdaptor;
    }

    @Override
    public ProjectMongoDBAdaptor getCatalogProjectDbAdaptor() {
        return projectDBAdaptor;
    }

    @Override
    public StudyMongoDBAdaptor getCatalogStudyDBAdaptor() {
        return studyDBAdaptor;
    }

    @Override
    public SampleMongoDBAdaptor getCatalogSampleDBAdaptor() {
        return sampleDBAdaptor;
    }

    @Override
    public IndividualMongoDBAdaptor getCatalogIndividualDBAdaptor() {
        return individualDBAdaptor;
    }

    @Override
    public FileMongoDBAdaptor getCatalogFileDBAdaptor() {
        return fileDBAdaptor;
    }

    @Override
    public JobMongoDBAdaptor getCatalogJobDBAdaptor() {
        return jobDBAdaptor;
    }

    @Override
    public CohortMongoDBAdaptor getCatalogCohortDBAdaptor() {
        return cohortDBAdaptor;
    }

    @Override
    public DatasetMongoDBAdaptor getCatalogDatasetDBAdaptor() {
        return datasetDBAdaptor;
    }

    @Override
    public PanelDBAdaptor getCatalogPanelDBAdaptor() {
        return panelDBAdaptor;
    }

    @Override
    public AuditMongoDBAdaptor getCatalogAuditDbAdaptor() {
        return auditDBAdaptor;
    }

    private void connect() throws CatalogDBException {
        db = mongoManager.get(database, configuration);
        if (db == null) {
            throw new CatalogDBException("Unable to connect to MongoDB");
        }

        metaCollection = db.getCollection(METADATA_COLLECTION);
        userCollection = db.getCollection(USER_COLLECTION);
        studyCollection = db.getCollection(STUDY_COLLECTION);
        fileCollection = db.getCollection(FILE_COLLECTION);
        sampleCollection = db.getCollection(SAMPLE_COLLECTION);
        individualCollection = db.getCollection(INDIVIDUAL_COLLECTION);
        jobCollection = db.getCollection(JOB_COLLECTION);
        cohortCollection = db.getCollection(COHORT_COLLECTION);
        datasetCollection = db.getCollection(DATASET_COLLECTION);
        auditCollection = db.getCollection(AUDIT_COLLECTION);
        panelCollection = db.getCollection(PANEL_COLLECTION);

        collections = new HashMap<>();
        collections.put(METADATA_COLLECTION, metaCollection);
        collections.put(USER_COLLECTION, userCollection);
        collections.put(STUDY_COLLECTION, studyCollection);
        collections.put(FILE_COLLECTION, fileCollection);
        collections.put(SAMPLE_COLLECTION, sampleCollection);
        collections.put(INDIVIDUAL_COLLECTION, individualCollection);
        collections.put(JOB_COLLECTION, jobCollection);
        collections.put(COHORT_COLLECTION, cohortCollection);
        collections.put(DATASET_COLLECTION, datasetCollection);
        collections.put(AUDIT_COLLECTION, auditCollection);
        collections.put(PANEL_COLLECTION, panelCollection);

        fileDBAdaptor = new FileMongoDBAdaptor(fileCollection, this);
        individualDBAdaptor = new IndividualMongoDBAdaptor(individualCollection, this);
        jobDBAdaptor = new JobMongoDBAdaptor(jobCollection, this);
        projectDBAdaptor = new ProjectMongoDBAdaptor(userCollection, this);
        sampleDBAdaptor = new SampleMongoDBAdaptor(sampleCollection, this);
        studyDBAdaptor = new StudyMongoDBAdaptor(studyCollection, this);
        userDBAdaptor = new UserMongoDBAdaptor(userCollection, this);
        cohortDBAdaptor = new CohortMongoDBAdaptor(cohortCollection, this);
        datasetDBAdaptor = new DatasetMongoDBAdaptor(datasetCollection, this);
        panelDBAdaptor = new PanelMongoDBAdaptor(panelCollection, this);
        metaDBAdaptor = new MetaMongoDBAdaptor(metaCollection, this);
        auditDBAdaptor = new AuditMongoDBAdaptor(auditCollection);

    }

}
