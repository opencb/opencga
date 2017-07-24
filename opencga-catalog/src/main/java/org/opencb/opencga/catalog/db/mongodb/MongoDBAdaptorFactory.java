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

package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DuplicateKeyException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
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
            "family",
            "clinical",
            "metadata",
            "audit"
    );

    public static final String USER_COLLECTION = "user";
    public static final String STUDY_COLLECTION = "study";
    public static final String FILE_COLLECTION = "file";
    public static final String JOB_COLLECTION = "job";
    public static final String SAMPLE_COLLECTION = "sample";
    public static final String INDIVIDUAL_COLLECTION = "individual";
    public static final String COHORT_COLLECTION = "cohort";
    public static final String FAMILY_COLLECTION = "family";
    public static final String DATASET_COLLECTION = "dataset";
    public static final String PANEL_COLLECTION = "panel";
    public static final String CLINICAL_ANALYSIS_COLLECTION = "clinical";
    public static final String METADATA_COLLECTION = "metadata";
    public static final String AUDIT_COLLECTION = "audit";
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
    private MongoDBCollection familyCollection;
    private MongoDBCollection datasetCollection;
    private MongoDBCollection panelCollection;
    private MongoDBCollection clinicalCollection;
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
    private FamilyMongoDBAdaptor familyDBAdaptor;
    private DatasetMongoDBAdaptor datasetDBAdaptor;
    private PanelMongoDBAdaptor panelDBAdaptor;
    private ClinicalAnalysisDBAdaptor clinicalDBAdaptor;
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
        } else {
            throw new CatalogDBException("Catalog already initialized");
        }
    }

    @Override
    public void installCatalogDB(Configuration configuration) throws CatalogException {
        // TODO: Check META object does not exist. Use {@link isCatalogDBReady}
        // TODO: Check all collections do not exists, or are empty
        // TODO: Catch DuplicatedKeyException while inserting META object

        MongoDataStore mongoDataStore = mongoManager.get(database, this.configuration);
        if (mongoDataStore.getCollectionNames().size() > 0) {
            throw new CatalogException("Database " + database + " already exists with the following collections: "
                + StringUtils.join(mongoDataStore.getCollectionNames()) + ".\nPlease, remove the database or choose a different one.");
        }
        COLLECTIONS_LIST.forEach(mongoDataStore::createCollection);
        metaDBAdaptor.createIndexes();
        metaDBAdaptor.initializeMetaCollection(configuration);
    }

    @Override
    public void createIndexes() throws CatalogDBException {
        metaDBAdaptor.createIndexes();
    }

    @Override
    public ObjectMap getDatabaseStatus() {
        Document dbStatus = mongoManager.get(database, this.configuration).getServerStatus();
        try {
            ObjectMap map = new ObjectMap(new ObjectMapper().writeValueAsString(dbStatus));
            return new ObjectMap("ok", map.getInt("ok", 0) > 0);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
            return new ObjectMap();
        }
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
    public FamilyMongoDBAdaptor getCatalogFamilyDBAdaptor() {
        return familyDBAdaptor;
    }

    @Override
    public ClinicalAnalysisDBAdaptor getClinicalAnalysisDBAdaptor() {
        return clinicalDBAdaptor;
    }

    @Override
    public Map<String, MongoDBCollection> getMongoDBCollectionMap() {
        return collections;
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
        familyCollection = db.getCollection(FAMILY_COLLECTION);
        clinicalCollection = db.getCollection(CLINICAL_ANALYSIS_COLLECTION);

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
        collections.put(FAMILY_COLLECTION, familyCollection);
        collections.put(CLINICAL_ANALYSIS_COLLECTION, clinicalCollection);

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
        familyDBAdaptor = new FamilyMongoDBAdaptor(familyCollection, this);
        clinicalDBAdaptor = new ClinicalAnalysisMongoDBAdaptor(clinicalCollection, this);
        metaDBAdaptor = new MetaMongoDBAdaptor(metaCollection, this);
        auditDBAdaptor = new AuditMongoDBAdaptor(auditCollection);

    }

}
