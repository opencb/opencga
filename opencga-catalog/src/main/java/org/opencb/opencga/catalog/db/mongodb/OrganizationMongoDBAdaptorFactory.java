package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

public class OrganizationMongoDBAdaptorFactory {

    public static final String USER_COLLECTION = "user";
    public static final String STUDY_COLLECTION = "study";
    public static final String FILE_COLLECTION = "file";
    public static final String JOB_COLLECTION = "job";
    public static final String SAMPLE_COLLECTION = "sample";
    public static final String INDIVIDUAL_COLLECTION = "individual";
    public static final String COHORT_COLLECTION = "cohort";
    public static final String FAMILY_COLLECTION = "family";
    public static final String PANEL_COLLECTION = "panel";
    public static final String CLINICAL_ANALYSIS_COLLECTION = "clinical";
    public static final String INTERPRETATION_COLLECTION = "interpretation";

    public static final String SAMPLE_ARCHIVE_COLLECTION = "sample_archive";
    public static final String INDIVIDUAL_ARCHIVE_COLLECTION = "individual_archive";
    public static final String FAMILY_ARCHIVE_COLLECTION = "family_archive";
    public static final String PANEL_ARCHIVE_COLLECTION = "panel_archive";
    public static final String INTERPRETATION_ARCHIVE_COLLECTION = "interpretation_archive";

    public static final String DELETED_ORGANIZATION_COLLECTION = "organization_deleted";
    public static final String DELETED_USER_COLLECTION = "user_deleted";
    public static final String DELETED_STUDY_COLLECTION = "study_deleted";
    public static final String DELETED_FILE_COLLECTION = "file_deleted";
    public static final String DELETED_JOB_COLLECTION = "job_deleted";
    public static final String DELETED_SAMPLE_COLLECTION = "sample_deleted";
    public static final String DELETED_INDIVIDUAL_COLLECTION = "individual_deleted";
    public static final String DELETED_COHORT_COLLECTION = "cohort_deleted";
    public static final String DELETED_FAMILY_COLLECTION = "family_deleted";
    public static final String DELETED_PANEL_COLLECTION = "panel_deleted";
    public static final String DELETED_CLINICAL_ANALYSIS_COLLECTION = "clinical_deleted";
    public static final String DELETED_INTERPRETATION_COLLECTION = "interpretation_deleted";

    public static final String METADATA_COLLECTION = "metadata";
    public static final String MIGRATION_COLLECTION = "migration";
    public static final String AUDIT_COLLECTION = "audit";

    public static final List<String> COLLECTIONS_LIST = Arrays.asList(
            MongoDBAdaptorFactory.ORGANIZATION_COLLECTION,
            USER_COLLECTION,
            STUDY_COLLECTION,
            FILE_COLLECTION,
            JOB_COLLECTION,
            SAMPLE_COLLECTION,
            INDIVIDUAL_COLLECTION,
            COHORT_COLLECTION,
            PANEL_COLLECTION,
            FAMILY_COLLECTION,
            CLINICAL_ANALYSIS_COLLECTION,
            INTERPRETATION_COLLECTION,

            SAMPLE_ARCHIVE_COLLECTION,
            INDIVIDUAL_ARCHIVE_COLLECTION,
            FAMILY_ARCHIVE_COLLECTION,
            PANEL_ARCHIVE_COLLECTION,
            INTERPRETATION_ARCHIVE_COLLECTION,

            DELETED_ORGANIZATION_COLLECTION,
            DELETED_USER_COLLECTION,
            DELETED_STUDY_COLLECTION,
            DELETED_FILE_COLLECTION,
            DELETED_JOB_COLLECTION,
            DELETED_SAMPLE_COLLECTION,
            DELETED_INDIVIDUAL_COLLECTION,
            DELETED_COHORT_COLLECTION,
            DELETED_PANEL_COLLECTION,
            DELETED_FAMILY_COLLECTION,
            DELETED_CLINICAL_ANALYSIS_COLLECTION,
            DELETED_INTERPRETATION_COLLECTION,

            MIGRATION_COLLECTION,
            METADATA_COLLECTION,
            AUDIT_COLLECTION
    );
    static final String METADATA_OBJECT_ID = "METADATA";

    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore mongoDataStore;
    private final String database;

    private final UserMongoDBAdaptor userDBAdaptor;
    private final StudyMongoDBAdaptor studyDBAdaptor;
    private final IndividualMongoDBAdaptor individualDBAdaptor;
    private final SampleMongoDBAdaptor sampleDBAdaptor;
    private final FileMongoDBAdaptor fileDBAdaptor;
    private final JobMongoDBAdaptor jobDBAdaptor;
    private final ProjectMongoDBAdaptor projectDBAdaptor;
    private final CohortMongoDBAdaptor cohortDBAdaptor;
    private final FamilyMongoDBAdaptor familyDBAdaptor;
    private final PanelMongoDBAdaptor panelDBAdaptor;
    private final ClinicalAnalysisMongoDBAdaptor clinicalDBAdaptor;
    private final InterpretationMongoDBAdaptor interpretationDBAdaptor;
    private final AuditMongoDBAdaptor auditDBAdaptor;
    private final MetaMongoDBAdaptor metaDBAdaptor;
    private final MigrationMongoDBAdaptor migrationDBAdaptor;

    private final MongoDBCollection metaCollection;
    private final Map<String, MongoDBCollection> mongoDBCollectionMap;

    private final Logger logger;

    public OrganizationMongoDBAdaptorFactory(MongoDataStoreManager mongoDataStoreManager, MongoDBConfiguration mongoDBConfiguration,
                                             String database, Configuration configuration) throws CatalogDBException {
        logger = LoggerFactory.getLogger(OrganizationMongoDBAdaptorFactory.class);
        this.mongoManager = mongoDataStoreManager;
        this.database = database;

        this.mongoDataStore = mongoManager.get(database, mongoDBConfiguration);
        if (mongoDataStore == null) {
            throw new CatalogDBException("Unable to connect to MongoDB '" + database + "'");
        }

        metaCollection = mongoDataStore.getCollection(METADATA_COLLECTION);
        MongoDBCollection migrationCollection = mongoDataStore.getCollection(MIGRATION_COLLECTION);

        MongoDBCollection userCollection = mongoDataStore.getCollection(USER_COLLECTION);
        MongoDBCollection studyCollection = mongoDataStore.getCollection(STUDY_COLLECTION);
        MongoDBCollection fileCollection = mongoDataStore.getCollection(FILE_COLLECTION);
        MongoDBCollection sampleCollection = mongoDataStore.getCollection(SAMPLE_COLLECTION);
        MongoDBCollection individualCollection = mongoDataStore.getCollection(INDIVIDUAL_COLLECTION);
        MongoDBCollection jobCollection = mongoDataStore.getCollection(JOB_COLLECTION);
        MongoDBCollection cohortCollection = mongoDataStore.getCollection(COHORT_COLLECTION);
        MongoDBCollection panelCollection = mongoDataStore.getCollection(PANEL_COLLECTION);
        MongoDBCollection familyCollection = mongoDataStore.getCollection(FAMILY_COLLECTION);
        MongoDBCollection clinicalCollection = mongoDataStore.getCollection(CLINICAL_ANALYSIS_COLLECTION);
        MongoDBCollection interpretationCollection = mongoDataStore.getCollection(INTERPRETATION_COLLECTION);

        MongoDBCollection sampleArchivedCollection = mongoDataStore.getCollection(SAMPLE_ARCHIVE_COLLECTION);
        MongoDBCollection individualArchivedCollection = mongoDataStore.getCollection(INDIVIDUAL_ARCHIVE_COLLECTION);
        MongoDBCollection familyArchivedCollection = mongoDataStore.getCollection(FAMILY_ARCHIVE_COLLECTION);
        MongoDBCollection panelArchivedCollection = mongoDataStore.getCollection(PANEL_ARCHIVE_COLLECTION);
        MongoDBCollection interpretationArchivedCollection = mongoDataStore.getCollection(INTERPRETATION_ARCHIVE_COLLECTION);

        MongoDBCollection deletedUserCollection = mongoDataStore.getCollection(DELETED_USER_COLLECTION);
        MongoDBCollection deletedStudyCollection = mongoDataStore.getCollection(DELETED_STUDY_COLLECTION);
        MongoDBCollection deletedFileCollection = mongoDataStore.getCollection(DELETED_FILE_COLLECTION);
        MongoDBCollection deletedSampleCollection = mongoDataStore.getCollection(DELETED_SAMPLE_COLLECTION);
        MongoDBCollection deletedIndividualCollection = mongoDataStore.getCollection(DELETED_INDIVIDUAL_COLLECTION);
        MongoDBCollection deletedJobCollection = mongoDataStore.getCollection(DELETED_JOB_COLLECTION);
        MongoDBCollection deletedCohortCollection = mongoDataStore.getCollection(DELETED_COHORT_COLLECTION);
        MongoDBCollection deletedPanelCollection = mongoDataStore.getCollection(DELETED_PANEL_COLLECTION);
        MongoDBCollection deletedFamilyCollection = mongoDataStore.getCollection(DELETED_FAMILY_COLLECTION);
        MongoDBCollection deletedClinicalCollection = mongoDataStore.getCollection(DELETED_CLINICAL_ANALYSIS_COLLECTION);
        MongoDBCollection deletedInterpretationCollection = mongoDataStore.getCollection(DELETED_INTERPRETATION_COLLECTION);

        MongoDBCollection auditCollection = mongoDataStore.getCollection(AUDIT_COLLECTION);

        fileDBAdaptor = new FileMongoDBAdaptor(fileCollection, deletedFileCollection, configuration, this);
        familyDBAdaptor = new FamilyMongoDBAdaptor(familyCollection, familyArchivedCollection, deletedFamilyCollection, configuration,
                this);
        individualDBAdaptor = new IndividualMongoDBAdaptor(individualCollection, individualArchivedCollection, deletedIndividualCollection,
                configuration, this);
        jobDBAdaptor = new JobMongoDBAdaptor(jobCollection, deletedJobCollection, configuration, this);
        projectDBAdaptor = new ProjectMongoDBAdaptor(userCollection, deletedUserCollection, configuration, this);

        sampleDBAdaptor = new SampleMongoDBAdaptor(sampleCollection, sampleArchivedCollection, deletedSampleCollection, configuration,
                this);
        studyDBAdaptor = new StudyMongoDBAdaptor(studyCollection, deletedStudyCollection, configuration, this);
        userDBAdaptor = new UserMongoDBAdaptor(userCollection, deletedUserCollection, configuration, this);
        cohortDBAdaptor = new CohortMongoDBAdaptor(cohortCollection, deletedCohortCollection, configuration, this);
        panelDBAdaptor = new PanelMongoDBAdaptor(panelCollection, panelArchivedCollection, deletedPanelCollection, configuration, this);
        clinicalDBAdaptor = new ClinicalAnalysisMongoDBAdaptor(clinicalCollection, deletedClinicalCollection, configuration, this);
        interpretationDBAdaptor = new InterpretationMongoDBAdaptor(interpretationCollection, interpretationArchivedCollection,
                deletedInterpretationCollection, configuration, this);
        metaDBAdaptor = new MetaMongoDBAdaptor(metaCollection, configuration, this);
        migrationDBAdaptor = new MigrationMongoDBAdaptor(migrationCollection, configuration, this);
        auditDBAdaptor = new AuditMongoDBAdaptor(auditCollection, configuration);

        mongoDBCollectionMap = new HashMap<>();
        mongoDBCollectionMap.put(METADATA_COLLECTION, metaCollection);
        mongoDBCollectionMap.put(MIGRATION_COLLECTION, migrationCollection);

        mongoDBCollectionMap.put(USER_COLLECTION, userCollection);
        mongoDBCollectionMap.put(STUDY_COLLECTION, studyCollection);
        mongoDBCollectionMap.put(FILE_COLLECTION, fileCollection);
        mongoDBCollectionMap.put(SAMPLE_COLLECTION, sampleCollection);
        mongoDBCollectionMap.put(INDIVIDUAL_COLLECTION, individualCollection);
        mongoDBCollectionMap.put(JOB_COLLECTION, jobCollection);
        mongoDBCollectionMap.put(COHORT_COLLECTION, cohortCollection);
        mongoDBCollectionMap.put(PANEL_COLLECTION, panelCollection);
        mongoDBCollectionMap.put(FAMILY_COLLECTION, familyCollection);
        mongoDBCollectionMap.put(CLINICAL_ANALYSIS_COLLECTION, clinicalCollection);
        mongoDBCollectionMap.put(INTERPRETATION_COLLECTION, interpretationCollection);

        mongoDBCollectionMap.put(SAMPLE_ARCHIVE_COLLECTION, sampleArchivedCollection);
        mongoDBCollectionMap.put(INDIVIDUAL_ARCHIVE_COLLECTION, individualArchivedCollection);
        mongoDBCollectionMap.put(FAMILY_ARCHIVE_COLLECTION, familyArchivedCollection);
        mongoDBCollectionMap.put(PANEL_ARCHIVE_COLLECTION, panelArchivedCollection);
        mongoDBCollectionMap.put(INTERPRETATION_ARCHIVE_COLLECTION, interpretationArchivedCollection);

        mongoDBCollectionMap.put(DELETED_USER_COLLECTION, deletedUserCollection);
        mongoDBCollectionMap.put(DELETED_STUDY_COLLECTION, deletedStudyCollection);
        mongoDBCollectionMap.put(DELETED_FILE_COLLECTION, deletedFileCollection);
        mongoDBCollectionMap.put(DELETED_SAMPLE_COLLECTION, deletedSampleCollection);
        mongoDBCollectionMap.put(DELETED_INDIVIDUAL_COLLECTION, deletedIndividualCollection);
        mongoDBCollectionMap.put(DELETED_JOB_COLLECTION, deletedJobCollection);
        mongoDBCollectionMap.put(DELETED_COHORT_COLLECTION, deletedCohortCollection);
        mongoDBCollectionMap.put(DELETED_PANEL_COLLECTION, deletedPanelCollection);
        mongoDBCollectionMap.put(DELETED_FAMILY_COLLECTION, deletedFamilyCollection);
        mongoDBCollectionMap.put(DELETED_CLINICAL_ANALYSIS_COLLECTION, deletedClinicalCollection);
        mongoDBCollectionMap.put(DELETED_INTERPRETATION_COLLECTION, deletedInterpretationCollection);

        mongoDBCollectionMap.put(AUDIT_COLLECTION, auditCollection);
    }

    public boolean isCatalogDBReady() {
        return metaCollection.count(Filters.eq("id", METADATA_OBJECT_ID)).getNumMatches() == 1;
    }

    public void createAllCollections(Configuration configuration) throws CatalogException {
        // TODO: Check META object does not exist. Use {@link isCatalogDBReady}
        // TODO: Check all collections do not exists, or are empty
        // TODO: Catch DuplicatedKeyException while inserting META object

        if (!mongoDataStore.getCollectionNames().isEmpty()) {
            throw new CatalogException("Database " + database + " already exists with the following collections: "
                    + StringUtils.join(mongoDataStore.getCollectionNames()) + ".\nPlease, remove the database or choose a different one.");
        }
        COLLECTIONS_LIST.forEach(mongoDataStore::createCollection);
    }

    public void initialiseMetaCollection(Admin admin) throws CatalogException {
        metaDBAdaptor.initializeMetaCollection(admin);
    }

    public void createIndexes() throws CatalogDBException {
        StopWatch stopWatch = StopWatch.createStarted();
        metaDBAdaptor.createIndexes();
        logger.info("Creating all indexes took {} milliseconds", stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    public Map<String, MongoDBCollection> getMongoDBCollectionMap() {
        return mongoDBCollectionMap;
    }

    public boolean getDatabaseStatus() {
        Document dbStatus = mongoDataStore.getServerStatus();
        try {
            ObjectMap map = new ObjectMap(getDefaultObjectMapper().writeValueAsString(dbStatus));
            return map.getInt("ok", 0) > 0;
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    public void deleteCatalogDB() {
        mongoManager.drop(database);
    }

    public void close() {
        mongoManager.close(database);
    }

    public MigrationMongoDBAdaptor getMigrationDBAdaptor() {
        return migrationDBAdaptor;
    }

    public MetaMongoDBAdaptor getCatalogMetaDBAdaptor() {
        return metaDBAdaptor;
    }

    public UserMongoDBAdaptor getCatalogUserDBAdaptor() {
        return userDBAdaptor;
    }

    public ProjectMongoDBAdaptor getCatalogProjectDbAdaptor() {
        return projectDBAdaptor;
    }

    public StudyMongoDBAdaptor getCatalogStudyDBAdaptor() {
        return studyDBAdaptor;
    }

    public FileMongoDBAdaptor getCatalogFileDBAdaptor() {
        return fileDBAdaptor;
    }

    public SampleMongoDBAdaptor getCatalogSampleDBAdaptor() {
        return sampleDBAdaptor;
    }

    public IndividualMongoDBAdaptor getCatalogIndividualDBAdaptor() {
        return individualDBAdaptor;
    }

    public JobMongoDBAdaptor getCatalogJobDBAdaptor() {
        return jobDBAdaptor;
    }

    public AuditMongoDBAdaptor getCatalogAuditDbAdaptor() {
        return auditDBAdaptor;
    }

    public CohortMongoDBAdaptor getCatalogCohortDBAdaptor() {
        return cohortDBAdaptor;
    }

    public PanelMongoDBAdaptor getCatalogPanelDBAdaptor() {
        return panelDBAdaptor;
    }

    public FamilyMongoDBAdaptor getCatalogFamilyDBAdaptor() {
        return familyDBAdaptor;
    }

    public ClinicalAnalysisMongoDBAdaptor getClinicalAnalysisDBAdaptor() {
        return clinicalDBAdaptor;
    }

    public InterpretationMongoDBAdaptor getInterpretationDBAdaptor() {
        return interpretationDBAdaptor;
    }

    public MongoDataStore getMongoDataStore() {
        return mongoDataStore;
    }
}
