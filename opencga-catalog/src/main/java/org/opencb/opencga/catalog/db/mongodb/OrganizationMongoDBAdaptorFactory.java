package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

public class OrganizationMongoDBAdaptorFactory {

    public static final String NOTE_COLLECTION = "note";
    public static final String ORGANIZATION_COLLECTION = "organization";
    public static final String USER_COLLECTION = "user";
    public static final String PROJECT_COLLECTION = "project";
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

    public static final String NOTE_ARCHIVE_COLLECTION = "note_archive";
    public static final String SAMPLE_ARCHIVE_COLLECTION = "sample_archive";
    public static final String INDIVIDUAL_ARCHIVE_COLLECTION = "individual_archive";
    public static final String FAMILY_ARCHIVE_COLLECTION = "family_archive";
    public static final String PANEL_ARCHIVE_COLLECTION = "panel_archive";
    public static final String CLINICAL_ANALYSIS_ARCHIVE_COLLECTION = "clinical_archive";
    public static final String INTERPRETATION_ARCHIVE_COLLECTION = "interpretation_archive";

    public static final String DELETED_NOTE_COLLECTION = "note_deleted";
    public static final String DELETED_USER_COLLECTION = "user_deleted";
    public static final String DELETED_PROJECT_COLLECTION = "project_deleted";
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
            NOTE_COLLECTION,
            ORGANIZATION_COLLECTION,
            USER_COLLECTION,
            PROJECT_COLLECTION,
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

            NOTE_ARCHIVE_COLLECTION,
            SAMPLE_ARCHIVE_COLLECTION,
            INDIVIDUAL_ARCHIVE_COLLECTION,
            FAMILY_ARCHIVE_COLLECTION,
            PANEL_ARCHIVE_COLLECTION,
            CLINICAL_ANALYSIS_ARCHIVE_COLLECTION,
            INTERPRETATION_ARCHIVE_COLLECTION,

            DELETED_NOTE_COLLECTION,
            DELETED_USER_COLLECTION,
            DELETED_PROJECT_COLLECTION,
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
            // FIXME metadata collection is unused
            METADATA_COLLECTION,
            AUDIT_COLLECTION
    );
    static final String METADATA_OBJECT_ID = "METADATA";

    private final MongoDataStoreManager mongoManager;
    private final MongoDataStore mongoDataStore;
    private final String organizationId;
    private final String database;

    private final NoteMongoDBAdaptor notesDBAdaptor;
    private final OrganizationMongoDBAdaptor organizationDBAdaptor;
    private UserMongoDBAdaptor userDBAdaptor;
    private final ProjectMongoDBAdaptor projectDBAdaptor;
    private final StudyMongoDBAdaptor studyDBAdaptor;
    private final IndividualMongoDBAdaptor individualDBAdaptor;
    private final SampleMongoDBAdaptor sampleDBAdaptor;
    private final FileMongoDBAdaptor fileDBAdaptor;
    private final JobMongoDBAdaptor jobDBAdaptor;
    private final CohortMongoDBAdaptor cohortDBAdaptor;
    private final FamilyMongoDBAdaptor familyDBAdaptor;
    private final PanelMongoDBAdaptor panelDBAdaptor;
    private final ClinicalAnalysisMongoDBAdaptor clinicalDBAdaptor;
    private final InterpretationMongoDBAdaptor interpretationDBAdaptor;
    private final AuditMongoDBAdaptor auditDBAdaptor;
//    private final MetaMongoDBAdaptor metaDBAdaptor;
    private final MigrationMongoDBAdaptor migrationDBAdaptor;
    private final AuthorizationMongoDBAdaptor authorizationMongoDBAdaptor;

    private final MongoDBCollection organizationCollection;
    private final Map<String, MongoDBCollection> mongoDBCollectionMap;

    private final Logger logger;

    public OrganizationMongoDBAdaptorFactory(String organizationId, MongoDataStoreManager mongoDataStoreManager,
                                             MongoDBConfiguration mongoDBConfiguration, Configuration configuration,
                                             IOManagerFactory ioManagerFactory) throws CatalogDBException {
        logger = LoggerFactory.getLogger(OrganizationMongoDBAdaptorFactory.class);
        this.mongoManager = mongoDataStoreManager;
        this.organizationId = organizationId;
        this.database = getCatalogOrganizationDatabase(configuration.getDatabasePrefix(), organizationId);

        this.mongoDataStore = mongoManager.get(database, mongoDBConfiguration);
        if (mongoDataStore == null) {
            throw new CatalogDBException("Unable to connect to MongoDB '" + database + "'");
        }

        MongoDBCollection migrationCollection = mongoDataStore.getCollection(MIGRATION_COLLECTION);

        organizationCollection = mongoDataStore.getCollection(ORGANIZATION_COLLECTION);
        MongoDBCollection notesCollection = mongoDataStore.getCollection(NOTE_COLLECTION);
        MongoDBCollection userCollection = mongoDataStore.getCollection(USER_COLLECTION);
        MongoDBCollection projectCollection = mongoDataStore.getCollection(PROJECT_COLLECTION);
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

        MongoDBCollection notesArchivedCollection = mongoDataStore.getCollection(NOTE_ARCHIVE_COLLECTION);
        MongoDBCollection sampleArchivedCollection = mongoDataStore.getCollection(SAMPLE_ARCHIVE_COLLECTION);
        MongoDBCollection individualArchivedCollection = mongoDataStore.getCollection(INDIVIDUAL_ARCHIVE_COLLECTION);
        MongoDBCollection familyArchivedCollection = mongoDataStore.getCollection(FAMILY_ARCHIVE_COLLECTION);
        MongoDBCollection panelArchivedCollection = mongoDataStore.getCollection(PANEL_ARCHIVE_COLLECTION);
        MongoDBCollection clinicalArchivedCollection = mongoDataStore.getCollection(CLINICAL_ANALYSIS_ARCHIVE_COLLECTION);
        MongoDBCollection interpretationArchivedCollection = mongoDataStore.getCollection(INTERPRETATION_ARCHIVE_COLLECTION);

        MongoDBCollection deletedNotesCollection = mongoDataStore.getCollection(DELETED_NOTE_COLLECTION);
        MongoDBCollection deletedUserCollection = mongoDataStore.getCollection(DELETED_USER_COLLECTION);
        MongoDBCollection deletedProjectCollection = mongoDataStore.getCollection(DELETED_PROJECT_COLLECTION);
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

        notesDBAdaptor = new NoteMongoDBAdaptor(notesCollection, notesArchivedCollection, deletedNotesCollection,
                configuration, this);
        organizationDBAdaptor = new OrganizationMongoDBAdaptor(organizationCollection, configuration, this);
        fileDBAdaptor = new FileMongoDBAdaptor(fileCollection, deletedFileCollection, configuration, this, ioManagerFactory);
        familyDBAdaptor = new FamilyMongoDBAdaptor(familyCollection, familyArchivedCollection, deletedFamilyCollection, configuration,
                this);
        individualDBAdaptor = new IndividualMongoDBAdaptor(individualCollection, individualArchivedCollection, deletedIndividualCollection,
                configuration, this);
        jobDBAdaptor = new JobMongoDBAdaptor(jobCollection, deletedJobCollection, configuration, this);
        projectDBAdaptor = new ProjectMongoDBAdaptor(projectCollection, deletedProjectCollection, configuration, this);

        sampleDBAdaptor = new SampleMongoDBAdaptor(sampleCollection, sampleArchivedCollection, deletedSampleCollection, configuration,
                this);
        studyDBAdaptor = new StudyMongoDBAdaptor(studyCollection, deletedStudyCollection, configuration, this);
        userDBAdaptor = new UserMongoDBAdaptor(userCollection, deletedUserCollection, configuration, this);
        cohortDBAdaptor = new CohortMongoDBAdaptor(cohortCollection, deletedCohortCollection, configuration, this);
        panelDBAdaptor = new PanelMongoDBAdaptor(panelCollection, panelArchivedCollection, deletedPanelCollection, configuration, this);
        clinicalDBAdaptor = new ClinicalAnalysisMongoDBAdaptor(clinicalCollection, clinicalArchivedCollection, deletedClinicalCollection,
                configuration, this);
        interpretationDBAdaptor = new InterpretationMongoDBAdaptor(interpretationCollection, interpretationArchivedCollection,
                deletedInterpretationCollection, configuration, this);
//        metaDBAdaptor = new MetaMongoDBAdaptor(metaCollection, configuration, this);
        migrationDBAdaptor = new MigrationMongoDBAdaptor(migrationCollection, configuration, this);
        auditDBAdaptor = new AuditMongoDBAdaptor(auditCollection, configuration);
        authorizationMongoDBAdaptor = new AuthorizationMongoDBAdaptor(this, configuration);

        mongoDBCollectionMap = new HashMap<>();
//        mongoDBCollectionMap.put(METADATA_COLLECTION, metaCollection);
        mongoDBCollectionMap.put(MIGRATION_COLLECTION, migrationCollection);

        mongoDBCollectionMap.put(NOTE_COLLECTION, notesCollection);
        mongoDBCollectionMap.put(ORGANIZATION_COLLECTION, organizationCollection);
        mongoDBCollectionMap.put(PROJECT_COLLECTION, projectCollection);
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

        mongoDBCollectionMap.put(NOTE_ARCHIVE_COLLECTION, notesArchivedCollection);
        mongoDBCollectionMap.put(SAMPLE_ARCHIVE_COLLECTION, sampleArchivedCollection);
        mongoDBCollectionMap.put(INDIVIDUAL_ARCHIVE_COLLECTION, individualArchivedCollection);
        mongoDBCollectionMap.put(FAMILY_ARCHIVE_COLLECTION, familyArchivedCollection);
        mongoDBCollectionMap.put(PANEL_ARCHIVE_COLLECTION, panelArchivedCollection);
        mongoDBCollectionMap.put(INTERPRETATION_ARCHIVE_COLLECTION, interpretationArchivedCollection);

        mongoDBCollectionMap.put(DELETED_NOTE_COLLECTION, deletedNotesCollection);
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
        return organizationCollection.count(new Document()).getNumMatches() == 1;
    }

//    public void createAllCollections(Configuration configuration) throws CatalogException {
//        // TODO: Check META object does not exist. Use {@link isCatalogDBReady}
//        // TODO: Check all collections do not exists, or are empty
//        // TODO: Catch DuplicatedKeyException while inserting META object
//
//        if (!mongoDataStore.getCollectionNames().isEmpty()) {
//            throw new CatalogException("Database " + database + " already exists with the following collections: "
//                   + StringUtils.join(mongoDataStore.getCollectionNames()) + ".\nPlease, remove the database or choose a different one.");
//        }
//        COLLECTIONS_LIST.forEach(mongoDataStore::createCollection);
//    }

    public void initialiseMetaCollection(Admin admin) throws CatalogException {
        throw new CatalogException("Initialise meta collection must disappear");
//        metaDBAdaptor.initializeMetaCollection(admin);
    }

    public void createAllCollections() throws CatalogDBException {
        if (!mongoDataStore.getCollectionNames().isEmpty()) {
            throw new CatalogDBException("Database " + mongoDataStore.getDatabaseName() + " already exists with the following "
                    + "collections: " + StringUtils.join(mongoDataStore.getCollectionNames()) + ".\nPlease, remove the database or"
                    + " choose a different one.");
        }
        OrganizationMongoDBAdaptorFactory.COLLECTIONS_LIST.forEach(mongoDataStore::createCollection);
    }

    public void createIndexes() throws CatalogDBException {
        StopWatch stopWatch = StopWatch.createStarted();

        InputStream resourceAsStream = getClass().getResourceAsStream("/catalog-indexes.txt");
        ObjectMapper objectMapper = getDefaultObjectMapper();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resourceAsStream));
        // We store all the indexes that are in the file in the indexes object
        Map<String, List<Map<String, ObjectMap>>> indexes = new HashMap<>();
        bufferedReader.lines().filter(s -> !s.trim().isEmpty()).forEach(s -> {
            try {
                HashMap hashMap = objectMapper.readValue(s, HashMap.class);

                List<String> collections = (List<String>) hashMap.get("collections");
                for (String collection : collections) {
                    if (!indexes.containsKey(collection)) {
                        indexes.put(collection, new ArrayList<>());
                    }
                    Map<String, ObjectMap> myIndexes = new HashMap<>();
                    myIndexes.put("fields", new ObjectMap((Map) hashMap.get("fields")));
                    myIndexes.put("options", new ObjectMap((Map) hashMap.getOrDefault("options", Collections.emptyMap())));
                    indexes.get(collection).add(myIndexes);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        try {
            bufferedReader.close();
        } catch (IOException e) {
            logger.error("Error closing the buffer reader", e);
            throw new UncheckedIOException(e);
        }

        createIndexes(OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.USER_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.FILE_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.COHORT_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.AUDIT_COLLECTION, indexes);

        // Versioned collections
        createIndexes(OrganizationMongoDBAdaptorFactory.NOTE_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.NOTE_ARCHIVE_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.SAMPLE_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.SAMPLE_ARCHIVE_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.INDIVIDUAL_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.INDIVIDUAL_ARCHIVE_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.FAMILY_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.FAMILY_ARCHIVE_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.PANEL_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.PANEL_ARCHIVE_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.INTERPRETATION_COLLECTION, indexes);
        createIndexes(OrganizationMongoDBAdaptorFactory.INTERPRETATION_ARCHIVE_COLLECTION, indexes);

        logger.info("Creating all indexes took {} milliseconds", stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    private void createIndexes(String collection, Map<String, List<Map<String, ObjectMap>>> indexCollectionMap) {
        MongoDBCollection mongoCollection = mongoDBCollectionMap.get(collection);
        List<Map<String, ObjectMap>> indexes = indexCollectionMap.get(collection);

        DataResult<Document> index = mongoCollection.getIndex();
        // We store the existing indexes
        Set<String> existingIndexes = index.getResults()
                .stream()
                .map(document -> (String) document.get("name"))
                .collect(Collectors.toSet());

        if (index.getNumResults() != indexes.size() + 1) { // It is + 1 because mongo always create the _id index by default
            for (Map<String, ObjectMap> userIndex : indexes) {
                String indexName = "";
                Document keys = new Document();
                Iterator fieldsIterator = userIndex.get("fields").entrySet().iterator();
                while (fieldsIterator.hasNext()) {
                    Map.Entry pair = (Map.Entry) fieldsIterator.next();
                    keys.append((String) pair.getKey(), pair.getValue());

                    if (!indexName.isEmpty()) {
                        indexName += "_";
                    }
                    indexName += pair.getKey() + "_" + pair.getValue();
                }

                if (!existingIndexes.contains(indexName)) {
                    mongoCollection.createIndex(keys, new ObjectMap(userIndex.get("options")));
                }
            }
        }
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

    public String getOrganizationId() {
        return organizationId;
    }

    public MigrationMongoDBAdaptor getMigrationDBAdaptor() {
        return migrationDBAdaptor;
    }

    public MetaMongoDBAdaptor getCatalogMetaDBAdaptor() {
        return null;
    }

    public NoteMongoDBAdaptor getCatalogNotesDBAdaptor() {
        return notesDBAdaptor;
    }

    public OrganizationMongoDBAdaptor getCatalogOrganizationDBAdaptor() {
        return organizationDBAdaptor;
    }

    public UserMongoDBAdaptor getCatalogUserDBAdaptor() {
        return userDBAdaptor;
    }

    public ProjectMongoDBAdaptor getCatalogProjectDBAdaptor() {
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

    public AuthorizationMongoDBAdaptor getAuthorizationDBAdaptor() {
        return authorizationMongoDBAdaptor;
    }

    public MongoDataStore getMongoDataStore() {
        return mongoDataStore;
    }

    private String getCatalogOrganizationDatabase(String prefix, String organization) {
        String dbPrefix = StringUtils.isEmpty(prefix) ? "opencga" : prefix;
        dbPrefix = dbPrefix.endsWith("_") ? dbPrefix : dbPrefix + "_";
        return (dbPrefix + "catalog_" + organization).toLowerCase();
    }
}
