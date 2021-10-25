/*
 * Copyright 2015-2020 OpenCB
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
import com.mongodb.BasicDBObject;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.MigrationDBAdaptor;
import org.opencb.opencga.catalog.db.api.PipelineDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by pfurio on 08/01/16.
 */
public class MongoDBAdaptorFactory implements DBAdaptorFactory {

    private final List<String> COLLECTIONS_LIST = Arrays.asList(
            USER_COLLECTION,
            STUDY_COLLECTION,
            FILE_COLLECTION,
            EXECUTION_COLLECTION,
            JOB_COLLECTION,
            SAMPLE_COLLECTION,
            INDIVIDUAL_COLLECTION,
            COHORT_COLLECTION,
            PANEL_COLLECTION,
            FAMILY_COLLECTION,
            CLINICAL_ANALYSIS_COLLECTION,
            INTERPRETATION_COLLECTION,
            PIPELINE_COLLECTION,

            DELETED_USER_COLLECTION,
            DELETED_STUDY_COLLECTION,
            DELETED_FILE_COLLECTION,
            DELETED_EXECUTION_COLLECTION,
            DELETED_JOB_COLLECTION,
            DELETED_SAMPLE_COLLECTION,
            DELETED_INDIVIDUAL_COLLECTION,
            DELETED_COHORT_COLLECTION,
            DELETED_PANEL_COLLECTION,
            DELETED_FAMILY_COLLECTION,
            DELETED_CLINICAL_ANALYSIS_COLLECTION,
            DELETED_INTERPRETATION_COLLECTION,
            DELETED_PIPELINE_COLLECTION,

            MIGRATION_COLLECTION,
            METADATA_COLLECTION,
            AUDIT_COLLECTION
    );

    public static final String USER_COLLECTION = "user";
    public static final String STUDY_COLLECTION = "study";
    public static final String FILE_COLLECTION = "file";
    public static final String EXECUTION_COLLECTION = "execution";
    public static final String JOB_COLLECTION = "job";
    public static final String SAMPLE_COLLECTION = "sample";
    public static final String INDIVIDUAL_COLLECTION = "individual";
    public static final String COHORT_COLLECTION = "cohort";
    public static final String FAMILY_COLLECTION = "family";
    public static final String PANEL_COLLECTION = "panel";
    public static final String CLINICAL_ANALYSIS_COLLECTION = "clinical";
    public static final String INTERPRETATION_COLLECTION = "interpretation";
    public static final String PIPELINE_COLLECTION = "study_pipeline";

    public static final String DELETED_USER_COLLECTION = "deleted_user";
    public static final String DELETED_STUDY_COLLECTION = "deleted_study";
    public static final String DELETED_FILE_COLLECTION = "deleted_file";
    public static final String DELETED_EXECUTION_COLLECTION = "deleted_execution";
    public static final String DELETED_JOB_COLLECTION = "deleted_job";
    public static final String DELETED_SAMPLE_COLLECTION = "deleted_sample";
    public static final String DELETED_INDIVIDUAL_COLLECTION = "deleted_individual";
    public static final String DELETED_COHORT_COLLECTION = "deleted_cohort";
    public static final String DELETED_FAMILY_COLLECTION = "deleted_family";
    public static final String DELETED_PANEL_COLLECTION = "deleted_panel";
    public static final String DELETED_CLINICAL_ANALYSIS_COLLECTION = "deleted_clinical";
    public static final String DELETED_INTERPRETATION_COLLECTION = "deleted_interpretation";
    public static final String DELETED_PIPELINE_COLLECTION = "deleted_study_pipeline";

    public static final String METADATA_COLLECTION = "metadata";
    public static final String MIGRATION_COLLECTION = "migration";
    public static final String AUDIT_COLLECTION = "audit";
    static final String METADATA_OBJECT_ID = "METADATA";
    private final MongoDataStoreManager mongoManager;
    private final MongoDBConfiguration configuration;
    private final String database;
    private MongoDataStore mongoDataStore;

    private MongoDBCollection metaCollection;
    private Map<String, MongoDBCollection> collections;
    private UserMongoDBAdaptor userDBAdaptor;
    private StudyMongoDBAdaptor studyDBAdaptor;
    private IndividualMongoDBAdaptor individualDBAdaptor;
    private SampleMongoDBAdaptor sampleDBAdaptor;
    private FileMongoDBAdaptor fileDBAdaptor;
    private ExecutionMongoDBAdaptor executionDBAdaptor;
    private JobMongoDBAdaptor jobDBAdaptor;
    private ProjectMongoDBAdaptor projectDBAdaptor;
    private CohortMongoDBAdaptor cohortDBAdaptor;
    private FamilyMongoDBAdaptor familyDBAdaptor;
    private PanelMongoDBAdaptor panelDBAdaptor;
    private ClinicalAnalysisMongoDBAdaptor clinicalDBAdaptor;
    private InterpretationMongoDBAdaptor interpretationDBAdaptor;
    private PipelineMongoDBAdaptor pipelineDBAdaptor;
    private AuditMongoDBAdaptor auditDBAdaptor;
    private MetaMongoDBAdaptor metaDBAdaptor;
    private MigrationMongoDBAdaptor migrationDBAdaptor;

    private Logger logger;

    public MongoDBAdaptorFactory(Configuration catalogConfiguration) throws CatalogDBException {
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .setUserPassword(
                        catalogConfiguration.getCatalog().getDatabase().getUser(),
                        catalogConfiguration.getCatalog().getDatabase().getPassword())
                .load(catalogConfiguration.getCatalog().getDatabase().getOptions())
                .build();

        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String host : catalogConfiguration.getCatalog().getDatabase().getHosts()) {
            if (host.contains(":")) {
                String[] hostAndPort = host.split(":");
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(host, 27017));
            }
        }

        this.mongoManager = new MongoDataStoreManager(dataStoreServerAddresses);
        this.configuration = mongoDBConfiguration;
        this.database = getCatalogDatabase(catalogConfiguration.getDatabasePrefix());

        logger = LoggerFactory.getLogger(this.getClass());
        connect(catalogConfiguration);
    }

    @Override
    public void installCatalogDB(Configuration configuration) throws CatalogException {
        // TODO: Check META object does not exist. Use {@link isCatalogDBReady}
        // TODO: Check all collections do not exists, or are empty
        // TODO: Catch DuplicatedKeyException while inserting META object

        MongoDataStore mongoDataStore = mongoManager.get(database, this.configuration);
        if (!mongoDataStore.getCollectionNames().isEmpty()) {
            throw new CatalogException("Database " + database + " already exists with the following collections: "
                    + StringUtils.join(mongoDataStore.getCollectionNames()) + ".\nPlease, remove the database or choose a different one.");
        }
        COLLECTIONS_LIST.forEach(mongoDataStore::createCollection);
        metaDBAdaptor.initializeMetaCollection(configuration);
    }

    @Override
    public void createIndexes(boolean uniqueIndexesOnly) throws CatalogDBException {
        metaDBAdaptor.createIndexes(uniqueIndexesOnly);
    }

    @Override
    public boolean getDatabaseStatus() {
        Document dbStatus = mongoManager.get(database, this.configuration).getServerStatus();
        try {
            ObjectMap map = new ObjectMap(getDefaultObjectMapper().writeValueAsString(dbStatus));
            return map.getInt("ok", 0) > 0;
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public String getCatalogDatabase(String prefix) {
        String database;
        if (StringUtils.isNotEmpty(prefix)) {
            if (!prefix.endsWith("_")) {
                database = prefix + "_catalog";
            } else {
                database = prefix + "catalog";
            }
        } else {
            database = "opencga_catalog";
        }
        return database;
    }

    @Override
    public void deleteCatalogDB() throws CatalogDBException {
        mongoManager.drop(database);
    }

    @Override
    public boolean isCatalogDBReady() {
        return metaCollection.count(new BasicDBObject("id", METADATA_OBJECT_ID)).getNumMatches() == 1;
    }

    @Override
    public void close() {
        mongoManager.close(mongoDataStore.getDatabaseName());
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
    public ExecutionMongoDBAdaptor getCatalogExecutionDBAdaptor() {
        return executionDBAdaptor;
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
    public PanelMongoDBAdaptor getCatalogPanelDBAdaptor() {
        return panelDBAdaptor;
    }

    @Override
    public FamilyMongoDBAdaptor getCatalogFamilyDBAdaptor() {
        return familyDBAdaptor;
    }

    @Override
    public ClinicalAnalysisMongoDBAdaptor getClinicalAnalysisDBAdaptor() {
        return clinicalDBAdaptor;
    }

    @Override
    public InterpretationMongoDBAdaptor getInterpretationDBAdaptor() {
        return interpretationDBAdaptor;
    }

    @Override
    public PipelineDBAdaptor getPipelineDBAdaptor() {
        return pipelineDBAdaptor;
    }

    @Override
    public MigrationDBAdaptor getMigrationDBAdaptor() {
        return migrationDBAdaptor;
    }

    @Override
    public Map<String, MongoDBCollection> getMongoDBCollectionMap() {
        return collections;
    }

    @Override
    public AuditMongoDBAdaptor getCatalogAuditDbAdaptor() {
        return auditDBAdaptor;
    }

    public MongoDataStore getMongoDataStore() {
        return mongoDataStore;
    }

    private void connect(Configuration catalogConfiguration) throws CatalogDBException {
        mongoDataStore = mongoManager.get(database, configuration);
        if (mongoDataStore == null) {
            throw new CatalogDBException("Unable to connect to MongoDB");
        }

        metaCollection = mongoDataStore.getCollection(METADATA_COLLECTION);
        MongoDBCollection migrationCollection = mongoDataStore.getCollection(MIGRATION_COLLECTION);

        MongoDBCollection userCollection = mongoDataStore.getCollection(USER_COLLECTION);
        MongoDBCollection studyCollection = mongoDataStore.getCollection(STUDY_COLLECTION);
        MongoDBCollection fileCollection = mongoDataStore.getCollection(FILE_COLLECTION);
        MongoDBCollection sampleCollection = mongoDataStore.getCollection(SAMPLE_COLLECTION);
        MongoDBCollection individualCollection = mongoDataStore.getCollection(INDIVIDUAL_COLLECTION);
        MongoDBCollection executionCollection = mongoDataStore.getCollection(EXECUTION_COLLECTION);
        MongoDBCollection jobCollection = mongoDataStore.getCollection(JOB_COLLECTION);
        MongoDBCollection pipelineCollection = mongoDataStore.getCollection(PIPELINE_COLLECTION);
        MongoDBCollection cohortCollection = mongoDataStore.getCollection(COHORT_COLLECTION);
        MongoDBCollection panelCollection = mongoDataStore.getCollection(PANEL_COLLECTION);
        MongoDBCollection familyCollection = mongoDataStore.getCollection(FAMILY_COLLECTION);
        MongoDBCollection clinicalCollection = mongoDataStore.getCollection(CLINICAL_ANALYSIS_COLLECTION);
        MongoDBCollection interpretationCollection = mongoDataStore.getCollection(INTERPRETATION_COLLECTION);

        MongoDBCollection deletedUserCollection = mongoDataStore.getCollection(DELETED_USER_COLLECTION);
        MongoDBCollection deletedStudyCollection = mongoDataStore.getCollection(DELETED_STUDY_COLLECTION);
        MongoDBCollection deletedFileCollection = mongoDataStore.getCollection(DELETED_FILE_COLLECTION);
        MongoDBCollection deletedSampleCollection = mongoDataStore.getCollection(DELETED_SAMPLE_COLLECTION);
        MongoDBCollection deletedIndividualCollection = mongoDataStore.getCollection(DELETED_INDIVIDUAL_COLLECTION);
        MongoDBCollection deletedExecutionCollection = mongoDataStore.getCollection(DELETED_EXECUTION_COLLECTION);
        MongoDBCollection deletedJobCollection = mongoDataStore.getCollection(DELETED_JOB_COLLECTION);
        MongoDBCollection deletedPipelineCollection = mongoDataStore.getCollection(DELETED_PIPELINE_COLLECTION);
        MongoDBCollection deletedCohortCollection = mongoDataStore.getCollection(DELETED_COHORT_COLLECTION);
        MongoDBCollection deletedPanelCollection = mongoDataStore.getCollection(DELETED_PANEL_COLLECTION);
        MongoDBCollection deletedFamilyCollection = mongoDataStore.getCollection(DELETED_FAMILY_COLLECTION);
        MongoDBCollection deletedClinicalCollection = mongoDataStore.getCollection(DELETED_CLINICAL_ANALYSIS_COLLECTION);
        MongoDBCollection deletedInterpretationCollection = mongoDataStore.getCollection(DELETED_INTERPRETATION_COLLECTION);

        MongoDBCollection auditCollection = mongoDataStore.getCollection(AUDIT_COLLECTION);

        collections = new HashMap<>();
        collections.put(METADATA_COLLECTION, metaCollection);
        collections.put(MIGRATION_COLLECTION, migrationCollection);

        collections.put(USER_COLLECTION, userCollection);
        collections.put(STUDY_COLLECTION, studyCollection);
        collections.put(FILE_COLLECTION, fileCollection);
        collections.put(SAMPLE_COLLECTION, sampleCollection);
        collections.put(INDIVIDUAL_COLLECTION, individualCollection);
        collections.put(EXECUTION_COLLECTION, executionCollection);
        collections.put(JOB_COLLECTION, jobCollection);
        collections.put(PIPELINE_COLLECTION, pipelineCollection);
        collections.put(COHORT_COLLECTION, cohortCollection);
        collections.put(PANEL_COLLECTION, panelCollection);
        collections.put(FAMILY_COLLECTION, familyCollection);
        collections.put(CLINICAL_ANALYSIS_COLLECTION, clinicalCollection);
        collections.put(INTERPRETATION_COLLECTION, interpretationCollection);

        collections.put(DELETED_USER_COLLECTION, deletedUserCollection);
        collections.put(DELETED_STUDY_COLLECTION, deletedStudyCollection);
        collections.put(DELETED_FILE_COLLECTION, deletedFileCollection);
        collections.put(DELETED_SAMPLE_COLLECTION, deletedSampleCollection);
        collections.put(DELETED_INDIVIDUAL_COLLECTION, deletedIndividualCollection);
        collections.put(DELETED_EXECUTION_COLLECTION, deletedExecutionCollection);
        collections.put(DELETED_JOB_COLLECTION, deletedJobCollection);
        collections.put(DELETED_PIPELINE_COLLECTION, deletedPipelineCollection);
        collections.put(DELETED_COHORT_COLLECTION, deletedCohortCollection);
        collections.put(DELETED_PANEL_COLLECTION, deletedPanelCollection);
        collections.put(DELETED_FAMILY_COLLECTION, deletedFamilyCollection);
        collections.put(DELETED_CLINICAL_ANALYSIS_COLLECTION, deletedClinicalCollection);
        collections.put(DELETED_INTERPRETATION_COLLECTION, deletedInterpretationCollection);

        collections.put(AUDIT_COLLECTION, auditCollection);

        fileDBAdaptor = new FileMongoDBAdaptor(fileCollection, deletedFileCollection, catalogConfiguration, this);
        individualDBAdaptor = new IndividualMongoDBAdaptor(individualCollection, deletedIndividualCollection, catalogConfiguration, this);
        executionDBAdaptor = new ExecutionMongoDBAdaptor(executionCollection, deletedExecutionCollection, catalogConfiguration, this);
        jobDBAdaptor = new JobMongoDBAdaptor(jobCollection, deletedJobCollection, catalogConfiguration, this);
        projectDBAdaptor = new ProjectMongoDBAdaptor(userCollection, deletedUserCollection, catalogConfiguration, this);
        sampleDBAdaptor = new SampleMongoDBAdaptor(sampleCollection, deletedSampleCollection, catalogConfiguration, this);
        studyDBAdaptor = new StudyMongoDBAdaptor(studyCollection, deletedStudyCollection, catalogConfiguration, this);
        userDBAdaptor = new UserMongoDBAdaptor(userCollection, deletedUserCollection, catalogConfiguration, this);
        cohortDBAdaptor = new CohortMongoDBAdaptor(cohortCollection, deletedCohortCollection, catalogConfiguration, this);
        panelDBAdaptor = new PanelMongoDBAdaptor(panelCollection, deletedPanelCollection, catalogConfiguration, this);
        familyDBAdaptor = new FamilyMongoDBAdaptor(familyCollection, deletedFamilyCollection, catalogConfiguration, this);
        clinicalDBAdaptor = new ClinicalAnalysisMongoDBAdaptor(clinicalCollection, deletedClinicalCollection, catalogConfiguration, this);
        interpretationDBAdaptor = new InterpretationMongoDBAdaptor(interpretationCollection, deletedInterpretationCollection,
                catalogConfiguration, this);
        pipelineDBAdaptor = new PipelineMongoDBAdaptor(pipelineCollection, deletedPipelineCollection, catalogConfiguration, this);
        metaDBAdaptor = new MetaMongoDBAdaptor(metaCollection, catalogConfiguration, this);
        auditDBAdaptor = new AuditMongoDBAdaptor(auditCollection, catalogConfiguration);
        migrationDBAdaptor = new MigrationMongoDBAdaptor(migrationCollection, catalogConfiguration, this);
    }

}
