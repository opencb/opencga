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
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.NoteManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.OrganizationSummary;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.models.notes.NoteCreateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by pfurio on 08/01/16.
 */
public class MongoDBAdaptorFactory implements DBAdaptorFactory {


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

    @Deprecated
    public static final String OLD_DELETED_USER_COLLECTION = "deleted_user";
    @Deprecated
    public static final String OLD_DELETED_STUDY_COLLECTION = "deleted_study";
    @Deprecated
    public static final String OLD_DELETED_FILE_COLLECTION = "deleted_file";
    @Deprecated
    public static final String OLD_DELETED_JOB_COLLECTION = "deleted_job";
    @Deprecated
    public static final String OLD_DELETED_SAMPLE_COLLECTION = "deleted_sample";
    @Deprecated
    public static final String OLD_DELETED_INDIVIDUAL_COLLECTION = "deleted_individual";
    @Deprecated
    public static final String OLD_DELETED_COHORT_COLLECTION = "deleted_cohort";
    @Deprecated
    public static final String OLD_DELETED_FAMILY_COLLECTION = "deleted_family";
    @Deprecated
    public static final String OLD_DELETED_PANEL_COLLECTION = "deleted_panel";
    @Deprecated
    public static final String OLD_DELETED_CLINICAL_ANALYSIS_COLLECTION = "deleted_clinical";
    @Deprecated
    public static final String OLD_DELETED_INTERPRETATION_COLLECTION = "deleted_interpretation";

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
    private final IOManagerFactory ioManagerFactory;
    private final MongoDataStoreManager mongoManager;
    private final MongoDBConfiguration mongoDbConfiguration;
    private final Configuration configuration;

    private static final String ORGANIZATION_PREFIX = "ORG_";
    private enum OrganizationTag {
        ACTIVE,
        SUSPENDED, // owner action
        INACTIVE,  // ADMINISTRATOR
        DELETED
    }

    private Map<String, OrganizationMongoDBAdaptorFactory> organizationDBAdaptorMap;

    private final Logger logger;

    public MongoDBAdaptorFactory(Configuration catalogConfiguration, IOManagerFactory ioManagerFactory) throws CatalogDBException {
        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String host : catalogConfiguration.getCatalog().getDatabase().getHosts()) {
            if (host.contains(":")) {
                String[] hostAndPort = host.split(":");
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(host, 27017));
            }
        }

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .setUserPassword(
                        catalogConfiguration.getCatalog().getDatabase().getUser(),
                        catalogConfiguration.getCatalog().getDatabase().getPassword())
                .setConnectionsPerHost(200)
                .setServerAddress(dataStoreServerAddresses)
                .load(catalogConfiguration.getCatalog().getDatabase().getOptions())
                .build();

        this.mongoManager = new MongoDataStoreManager(dataStoreServerAddresses);
        this.mongoDbConfiguration = mongoDBConfiguration;
        this.configuration = catalogConfiguration;
        this.ioManagerFactory = ioManagerFactory;

        logger = LoggerFactory.getLogger(this.getClass());
        connect(catalogConfiguration);
    }

    @Override
    public void createAllCollections(Configuration configuration) throws CatalogException {
        for (OrganizationMongoDBAdaptorFactory orgFactory : organizationDBAdaptorMap.values()) {
            createAllCollections(orgFactory);
        }
    }

    private void createAllCollections(OrganizationMongoDBAdaptorFactory organizationFactory) throws CatalogDBException {
        MongoDataStore mongoDataStore = organizationFactory.getMongoDataStore();
        if (!mongoDataStore.getCollectionNames().isEmpty()) {
            throw new CatalogDBException("Database " + mongoDataStore.getDatabaseName() + " already exists with the following "
                    + "collections: " + StringUtils.join(mongoDataStore.getCollectionNames()) + ".\nPlease, remove the database or"
                    + " choose a different one.");
        }
        OrganizationMongoDBAdaptorFactory.COLLECTIONS_LIST.forEach(mongoDataStore::createCollection);
    }

    @Override
    public void initialiseMetaCollection(Admin admin) throws CatalogException {
        for (OrganizationMongoDBAdaptorFactory dbAdaptorFactory : organizationDBAdaptorMap.values()) {
            dbAdaptorFactory.initialiseMetaCollection(admin);
        }
    }

    @Override
    public boolean getDatabaseStatus() throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION).getDatabaseStatus();
    }

    @Override
    public void deleteCatalogDB() {
        for (OrganizationMongoDBAdaptorFactory dbAdaptorFactory : organizationDBAdaptorMap.values()) {
            dbAdaptorFactory.deleteCatalogDB();
        }
    }

    @Override
    public boolean isCatalogDBReady() throws CatalogDBException {
        if (organizationDBAdaptorMap.isEmpty()) {
            return false;
        }
        return getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION).isCatalogDBReady();
    }

    @Override
    public void close() {
        for (OrganizationMongoDBAdaptorFactory dbAdaptorFactory : organizationDBAdaptorMap.values()) {
            dbAdaptorFactory.close();
        }
    }

    @Override
    public void createIndexes(String organization) throws CatalogDBException {
        OrganizationMongoDBAdaptorFactory orgFactory = getOrganizationMongoDBAdaptorFactory(organization);
        orgFactory.createIndexes();
    }

    @Override
    public List<String> getOrganizationIds() throws CatalogDBException {
        // Recheck in case there are new organizations
        initOrganizations(configuration);
        return new ArrayList<>(organizationDBAdaptorMap.keySet());
    }

    public MongoDataStore getMongoDataStore(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getMongoDataStore();
    }

    private void connect(Configuration catalogConfiguration) throws CatalogDBException {
        // Init map of organization db adaptor factories
        organizationDBAdaptorMap = new HashMap<>();
        initOrganizations(catalogConfiguration);
    }

    private void initOrganizations(Configuration catalogConfiguration) throws CatalogDBException {
        // Configure admin organization first
        OrganizationMongoDBAdaptorFactory adminFactory;
        if (organizationDBAdaptorMap.containsKey(ParamConstants.ADMIN_ORGANIZATION)) {
            adminFactory = organizationDBAdaptorMap.get(ParamConstants.ADMIN_ORGANIZATION);
        } else {
            adminFactory = configureOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION, catalogConfiguration);
            organizationDBAdaptorMap.put(ParamConstants.ADMIN_ORGANIZATION, adminFactory);
        }
        if (adminFactory.isCatalogDBReady()) {
            // Read organizations present in the installation
            Query query = new Query(NoteDBAdaptor.QueryParams.TAGS.key(), OrganizationTag.ACTIVE.name());
            OpenCGAResult<Note> results = adminFactory.getCatalogNotesDBAdaptor().get(query, new QueryOptions());

            for (Note organizationNote : results.getResults()) {
                OrganizationSummary organizationSummary = getOrganizationSummary(organizationNote);
                if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationSummary.getId())
                        && (!organizationDBAdaptorMap.containsKey(organizationSummary.getId()))) {
                    OrganizationMongoDBAdaptorFactory orgFactory = configureOrganizationMongoDBAdaptorFactory(organizationSummary.getId(),
                            catalogConfiguration);
                    organizationDBAdaptorMap.put(organizationSummary.getId(), orgFactory);
                }
            }
        }
    }

    private OrganizationSummary getOrganizationSummary(Note note) {
        try {
            String orgSummaryString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(note.getValue());
            return JacksonUtils.getDefaultObjectMapper().readerFor(OrganizationSummary.class).readValue(orgSummaryString);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private OrganizationMongoDBAdaptorFactory configureOrganizationMongoDBAdaptorFactory(String organizationId, Configuration configuration)
            throws CatalogDBException {
        return new OrganizationMongoDBAdaptorFactory(organizationId, mongoManager, mongoDbConfiguration, configuration, ioManagerFactory);
    }

    public OrganizationMongoDBAdaptorFactory getOrganizationMongoDBAdaptorFactory(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization, true);
    }

    private OrganizationMongoDBAdaptorFactory getOrganizationMongoDBAdaptorFactory(String organizationId, boolean raiseException)
            throws CatalogDBException {
        OrganizationMongoDBAdaptorFactory orgFactory = organizationDBAdaptorMap.get(organizationId);
        if (orgFactory == null) {
            if (!ParamConstants.ADMIN_ORGANIZATION.equals(organizationId)) {
                orgFactory = getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION);

                // Read organizations present in the installation
                Query query = new Query(NoteDBAdaptor.QueryParams.TAGS.key(), OrganizationTag.ACTIVE.name());
                OpenCGAResult<Note> results = orgFactory.getCatalogNotesDBAdaptor().get(query, new QueryOptions());

                for (Note organizationNote : results.getResults()) {
                    OrganizationSummary organizationSummary = getOrganizationSummary(organizationNote);
                    if (organizationSummary.getId().equals(organizationId)) {
                        // Organization is present, so create new OrganizationMongoDBAdaptorFactory for the organization
                        OrganizationMongoDBAdaptorFactory organizationMongoDBAdaptorFactory =
                                new OrganizationMongoDBAdaptorFactory(organizationId, mongoManager, mongoDbConfiguration, configuration,
                                        ioManagerFactory);
                        organizationDBAdaptorMap.put(organizationId, organizationMongoDBAdaptorFactory);
                        return organizationMongoDBAdaptorFactory;
                    }
                }
            }

            if (raiseException) {
                throw new CatalogDBException("Could not find database for organization '" + organizationId + "'");
            } else {
                return null;
            }
        }
        return orgFactory;
    }

    @Override
    public MigrationDBAdaptor getMigrationDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getMigrationDBAdaptor();
    }

    @Override
    public MetaDBAdaptor getCatalogMetaDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogMetaDBAdaptor();
    }

    @Override
    public OpenCGAResult<Organization> createOrganization(Organization organization, QueryOptions options, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        OrganizationMongoDBAdaptorFactory orgFactory = getOrganizationMongoDBAdaptorFactory(organization.getId(), false);
        if (orgFactory != null && orgFactory.isCatalogDBReady()) {
            throw new CatalogDBException("Organization '" + organization.getId() + "' already exists.");
        }

        try {
            // Create organization
            OrganizationMongoDBAdaptorFactory organizationDBAdaptorFactory = new OrganizationMongoDBAdaptorFactory(organization.getId(),
                    mongoManager, mongoDbConfiguration, configuration, ioManagerFactory);
            organizationDBAdaptorMap.put(organization.getId(), organizationDBAdaptorFactory);

            OrganizationSummary organizationSummary = new OrganizationSummary(organization.getId(),
                    organizationDBAdaptorFactory.getMongoDataStore().getDatabaseName(), OrganizationTag.ACTIVE.name(), null);
            NoteCreateParams noteCreateParams = new NoteCreateParams(ORGANIZATION_PREFIX + organization.getId(),
                    Collections.singletonList(OrganizationTag.ACTIVE.name()), Note.Visibility.PRIVATE, Note.Type.OBJECT, null);
            try {
                String orgSummaryString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(organizationSummary);
                Map<String, Object> value = JacksonUtils.getDefaultObjectMapper().readerFor(Map.class).readValue(orgSummaryString);
                noteCreateParams.setValue(value);
            } catch (JsonMappingException e) {
                throw new RuntimeException(e);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            Note note = noteCreateParams.toNote(Note.Scope.ORGANIZATION, userId);
            NoteManager.validateNewNote(note, userId);

            // Create new database and indexes
            organizationDBAdaptorFactory.createAllCollections();
            organizationDBAdaptorFactory.createIndexes();

            // Create organization
            OpenCGAResult<Organization> result = organizationDBAdaptorFactory.getCatalogOrganizationDBAdaptor()
                    .insert(organization, options);

            // Keep track of current organization in the ADMIN organization
            if (StringUtils.isNotEmpty(note.getUserId())) {
                // Remove admin organization prefix from userId as it's written in that same organization
                note.setUserId(note.getUserId().replace(ParamConstants.ADMIN_ORGANIZATION + ":", ""));
            }
            getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION).getCatalogNotesDBAdaptor().insert(note);
            return result;
        } catch (Exception e) {
            OrganizationMongoDBAdaptorFactory tmpOrgFactory = organizationDBAdaptorMap.remove(organization.getId());
            if (tmpOrgFactory != null) {
                tmpOrgFactory.deleteCatalogDB();
            }
            // TODO: Delete settings from ADMIN database

            throw e;
        }
    }

    @Override
    public void deleteOrganization(Organization organizationId) throws CatalogDBException {
        OrganizationMongoDBAdaptorFactory orgFactory = getOrganizationMongoDBAdaptorFactory(organizationId.getId());
        orgFactory.deleteCatalogDB();
        orgFactory.close();
        organizationDBAdaptorMap.remove(organizationId.getId());

        // TODO: Remove organization from ADMIN database
    }

    @Override
    public NoteDBAdaptor getCatalogNoteDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogNotesDBAdaptor();
    }

    @Override
    public OrganizationDBAdaptor getCatalogOrganizationDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogOrganizationDBAdaptor();
    }

    @Override
    public UserDBAdaptor getCatalogUserDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogUserDBAdaptor();
    }

    @Override
    public ProjectDBAdaptor getCatalogProjectDbAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogProjectDBAdaptor();
    }

    @Override
    public StudyDBAdaptor getCatalogStudyDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogStudyDBAdaptor();
    }

    @Override
    public FileDBAdaptor getCatalogFileDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogFileDBAdaptor();
    }

    @Override
    public SampleDBAdaptor getCatalogSampleDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogSampleDBAdaptor();
    }

    @Override
    public IndividualDBAdaptor getCatalogIndividualDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogIndividualDBAdaptor();
    }

    @Override
    public JobDBAdaptor getCatalogJobDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogJobDBAdaptor();
    }

    @Override
    public AuditDBAdaptor getCatalogAuditDbAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogAuditDbAdaptor();
    }

    @Override
    public CohortDBAdaptor getCatalogCohortDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogCohortDBAdaptor();
    }

    @Override
    public PanelDBAdaptor getCatalogPanelDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogPanelDBAdaptor();
    }

    @Override
    public FamilyDBAdaptor getCatalogFamilyDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getCatalogFamilyDBAdaptor();
    }

    @Override
    public ClinicalAnalysisDBAdaptor getClinicalAnalysisDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getClinicalAnalysisDBAdaptor();
    }

    @Override
    public InterpretationDBAdaptor getInterpretationDBAdaptor(String organizationId) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organizationId).getInterpretationDBAdaptor();
    }

    public MongoDataStoreManager getMongoManager() {
        return mongoManager;
    }

    public MongoDBConfiguration getMongoDbConfiguration() {
        return mongoDbConfiguration;
    }
}
