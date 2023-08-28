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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created by pfurio on 08/01/16.
 */
public class MongoDBAdaptorFactory implements DBAdaptorFactory {

    public static final String ORGANIZATION_COLLECTION = "organization";

    private final MongoDataStoreManager mongoManager;
    private final MongoDBConfiguration configuration;
    private final String database;
    private MongoDataStore mongoDataStore;

    private Map<String, OrganizationMongoDBAdaptorFactory> organizationDBAdaptorMap;
    private OrganizationMongoDBAdaptor organizationDBAdaptor;

    private final Logger logger;

    public MongoDBAdaptorFactory(Configuration catalogConfiguration) throws CatalogDBException {
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
        this.configuration = mongoDBConfiguration;
        this.database = getCatalogDatabase(catalogConfiguration.getDatabasePrefix());

        logger = LoggerFactory.getLogger(this.getClass());
        connect(catalogConfiguration);
    }

    @Override
    public void createAllCollections(Configuration configuration) throws CatalogException {
        // TODO: Check META object does not exist. Use {@link isCatalogDBReady}
        // TODO: Check all collections do not exists, or are empty
        // TODO: Catch DuplicatedKeyException while inserting META object

        MongoDataStore mongoDataStore = mongoManager.get(database, this.configuration);
        if (!mongoDataStore.getCollectionNames().isEmpty()) {
            throw new CatalogException("Database " + database + " already exists with the following collections: "
                    + StringUtils.join(mongoDataStore.getCollectionNames()) + ".\nPlease, remove the database or choose a different one.");
        }
        OrganizationMongoDBAdaptorFactory.COLLECTIONS_LIST.forEach(mongoDataStore::createCollection);
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
    public void deleteCatalogDB() throws CatalogDBException {
        mongoManager.drop(database);
    }

    @Override
    public boolean isCatalogDBReady() {
        return !mongoDataStore.getCollectionNames().isEmpty();
    }

    @Override
    public void close() {
        mongoManager.close(mongoDataStore.getDatabaseName());
    }

    @Override
    public void createIndexes(String organization) throws CatalogDBException {
        OrganizationMongoDBAdaptorFactory orgFactory = getOrganizationMongoDBAdaptorFactory(organization);
        orgFactory.createIndexes();
    }

    @Override
    public List<String> getOrganizationIds() {
        return new ArrayList<>(organizationDBAdaptorMap.keySet());
    }

    public MongoDataStore getMongoDataStore() {
        return mongoDataStore;
    }

    private void connect(Configuration catalogConfiguration) throws CatalogDBException {
        mongoDataStore = mongoManager.get(database, configuration);
        if (mongoDataStore == null) {
            throw new CatalogDBException("Unable to connect to MongoDB '" + database + "'");
        }

        MongoDBCollection organizationCollection = mongoDataStore.getCollection(ORGANIZATION_COLLECTION);
        MongoDBCollection deletedOrganizationCollection = mongoDataStore
                .getCollection(OrganizationMongoDBAdaptorFactory.DELETED_ORGANIZATION_COLLECTION);
        organizationDBAdaptor = new OrganizationMongoDBAdaptor(organizationCollection, deletedOrganizationCollection, catalogConfiguration,
                this);

        // Fetch all organizations present in the database
        OpenCGAResult<Organization> result = organizationDBAdaptor.get(new Query(), OrganizationManager.INCLUDE_ORGANIZATION_IDS);

        // And create connections to them
        organizationDBAdaptorMap = new HashMap<>();
        for (Organization organization : result.getResults()) {
            String organizationDB = getCatalogOrganizationDatabase(catalogConfiguration.getDatabasePrefix(), organization.getId());
            OrganizationMongoDBAdaptorFactory orgFactory = new OrganizationMongoDBAdaptorFactory(mongoManager, configuration,
                    organizationDB, catalogConfiguration);

            organizationDBAdaptorMap.put(organization.getId(), orgFactory);
        }
    }

    @Override
    public OrganizationDBAdaptor getCatalogOrganizationDBAdaptor() {
        return organizationDBAdaptor;
    }

    private OrganizationMongoDBAdaptorFactory getOrganizationMongoDBAdaptorFactory(String organization) throws CatalogDBException {
        OrganizationMongoDBAdaptorFactory orgFactory = organizationDBAdaptorMap.get(organization);
        if (orgFactory == null) {
            throw new CatalogDBException("Could not find database for organization '" + organization + "'");
        }
        return orgFactory;
    }

    @Override
    public MigrationDBAdaptor getMigrationDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getMigrationDBAdaptor();
    }

    @Override
    public MetaDBAdaptor getCatalogMetaDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogMetaDBAdaptor();
    }

    @Override
    public UserDBAdaptor getCatalogUserDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogUserDBAdaptor();
    }

    @Override
    public ProjectDBAdaptor getCatalogProjectDbAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogProjectDbAdaptor();
    }

    @Override
    public StudyDBAdaptor getCatalogStudyDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogStudyDBAdaptor();
    }

    @Override
    public FileDBAdaptor getCatalogFileDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogFileDBAdaptor();
    }

    @Override
    public SampleDBAdaptor getCatalogSampleDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogSampleDBAdaptor();
    }

    @Override
    public IndividualDBAdaptor getCatalogIndividualDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogIndividualDBAdaptor();
    }

    @Override
    public JobDBAdaptor getCatalogJobDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogJobDBAdaptor();
    }

    @Override
    public AuditDBAdaptor getCatalogAuditDbAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogAuditDbAdaptor();
    }

    @Override
    public CohortDBAdaptor getCatalogCohortDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogCohortDBAdaptor();
    }

    @Override
    public PanelDBAdaptor getCatalogPanelDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogPanelDBAdaptor();
    }

    @Override
    public FamilyDBAdaptor getCatalogFamilyDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogFamilyDBAdaptor();
    }

    @Override
    public ClinicalAnalysisDBAdaptor getClinicalAnalysisDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getClinicalAnalysisDBAdaptor();
    }

    @Override
    public InterpretationDBAdaptor getInterpretationDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getInterpretationDBAdaptor();
    }
}
