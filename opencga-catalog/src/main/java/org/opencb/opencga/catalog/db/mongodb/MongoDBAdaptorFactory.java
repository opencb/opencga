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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
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
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by pfurio on 08/01/16.
 */
public class MongoDBAdaptorFactory implements DBAdaptorFactory {

    private final MongoDataStoreManager mongoManager;
    private final MongoDBConfiguration mongoDbConfiguration;
    private final Configuration configuration;

    private Map<String, OrganizationMongoDBAdaptorFactory> organizationDBAdaptorMap;

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
        this.mongoDbConfiguration = mongoDBConfiguration;
        this.configuration = catalogConfiguration;
//        this.database = getAdminCatalogDatabase(catalogConfiguration.getDatabasePrefix());

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
    public List<String> getOrganizationIds() {
        return new ArrayList<>(organizationDBAdaptorMap.keySet());
    }

    public MongoDataStore getMongoDataStore(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getMongoDataStore();
    }

    private void connect(Configuration catalogConfiguration) throws CatalogDBException {
        // Init map of organization db adaptor factories
        organizationDBAdaptorMap = new HashMap<>();

        // Configure admin organization first
        OrganizationMongoDBAdaptorFactory adminFactory = configureOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION,
                catalogConfiguration);
        organizationDBAdaptorMap.put(ParamConstants.ADMIN_ORGANIZATION.toLowerCase(), adminFactory);

        // Read organizations present in the installation
        OpenCGAResult<Organization> result = adminFactory.getCatalogOrganizationDBAdaptor().get(QueryOptions.empty());
        if (result.getNumResults() == 1) {
            // TODO: Read organizations present in the installation
            List<String> organizationIds = Collections.emptyList();

            for (String organizationId : organizationIds) {
                OrganizationMongoDBAdaptorFactory orgFactory = configureOrganizationMongoDBAdaptorFactory(organizationId,
                        catalogConfiguration);
                organizationDBAdaptorMap.put(organizationId.toLowerCase(), orgFactory);
            }
        }
    }

    private OrganizationMongoDBAdaptorFactory configureOrganizationMongoDBAdaptorFactory(String organizationId, Configuration configuration)
            throws CatalogDBException {
        String organizationDB = getCatalogDatabase(configuration.getDatabasePrefix(), organizationId);
        return new OrganizationMongoDBAdaptorFactory(mongoManager, mongoDbConfiguration, organizationDB, configuration);
    }

    public OrganizationMongoDBAdaptorFactory getOrganizationMongoDBAdaptorFactory(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization, true);
    }

    private OrganizationMongoDBAdaptorFactory getOrganizationMongoDBAdaptorFactory(String organization, boolean raiseException)
            throws CatalogDBException {
        OrganizationMongoDBAdaptorFactory orgFactory = organizationDBAdaptorMap.get(organization.toLowerCase());
        if (orgFactory == null) {
            if (!organization.equalsIgnoreCase(ParamConstants.ADMIN_ORGANIZATION)) {
                orgFactory = getOrganizationMongoDBAdaptorFactory(ParamConstants.ADMIN_ORGANIZATION);
                QueryOptions options = new QueryOptions();
//                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.CONFIG.key());
                OpenCGAResult<Organization> result = orgFactory.getCatalogOrganizationDBAdaptor().get(options);
                if (result.getNumResults() == 1) {
                    // TODO: look for the organization. Will assume it is not present meanwhile
                    boolean present = false;
                    if (present) {
                        // Create new OrganizationMongoDBAdaptorFactory for the organization
                        OrganizationMongoDBAdaptorFactory organizationMongoDBAdaptorFactory =
                                new OrganizationMongoDBAdaptorFactory(mongoManager, mongoDbConfiguration, organization, configuration);
                        organizationDBAdaptorMap.put(organization.toLowerCase(), organizationMongoDBAdaptorFactory);
                        return organizationMongoDBAdaptorFactory;
                    }
                }
            }

            if (raiseException) {
                throw new CatalogDBException("Could not find database for organization '" + organization + "'");
            } else {
                return null;
            }
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
    public OpenCGAResult<Organization> createOrganization(Organization organization, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (getOrganizationMongoDBAdaptorFactory(organization.getId(), false) != null) {
            throw new CatalogDBException("Organization '" + organization.getId() + "' already exists.");
        }

        // Create organization
        OrganizationMongoDBAdaptorFactory organizationDBAdaptorFactory = new OrganizationMongoDBAdaptorFactory(mongoManager,
                mongoDbConfiguration, organization.getId(), configuration);
        organizationDBAdaptorFactory.createAllCollections();
        organizationDBAdaptorFactory.createIndexes();
        OpenCGAResult<Organization> result = organizationDBAdaptorFactory.getCatalogOrganizationDBAdaptor()
                .insert(organization, options);

        // TODO: Add organization id to list of organization ids in ADMIN database
        return result;
    }

    @Override
    public void deleteOrganization(Organization organization) throws CatalogDBException {
        OrganizationMongoDBAdaptorFactory orgFactory = getOrganizationMongoDBAdaptorFactory(organization.getId());
        orgFactory.deleteCatalogDB();
        orgFactory.close();
        organizationDBAdaptorMap.remove(organization.getId());

        // TODO: Remove organization from ADMIN database
    }

    @Override
    public OrganizationDBAdaptor getCatalogOrganizationDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogOrganizationDBAdaptor();
    }

    @Override
    public UserDBAdaptor getCatalogUserDBAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogUserDBAdaptor();
    }

    @Override
    public ProjectDBAdaptor getCatalogProjectDbAdaptor(String organization) throws CatalogDBException {
        return getOrganizationMongoDBAdaptorFactory(organization).getCatalogProjectDBAdaptor();
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
