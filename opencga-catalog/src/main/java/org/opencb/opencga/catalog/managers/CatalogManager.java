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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.opencb.opencga.catalog.managers.AbstractManager.OPENCGA;

public class CatalogManager implements AutoCloseable {

    protected static Logger logger = LoggerFactory.getLogger(CatalogManager.class);

    private DBAdaptorFactory catalogDBAdaptorFactory;
    private CatalogIOManagerFactory catalogIOManagerFactory;

    private UserManager userManager;
    private ProjectManager projectManager;
    private StudyManager studyManager;
    private FileManager fileManager;
    private JobManager jobManager;
    private IndividualManager individualManager;
    private SampleManager sampleManager;
    private CohortManager cohortManager;
    private FamilyManager familyManager;
    private ClinicalAnalysisManager clinicalAnalysisManager;
    private InterpretationManager interpretationManager;
    private PanelManager panelManager;

    private AuditManager auditManager;
    private AuthorizationManager authorizationManager;

    private Configuration configuration;

    public CatalogManager(Configuration configuration) throws CatalogException {
        this.configuration = configuration;
        logger.debug("CatalogManager configureDBAdaptorFactory");
        catalogDBAdaptorFactory = new MongoDBAdaptorFactory(configuration);
        logger.debug("CatalogManager configureIOManager");
        configureIOManager(configuration);
        logger.debug("CatalogManager configureManager");
        configureManagers(configuration);
    }

    public String getCatalogDatabase() {
        return catalogDBAdaptorFactory.getCatalogDatabase(configuration.getDatabasePrefix());
    }

    private void configureManagers(Configuration configuration) throws CatalogException {
//        catalogClient = new CatalogDBClient(this);
        //TODO: Check if catalog is empty
        //TODO: Setup catalog if it's empty.
        this.initializeAdmin(configuration);
        authorizationManager = new CatalogAuthorizationManager(this.catalogDBAdaptorFactory, configuration);
        auditManager = new AuditManager(authorizationManager, this, this.catalogDBAdaptorFactory, configuration);

        userManager = new UserManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        projectManager = new ProjectManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        studyManager = new StudyManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        fileManager = new FileManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, catalogIOManagerFactory,
                configuration);
        jobManager = new JobManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        sampleManager = new SampleManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        individualManager = new IndividualManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        cohortManager = new CohortManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        familyManager = new FamilyManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, catalogIOManagerFactory,
                configuration);
        panelManager = new PanelManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, catalogIOManagerFactory,
                configuration);
        clinicalAnalysisManager = new ClinicalAnalysisManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        interpretationManager = new InterpretationManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
    }

    private void initializeAdmin(Configuration configuration) throws CatalogDBException {
        if (configuration.getAdmin() == null) {
            configuration.setAdmin(new Admin());
        }

        if (existsCatalogDB()) {
            String secretKey = catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().readSecretKey();
            String algorithm = catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().readAlgorithm();

            configuration.getAdmin().setAlgorithm(algorithm);
            configuration.getAdmin().setSecretKey(secretKey);
        } else {
            configuration.getAdmin().setAlgorithm("HS256");
            configuration.getAdmin().setSecretKey(RandomStringUtils.randomAlphanumeric(15));
        }
    }

    public void updateJWTParameters(ObjectMap params, String token) throws CatalogException {
        if (!OPENCGA.equals(userManager.getUserId(token))) {
            throw new CatalogException("Operation only allowed for the OpenCGA admin");
        }

        if (params == null || params.size() == 0) {
            return;
        }

        catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().updateJWTParameters(params);
    }

    public boolean getDatabaseStatus() {
        if (existsCatalogDB()) {
            return catalogDBAdaptorFactory.getDatabaseStatus();
        } else {
            return false;
        }
    }

    /**
     * Checks if the database exists.
     *
     * @return true if the database exists.
     */
    public boolean existsCatalogDB() {
        return catalogDBAdaptorFactory.isCatalogDBReady();
    }

    public void installCatalogDB(String secretKey, String password, String email, String organization) throws CatalogException {
        if (existsCatalogDB()) {
            throw new CatalogException("Nothing to install. There already exists a catalog database");
        }

        ParamUtils.checkParameter(secretKey, "secretKey");
        ParamUtils.checkParameter(password, "password");

        configuration.getAdmin().setSecretKey(secretKey);

        catalogDBAdaptorFactory.installCatalogDB(configuration);

        User user = new User(OPENCGA, new Account().setType(Account.AccountType.ADMINISTRATOR).setExpirationDate(""))
                .setEmail(StringUtils.isEmpty(email) ? "opencga@admin.com" : email)
                .setOrganization(organization);
        userManager.create(user, password, null);

        String token = userManager.login(OPENCGA, password);
        projectManager.create("admin", "admin", "Default project", "", "", "", null, token);
        studyManager.create("admin", "admin", "admin", "admin", "Default study", null, "", "", null, null, Collections.emptyMap(), null,
                token);
    }

    public void installIndexes(String token) throws CatalogException {
        if (!OPENCGA.equals(userManager.getUserId(token))) {
            throw new CatalogAuthorizationException("Only the admin can install new indexes");
        }
        catalogDBAdaptorFactory.createIndexes();
    }

    public void deleteCatalogDB(String token) throws CatalogException, URISyntaxException {
        String userId = userManager.getUserId(token);
        if (!authorizationManager.checkIsAdmin(userId)) {
            throw new CatalogException("Only the admin can delete the database");
        }

        catalogDBAdaptorFactory.deleteCatalogDB();
        clearCatalog();
    }

    private void clearCatalog() throws URISyntaxException {
        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : configuration.getCatalog().getDatabase().getHosts()) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddresses);
//        MongoDataStore db = mongoManager.get(catalogConfiguration.getDatabase().getDatabase());
        MongoDataStore db = mongoManager.get(getCatalogDatabase());
        db.getDb().drop();
//        mongoManager.close(catalogConfiguration.getDatabase().getDatabase());
        mongoManager.close(getCatalogDatabase());

        Path rootdir = Paths.get(UriUtils.createDirectoryUri(configuration.getWorkspace()));
        deleteFolderTree(rootdir.toFile());
    }

    private void deleteFolderTree(java.io.File folder) {
        java.io.File[] files = folder.listFiles();
        if (files != null) {
            for (java.io.File f : files) {
                if (f.isDirectory()) {
                    deleteFolderTree(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    public CatalogIOManagerFactory getCatalogIOManagerFactory() {
        return catalogIOManagerFactory;
    }

    private void configureIOManager(Configuration properties) throws CatalogIOException {
        catalogIOManagerFactory = new CatalogIOManagerFactory(properties);
    }

    @Override
    public void close() throws CatalogException {
        catalogDBAdaptorFactory.close();
    }

    public UserManager getUserManager() {
        return userManager;
    }

    public ProjectManager getProjectManager() {
        return projectManager;
    }

    public StudyManager getStudyManager() {
        return studyManager;
    }

    public FileManager getFileManager() {
        return fileManager;
    }

    public JobManager getJobManager() {
        return jobManager;
    }

    public IndividualManager getIndividualManager() {
        return individualManager;
    }

    public SampleManager getSampleManager() {
        return sampleManager;
    }

    public CohortManager getCohortManager() {
        return cohortManager;
    }

    public FamilyManager getFamilyManager() {
        return familyManager;
    }

    public ClinicalAnalysisManager getClinicalAnalysisManager() {
        return clinicalAnalysisManager;
    }

    public InterpretationManager getInterpretationManager() {
        return interpretationManager;
    }

    public PanelManager getPanelManager() {
        return panelManager;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public AuthorizationManager getAuthorizationManager() {
        return authorizationManager;
    }

    public AuditManager getAuditManager() {
        return auditManager;
    }
}
