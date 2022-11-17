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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.migration.MigrationManager;
import org.opencb.opencga.catalog.utils.JwtUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.catalog.managers.AbstractManager.OPENCGA;
import static org.opencb.opencga.core.api.ParamConstants.ADMIN_PROJECT;
import static org.opencb.opencga.core.api.ParamConstants.ADMIN_STUDY;

public class CatalogManager implements AutoCloseable {

    protected static Logger logger = LoggerFactory.getLogger(CatalogManager.class);

    private DBAdaptorFactory catalogDBAdaptorFactory;
    private IOManagerFactory ioManagerFactory;
    private CatalogIOManager catalogIOManager;

    private AdminManager adminManager;
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

    private MigrationManager migrationManager;

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
        migrationManager = new MigrationManager(this, catalogDBAdaptorFactory, configuration);

        adminManager = new AdminManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, catalogIOManager, configuration);
        userManager = new UserManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, catalogIOManager, configuration);
        projectManager = new ProjectManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, catalogIOManager,
                configuration);
        studyManager = new StudyManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, ioManagerFactory,
                catalogIOManager, configuration);
        fileManager = new FileManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, ioManagerFactory, configuration);
        jobManager = new JobManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, ioManagerFactory, configuration);
        sampleManager = new SampleManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, configuration);
        individualManager = new IndividualManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, configuration);
        cohortManager = new CohortManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, configuration);
        familyManager = new FamilyManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, configuration);
        panelManager = new PanelManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, configuration);
        clinicalAnalysisManager = new ClinicalAnalysisManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                configuration);
        interpretationManager = new InterpretationManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, configuration);
    }

    private void initializeAdmin(Configuration configuration) throws CatalogDBException {
        if (configuration.getAdmin() == null) {
            configuration.setAdmin(new Admin());
        }

        String secretKey = ParamUtils.defaultString(configuration.getAdmin().getSecretKey(),
                PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH));
        String algorithm = ParamUtils.defaultString(configuration.getAdmin().getAlgorithm(), "HS256");
        if (existsCatalogDB()) {
            secretKey = catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().readSecretKey();
            algorithm = catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().readAlgorithm();
        }
        configuration.getAdmin().setAlgorithm(algorithm);
        configuration.getAdmin().setSecretKey(secretKey);
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
        return catalogDBAdaptorFactory.getDatabaseStatus();
    }

    public boolean getCatalogDatabaseStatus() {
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

    public void installCatalogDB(String secretKey, String password, String email, String organization, boolean force, boolean wholeIndexes)
            throws CatalogException {
        if (existsCatalogDB()) {
            if (force) {
                // The password of the old db should match the one to be used in the new installation. Otherwise, they can obtain the same
                // results calling first to "catalog delete" and then "catalog install"
                deleteCatalogDB(password);
            } else {
                // Check admin password ...
                try {
                    userManager.loginAsAdmin(password);
                    logger.warn("A database called " + getCatalogDatabase() + " already exists");
                    return;
                } catch (CatalogException e) {
                    throw new CatalogException("A database called " + getCatalogDatabase() + " with a different admin"
                            + " password already exists. If you are aware of that installation, please delete it first.");
                }
            }
        }

        try {
            logger.info("Installing database {} in {}", getCatalogDatabase(), configuration.getCatalog().getDatabase().getHosts());
            installCatalogDB(secretKey, password, email, organization);
            String token = userManager.loginAsAdmin(password).getToken();
            installIndexes(token, wholeIndexes);
        } catch (Exception e) {
            try {
                clearCatalog();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private void installCatalogDB(String secretKey, String password, String email, String organization) throws CatalogException {
        if (existsCatalogDB()) {
            throw new CatalogException("Nothing to install. There already exists a catalog database");
        }
        if (!PasswordUtils.isStrongPassword(password)) {
            throw new CatalogException("Invalid password. Check password strength for user ");
        }
        ParamUtils.checkParameter(secretKey, "secretKey");
        ParamUtils.checkParameter(password, "password");
        JwtUtils.validateJWTKey(configuration.getAdmin().getAlgorithm(), secretKey);

        configuration.getAdmin().setSecretKey(secretKey);

        catalogDBAdaptorFactory.installCatalogDB(configuration);
        catalogIOManager.createDefaultOpenCGAFolders();

        User user = new User(OPENCGA, new Account().setType(Account.AccountType.ADMINISTRATOR).setExpirationDate(""))
                .setEmail(StringUtils.isEmpty(email) ? "opencga@admin.com" : email)
                .setOrganization(organization);
        userManager.create(user, password, null);

        String token = userManager.login(OPENCGA, password).getToken();
        projectManager.create(ADMIN_PROJECT, ADMIN_PROJECT, "Default project", "", "", "", null, token);
        studyManager.create(ADMIN_PROJECT, new Study().setId(ADMIN_STUDY).setDescription("Default study"), QueryOptions.empty(), token);

        // Skip old available migrations
        migrationManager.skipPendingMigrations(token);
    }

    public void installIndexes(String token) throws CatalogException {
        installIndexes(token, false);
    }

    public void installIndexes(String token, boolean wholeIndexes) throws CatalogException {
        if (!OPENCGA.equals(userManager.getUserId(token))) {
            throw new CatalogAuthorizationException("Only the admin can install new indexes");
        }
        catalogDBAdaptorFactory.createIndexes(wholeIndexes);
    }

    public void deleteCatalogDB(String password) throws CatalogException {
        try {
            userManager.loginAsAdmin(password);
        } catch (CatalogException e) {
            // Validate that the admin user exists.
            OpenCGAResult<User> result = catalogDBAdaptorFactory.getCatalogUserDBAdaptor().get(OPENCGA, QueryOptions.empty());
            if (result.getNumResults() == 1) {
                // Admin user exists so we have to fail. Password must be incorrect.
                throw e;
            } else {
                logger.error("Password could not be validated. Database seems corrupted. Deleting...");
            }
        }

        clearCatalog();
    }

    private void clearCatalog() throws CatalogException {
        // Clear DB
        catalogDBAdaptorFactory.deleteCatalogDB();
        catalogDBAdaptorFactory.close();

        // Clear workspace folder
        Path rootdir;
        try {
            rootdir = Paths.get(UriUtils.createDirectoryUri(configuration.getWorkspace()));
        } catch (URISyntaxException e) {
            throw new CatalogException("Could not create uri for " + configuration.getWorkspace(), e);
        }
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

    public IOManagerFactory getIoManagerFactory() {
        return ioManagerFactory;
    }

    private void configureIOManager(Configuration configuration) throws CatalogIOException {
        ioManagerFactory = new IOManagerFactory();
        catalogIOManager = new CatalogIOManager(configuration);
    }

    @Override
    public void close() throws CatalogException {
        catalogDBAdaptorFactory.close();
    }

    public AdminManager getAdminManager() {
        return adminManager;
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

    public MigrationManager getMigrationManager() {
        return migrationManager;
    }
}
