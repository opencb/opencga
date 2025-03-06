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
import org.opencb.opencga.catalog.auth.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.auth.authentication.JwtManager;
import org.opencb.opencga.catalog.auth.authentication.azure.AuthenticationFactory;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationDBAdaptorFactory;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.migration.MigrationManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.JwtUtils;
import org.opencb.opencga.core.common.PasswordUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.Optimizations;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.organizations.*;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectOrganism;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.opencb.opencga.catalog.managers.AbstractManager.OPENCGA;
import static org.opencb.opencga.core.api.ParamConstants.*;

public class CatalogManager implements AutoCloseable {

    protected static Logger logger = LoggerFactory.getLogger(CatalogManager.class);

    private DBAdaptorFactory catalogDBAdaptorFactory;
    private AuthorizationDBAdaptorFactory authorizationDBAdaptorFactory;
    private IOManagerFactory ioManagerFactory;
    private CatalogIOManager catalogIOManager;
    private AuthenticationFactory authenticationFactory;

    private AdminManager adminManager;
    private NoteManager noteManager;
    private OrganizationManager organizationManager;
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
    private WorkflowManager workflowManager;

    private AuditManager auditManager;
    private AuthorizationManager authorizationManager;

    private MigrationManager migrationManager;

    private Configuration configuration;

    public CatalogManager(Configuration configuration) throws CatalogException {
        this.configuration = configuration;
        init();
    }

    private void init() throws CatalogException {
        logger.debug("CatalogManager configureIOManager");
        configureIOManager(configuration);
        logger.debug("CatalogManager configureDBAdaptorFactory");
        catalogDBAdaptorFactory = new MongoDBAdaptorFactory(configuration, ioManagerFactory, catalogIOManager);
        authorizationDBAdaptorFactory = new AuthorizationMongoDBAdaptorFactory((MongoDBAdaptorFactory) catalogDBAdaptorFactory,
                configuration);
        authenticationFactory = new AuthenticationFactory(catalogDBAdaptorFactory, configuration);
        logger.debug("CatalogManager configureManager");
        configureManagers(configuration);
    }

    public String getCatalogDatabase(String organizationId) {
        return catalogDBAdaptorFactory.getCatalogDatabase(configuration.getDatabasePrefix(), organizationId);
    }

    public String getCatalogAdminDatabase() {
        return getCatalogDatabase(ADMIN_ORGANIZATION);
    }

    public List<String> getCatalogDatabaseNames() throws CatalogDBException {
        List<String> databaseNames = new LinkedList<>();
        for (String organizationId : catalogDBAdaptorFactory.getOrganizationIds()) {
            databaseNames.add(getCatalogDatabase(organizationId));
        }
        return databaseNames;
    }

    private void configureManagers(Configuration configuration) throws CatalogException {
        initializeAdmin(configuration);
        EventManager.configure(this, catalogDBAdaptorFactory);

        for (String organizationId : catalogDBAdaptorFactory.getOrganizationIds()) {
            QueryOptions options = new QueryOptions(OrganizationManager.INCLUDE_ORGANIZATION_CONFIGURATION);
            options.put(OrganizationDBAdaptor.IS_ORGANIZATION_ADMIN_OPTION, true);
            Organization organization = catalogDBAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId).get(options).first();
            if (organization != null) {
                authenticationFactory.configureOrganizationAuthenticationManager(organization);
            }
        }
        authorizationManager = new CatalogAuthorizationManager(catalogDBAdaptorFactory, authorizationDBAdaptorFactory);
        auditManager = new AuditManager(authorizationManager, this, this.catalogDBAdaptorFactory, configuration);
        migrationManager = new MigrationManager(this, catalogDBAdaptorFactory, configuration);

        noteManager = new NoteManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, configuration);
        adminManager = new AdminManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, catalogIOManager,
                configuration);
        organizationManager = new OrganizationManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManager, authenticationFactory, configuration);
        userManager = new UserManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, catalogIOManager,
                authenticationFactory, configuration);
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
        clinicalAnalysisManager = new ClinicalAnalysisManager(authorizationManager, auditManager, this,
                catalogDBAdaptorFactory, configuration);
        interpretationManager = new InterpretationManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, configuration);
        workflowManager = new WorkflowManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, ioManagerFactory,
                catalogIOManager, configuration);
    }

    private void initializeAdmin(Configuration configuration) throws CatalogDBException {
        // TODO: Each organization will have different configurations
//        if (configuration.getAdmin() == null) {
//            configuration.setAdmin(new Admin());
//        }
//
//        String secretKey = ParamUtils.defaultString(configuration.getAdmin().getSecretKey(),
//                PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH));
//        String algorithm = ParamUtils.defaultString(configuration.getAdmin().getAlgorithm(), "HS256");
//        if (existsCatalogDB()) {
//            secretKey = catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().readSecretKey();
//            algorithm = catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().readAlgorithm();
//        }
//        configuration.getAdmin().setAlgorithm(algorithm);
//        configuration.getAdmin().setSecretKey(secretKey);
    }

    public void updateJWTParameters(String organizationId, ObjectMap params, String token) throws CatalogException {
        JwtPayload payload = userManager.validateToken(token);
        String userId = payload.getUserId();
        if (!authorizationManager.isAtLeastOrganizationOwnerOrAdmin(organizationId, userId)) {
            throw CatalogAuthorizationException.notOrganizationOwnerOrAdmin();
        }

        if (params == null || params.isEmpty()) {
            return;
        }

        catalogDBAdaptorFactory.getCatalogMetaDBAdaptor(organizationId).updateJWTParameters(params);
    }

    public boolean getDatabaseStatus() throws CatalogDBException {
        return catalogDBAdaptorFactory.getDatabaseStatus();
    }

    public boolean getCatalogDatabaseStatus() throws CatalogDBException {
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
     * @throws CatalogDBException CatalogDBException
     */
    public boolean existsCatalogDB() throws CatalogDBException {
        return catalogDBAdaptorFactory.isCatalogDBReady();
    }

    public void installCatalogDB(String algorithm, String secretKey, String password, String email, boolean force) throws CatalogException {
        if (existsCatalogDB()) {
            if (force) {
                // The password of the old db should match the one to be used in the new installation. Otherwise, they can obtain the same
                // results calling first to "catalog delete" and then "catalog install"
                deleteCatalogDB(password);
                init();
            } else {
                // Check admin password ...
                try {
                    userManager.loginAsAdmin(password);
                    logger.warn("A database called {} already exists", getCatalogAdminDatabase());
                    return;
                } catch (CatalogException e) {
                    throw new CatalogException("A database called " + getCatalogAdminDatabase() + " with a different admin"
                            + " password already exists. If you are aware of that installation, please delete it first.");
                }
            }
        }

        try {
            logger.info("Installing database {} in {}", getCatalogAdminDatabase(), configuration.getCatalog().getDatabase().getHosts());
            privateInstall(algorithm, secretKey, password, email);
            String token = userManager.loginAsAdmin(password).first().getToken();
            installIndexes(ADMIN_ORGANIZATION, token);
        } catch (Exception e) {
            try {
                clearCatalog();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private void privateInstall(String algorithm, String secretKey, String password, String email) throws CatalogException {
        if (existsCatalogDB()) {
            throw new CatalogException("Nothing to install. There already exists a catalog database");
        }
        if (!PasswordUtils.isStrongPassword(password)) {
            throw new CatalogException("Invalid password. Check password strength for user ");
        }
        if (StringUtils.isEmpty(secretKey)) {
            logger.info("Generating secret key");
            secretKey = PasswordUtils.getStrongRandomPassword(JwtManager.SECRET_KEY_MIN_LENGTH);
        }
        ParamUtils.checkParameter(secretKey, "secretKey");
        ParamUtils.checkParameter(password, "password");
        JwtUtils.validateJWTKey(algorithm, secretKey);

        catalogIOManager.createDefaultOpenCGAFolders();

        OrganizationConfiguration organizationConfiguration = new OrganizationConfiguration(
                Collections.singletonList(CatalogAuthenticationManager.createOpencgaAuthenticationOrigin()),
                Constants.DEFAULT_USER_EXPIRATION_DATE, new Optimizations(), new TokenConfiguration(algorithm, secretKey, 3600L));
        organizationManager.create(new OrganizationCreateParams(ADMIN_ORGANIZATION, ADMIN_ORGANIZATION, null, null,
                        organizationConfiguration, null),
                QueryOptions.empty(), null);

        User user = new User(OPENCGA)
                .setEmail(StringUtils.isEmpty(email) ? "opencga@admin.com" : email)
                .setOrganization(ADMIN_ORGANIZATION);
        userManager.create(user, password, null);
        String token = userManager.login(ADMIN_ORGANIZATION, OPENCGA, password).first().getToken();

        // Add OPENCGA as owner of ADMIN_ORGANIZATION
        organizationManager.update(ADMIN_ORGANIZATION, new OrganizationUpdateParams().setOwner(OPENCGA), QueryOptions.empty(), token);
        projectManager.create(new ProjectCreateParams().setId(ADMIN_PROJECT).setDescription("Default project")
                        .setOrganism(new ProjectOrganism("Homo sapiens", "grch38")), null, token);
        studyManager.create(ADMIN_PROJECT, new Study().setId(ADMIN_STUDY).setDescription("Default study"),
                QueryOptions.empty(), token);

        // Skip old available migrations
        migrationManager.skipPendingMigrations(ADMIN_ORGANIZATION, token);
    }

    public void installIndexes(String token) throws CatalogException {
        JwtPayload payload = userManager.validateToken(token);
        if (!authorizationManager.isOpencgaAdministrator(payload)) {
            throw new CatalogException("Operation only allowed for the opencga administrator");
        }
        for (String organizationId : organizationManager.getOrganizationIds(token)) {
            catalogDBAdaptorFactory.createIndexes(organizationId);
        }
    }

    public void installIndexes(String organizationId, String token) throws CatalogException {
        JwtPayload payload = userManager.validateToken(token);
        String userId = payload.getUserId(organizationId);
        if (!authorizationManager.isAtLeastOrganizationOwnerOrAdmin(organizationId, userId)) {
            throw CatalogAuthorizationException.notOrganizationOwnerOrAdmin();
        }

        catalogDBAdaptorFactory.createIndexes(organizationId);
    }

    public void deleteCatalogDB(String password) throws CatalogException {
        try {
            userManager.loginAsAdmin(password);
        } catch (CatalogException e) {
            // Validate that the admin user exists.
            OpenCGAResult<User> result = catalogDBAdaptorFactory.getCatalogUserDBAdaptor(ADMIN_ORGANIZATION)
                    .get(OPENCGA, QueryOptions.empty());
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

    public CatalogIOManager getCatalogIOManager() {
        return catalogIOManager;
    }

    private void configureIOManager(Configuration configuration) throws CatalogIOException {
        ioManagerFactory = new IOManagerFactory();
        catalogIOManager = new CatalogIOManager(configuration);
    }

    @Override
    public void close() throws CatalogException {
        try {
            EventManager.getInstance().close();
        } catch (Exception e) {
            throw new CatalogException(e);
        }
        catalogDBAdaptorFactory.close();
    }

    public AdminManager getAdminManager() {
        return adminManager;
    }

    public NoteManager getNotesManager() {
        return noteManager;
    }

    public OrganizationManager getOrganizationManager() {
        return organizationManager;
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

    public WorkflowManager getWorkflowManager() {
        return workflowManager;
    }
}
