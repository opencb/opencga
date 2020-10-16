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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.audit.CatalogAuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.AuthenticationOrigin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.UriCheck;
import org.opencb.opencga.core.models.monitor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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

    private CatalogAuditManager auditManager;
    private AuthorizationManager authorizationManager;

    private Configuration configuration;

    private static final String ADMIN = "admin";

    public CatalogManager(Configuration configuration) throws CatalogException {
        this.configuration = configuration;
        logger.debug("CatalogManager configureDBAdaptorFactory");
        catalogDBAdaptorFactory = new MongoDBAdaptorFactory(configuration) {};
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
        this.initializeAdmin();
        auditManager = new CatalogAuditManager(catalogDBAdaptorFactory.getCatalogAuditDbAdaptor());
        authorizationManager = new CatalogAuthorizationManager(catalogDBAdaptorFactory, auditManager, this.configuration);
        userManager = new UserManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        fileManager = new FileManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        studyManager = new StudyManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        projectManager = new ProjectManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        jobManager = new JobManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, this.configuration);
        sampleManager = new SampleManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        individualManager = new IndividualManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        cohortManager = new CohortManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
        familyManager = new FamilyManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory, catalogIOManagerFactory,
                configuration);
        clinicalAnalysisManager = new ClinicalAnalysisManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, configuration);
    }

    private void initializeAdmin() throws CatalogDBException {

        if (StringUtils.isEmpty(this.configuration.getAdmin().getSecretKey())) {
            this.configuration.getAdmin().setSecretKey(this.catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().readSecretKey());
        }

        if (StringUtils.isEmpty(this.configuration.getAdmin().getAlgorithm())) {
            this.configuration.getAdmin().setAlgorithm(this.catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().readAlgorithm());
        }
    }

    public void insertUpdatedAdmin(Admin admin) throws CatalogDBException {

        this.catalogDBAdaptorFactory.getCatalogMetaDBAdaptor().updateAdmin(admin);
    }

    /**
     * Checks if the database exists.
     *
     * @return true if the database exists.
     */
    public boolean existsCatalogDB() {
        return catalogDBAdaptorFactory.isCatalogDBReady();
    }

    public void installCatalogDB() throws CatalogException {
        // Check jobs folder is empty
        URI jobsURI;
        try {
            jobsURI = UriUtils.createDirectoryUri(configuration.getTempJobsDir());
        } catch (URISyntaxException e) {
            throw new CatalogException("Failed to create a directory URI from " + configuration.getTempJobsDir());
        }
        CatalogIOManager ioManager = getCatalogIOManagerFactory().get(jobsURI);
        if (!ioManager.isDirectory(jobsURI) || CollectionUtils.isNotEmpty(ioManager.listFiles(jobsURI))) {
            throw new CatalogException("Cannot install openCGA. Jobs folder is not empty.\nPlease, empty it first.");
        }
        catalogDBAdaptorFactory.installCatalogDB(configuration);
    }

    public void installIndexes(String token) throws CatalogException {
        if (!ADMIN.equals(userManager.getUserId(token))) {
            throw new CatalogAuthorizationException("Only the admin can install new indexes");
        }
        catalogDBAdaptorFactory.createIndexes();
    }

    public void deleteCatalogDB(boolean force) throws CatalogException, URISyntaxException {
        if (!force) {
            userManager.login("admin", configuration.getAdmin().getPassword());
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

        Path rootdir = Paths.get(UriUtils.createDirectoryUri(configuration.getDataDir()));
        deleteFolderTree(rootdir.toFile());
        if (!configuration.getTempJobsDir().isEmpty()) {
            Path jobsDir = Paths.get(UriUtils.createDirectoryUri(configuration.getTempJobsDir()));
            if (jobsDir.toFile().exists()) {
                deleteFolderTree(jobsDir.toFile());
            }
        }
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

    public HealthCheckResponse healthCheck(String url, String token) throws CatalogException {
        // Only check token if present
        if (StringUtils.isNotEmpty(token)) {
            String user = userManager.getUserId(token);
            if (!ADMIN.equals(user)) {
                throw new CatalogAuthorizationException("Only admin user can run health check operation");
            }
        }

        List<HealthCheckDependency> datastores = new LinkedList<>();
        List<HealthCheckDependency> apis = new LinkedList<>();

        // ---- DBs -----
        // Mongo
        List<DatastoreStatus> databaseStatus = catalogDBAdaptorFactory.getDatabaseStatus();
        long count = databaseStatus.stream().filter(d -> d.getStatus() == DatastoreStatus.Status.UP).count();
        HealthCheckResponse.Status status = count > 0
                ? (count == databaseStatus.size()
                ? HealthCheckResponse.Status.OK
                : HealthCheckResponse.Status.DEGRADED)
                : HealthCheckResponse.Status.DOWN;
        datastores.add(new HealthCheckDependency("", status, "DATABASE", "MongoDB", databaseStatus));

        // --- File system ----
        List<UriStatus> uriHealthCheckList = new LinkedList<>();
        uriHealthCheckList.add(checkUriDependency(configuration.getDataDir(), UriCheck.Permission.WRITE));
        if (configuration.getHealth() != null && configuration.getHealth().getUris() != null) {
            for (UriCheck uriCheck : configuration.getHealth().getUris()) {
                uriHealthCheckList.add(checkUriDependency(uriCheck.getPath(), uriCheck.getPermission()));
            }
        }

        count = uriHealthCheckList.stream().filter(d -> d.getStatus() == HealthCheckResponse.Status.OK).count();
        status = count > 0
                ? (count == uriHealthCheckList.size()
                ? HealthCheckResponse.Status.OK
                : HealthCheckResponse.Status.DEGRADED)
                : HealthCheckResponse.Status.DOWN;
        datastores.add(new HealthCheckDependency("", status, "FILE_SYSTEM", "File mounts", uriHealthCheckList));

        // --- APIs ----
        List<AuthenticationStatus> authenticationStatusList = new LinkedList<>();
        if (this.configuration.getAuthentication() != null && this.configuration.getAuthentication().getAuthenticationOrigins() != null) {
            for (AuthenticationOrigin authenticationOrigin : this.configuration.getAuthentication().getAuthenticationOrigins()) {
                authenticationStatusList.add(userManager.checkAuthenticationHealth(authenticationOrigin));
            }
        }
        count = authenticationStatusList.stream().filter(d -> d.getStatus() == HealthCheckResponse.Status.OK).count();
        status = count > 0
                ? (count == authenticationStatusList.size()
                ? HealthCheckResponse.Status.OK
                : HealthCheckResponse.Status.DEGRADED)
                : HealthCheckResponse.Status.DOWN;
        apis.add(new HealthCheckDependency("", status, "AUTHENTICATION_ORIGINS", "Authentication origins", authenticationStatusList));

        HealthCheckDependencies healthCheckDependencies = new HealthCheckDependencies(datastores, apis);

        // Generate HealthCheckResponse
        List<String> availableComponents = new ArrayList<>();
        List<String> unavailableComponents = new ArrayList<>();

        status = HealthCheckResponse.Status.NOT_CONFIGURED;
        for (HealthCheckDependency datastore : healthCheckDependencies.getDatastores()) {
            if (datastore.getStatus() == HealthCheckResponse.Status.OK) {
                availableComponents.add(datastore.getDescription());
            } else {
                unavailableComponents.add(datastore.getDescription());
            }
            if (datastore.getDescription().equals("MongoDB")) {
                status = datastore.getStatus();
            }
        }

        for (HealthCheckDependency api : healthCheckDependencies.getApis()) {
            if (api.getStatus() == HealthCheckResponse.Status.OK) {
                availableComponents.add(api.getDescription());
            } else {
                unavailableComponents.add(api.getDescription());
            }
        }

        if (status == HealthCheckResponse.Status.OK && !unavailableComponents.isEmpty()) {
            status = HealthCheckResponse.Status.DEGRADED;
        }

        if (StringUtils.isEmpty(token)) {
            return new HealthCheckResponse("OpenCGA", url, TimeUtils.getTime(), new HealthCheckDependencies(), status, availableComponents,
                    unavailableComponents);
        } else {
            return new HealthCheckResponse("OpenCGA", url, TimeUtils.getTime(), healthCheckDependencies, status, availableComponents,
                    unavailableComponents);
        }
    }

    private UriStatus checkUriDependency(String path, UriCheck.Permission permission) {
        UriStatus dependency = new UriStatus(path, permission, HealthCheckResponse.Status.NOT_CONFIGURED, null);
        CatalogIOManager ioManager;
        URI uri;
        try {
            uri = UriUtils.createUri(path);
            ioManager = catalogIOManagerFactory.get(uri);
        } catch (URISyntaxException | CatalogIOException e) {
            dependency.setStatus(HealthCheckResponse.Status.DOWN);
            dependency.setException(e.getMessage());
            logger.error(path + ": " + e.getMessage(), e);
            return dependency;
        }

        try {
            ioManager.checkDirectoryUri(uri, permission == UriCheck.Permission.WRITE);
        } catch (CatalogIOException e) {
            dependency.setStatus(HealthCheckResponse.Status.DEGRADED);
            dependency.setException(e.getMessage());
            logger.error(path + ": " + e.getMessage(), e);
            return dependency;
        }

        dependency.setStatus(HealthCheckResponse.Status.OK);

        return dependency;
    }

//    private UriStatus checkUriDependency(String path, UriCheck.Permission permission) {
//        HealthCheckDependency dependency = new HealthCheckDependency(, path, "File system", "File system dependency");
//        CatalogIOManager ioManager;
//        URI uri;
//        try {
//            uri = UriUtils.createUri(path);
//            ioManager = catalogIOManagerFactory.get(uri);
//        } catch (URISyntaxException | CatalogIOException e) {
//            dependency.setStatus(HealthCheckResponse.Status.DOWN);
//            dependency.setAdditionalProperties(new ObjectMap()
//                    .append("exception", e.getMessage())
//                    .append("permission", permission));
//            logger.error(path + ": " + e.getMessage(), e);
//            return dependency;
//        }
//
//        try {
//            ioManager.checkDirectoryUri(uri, permission == UriCheck.Permission.WRITE);
//        } catch (CatalogIOException e) {
//            dependency.setStatus(HealthCheckResponse.Status.DEGRADED);
//            dependency.setAdditionalProperties(new ObjectMap()
//                    .append("exception", e.getMessage())
//                    .append("permission", permission));
//            logger.error(path + ": " + e.getMessage(), e);
//            return dependency;
//        }
//
//        dependency.setStatus(HealthCheckResponse.Status.OK);
//        dependency.setAdditionalProperties(new ObjectMap("permission", permission));
//
//        return dependency;
//    }

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

    public Configuration getConfiguration() {
        return configuration;
    }

    public AuthorizationManager getAuthorizationManager() {
        return authorizationManager;
    }
}
