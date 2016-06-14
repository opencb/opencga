/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.catalog;

import org.apache.commons.lang.NotImplementedException;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.audit.CatalogAuditManager;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.config.Admin;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogStudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.*;
import org.opencb.opencga.catalog.managers.api.*;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.*;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.catalog.session.CatalogSessionManager;
import org.opencb.opencga.catalog.session.SessionManager;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CatalogManager implements AutoCloseable {

    /* DBAdaptor properties */
    public static final String CATALOG_DB_USER = "OPENCGA.CATALOG.DB.USER";
    public static final String CATALOG_DB_DATABASE = "OPENCGA.CATALOG.DB.DATABASE";
    public static final String CATALOG_DB_PASSWORD = "OPENCGA.CATALOG.DB.PASSWORD";
    public static final String CATALOG_DB_HOSTS = "OPENCGA.CATALOG.DB.HOSTS";
    public static final String CATALOG_DB_AUTHENTICATION_DB = "OPENCGA.CATALOG.DB.AUTHENTICATION.DB";
    /* IOManager properties */
    public static final String CATALOG_MAIN_ROOTDIR = "OPENCGA.CATALOG.MAIN.ROOTDIR";
    public static final String CATALOG_JOBS_ROOTDIR = "OPENCGA.CATALOG.JOBS.ROOTDIR";
    /* Manager policies properties */
    public static final String CATALOG_MANAGER_POLICY_CREATION_USER = "OPENCGA.CATALOG.MANAGER.POLICY.CREATION_USER";
    /* Other properties */
    public static final String CATALOG_MAIL_USER = "CATALOG.MAIL.USER";
    public static final String CATALOG_MAIL_PASSWORD = "CATALOG.MAIL.PASSWORD";
    public static final String CATALOG_MAIL_HOST = "CATALOG.MAIL.HOST";
    public static final String CATALOG_MAIL_PORT = "CATALOG.MAIL.PORT";

    protected static Logger logger = LoggerFactory.getLogger(CatalogManager.class);

    private CatalogDBAdaptorFactory catalogDBAdaptorFactory;
    private CatalogIOManagerFactory catalogIOManagerFactory;
//    private CatalogClient catalogClient;

    private IUserManager userManager;
    private IProjectManager projectManager;
    private IStudyManager studyManager;
    private IFileManager fileManager;
    private IJobManager jobManager;
    private IIndividualManager individualManager;
    private ISampleManager sampleManager;
    private Properties properties;
    private AuthenticationManager authenticationManager;
    private CatalogAuditManager auditManager;
    private SessionManager sessionManager;
    private AuthorizationManager authorizationManager;

    private CatalogConfiguration catalogConfiguration;

    @Deprecated
    public CatalogManager(CatalogDBAdaptorFactory catalogDBAdaptorFactory, Properties catalogProperties)
            throws IOException, CatalogIOException {
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.properties = catalogProperties;

        configureIOManager(properties);
        configureManagers(properties);
    }

    public CatalogManager(CatalogConfiguration catalogConfiguration) throws CatalogException {
        this.catalogConfiguration = catalogConfiguration;
        logger.debug("CatalogManager configureDBAdaptor");
        configureDBAdaptor(catalogConfiguration);
        logger.debug("CatalogManager configureIOManager");
        configureIOManager(catalogConfiguration);
        logger.debug("CatalogManager configureManager");
        configureManagers(catalogConfiguration);

//        if (!catalogDBAdaptorFactory.isCatalogDBReady()) {
//            catalogDBAdaptorFactory.installCatalogDB(catalogConfiguration);
////            Admin admin = catalogConfiguration.getAdmin();
////            admin.setPassword(CatalogAuthenticationManager.cipherPassword(admin.getPassword()));
////            catalogDBAdaptorFactory.initializeCatalogDB(admin);
//        }
    }

    @Deprecated
    public CatalogManager(Properties catalogProperties) throws CatalogException {
        this.properties = catalogProperties;
        logger.debug("CatalogManager configureDBAdaptor");
        configureDBAdaptor(properties);
        logger.debug("CatalogManager configureIOManager");
        configureIOManager(properties);
        logger.debug("CatalogManager configureManager");
        configureManagers(properties);

        if (!catalogDBAdaptorFactory.isCatalogDBReady()) {
            catalogDBAdaptorFactory.initializeCatalogDB(new Admin());
            User admin = new User("admin", "admin", "admin@email.com", "", "openCB", User.Role.ADMIN, new User.UserStatus());
            catalogDBAdaptorFactory.getCatalogUserDBAdaptor().insertUser(admin, null);
            authenticationManager.newPassword("admin", "admin");
        }
    }


    @Deprecated
    private void configureManagers(Properties properties) {
//        catalogClient = new CatalogDBClient(this);
        //TODO: Check if catalog is empty
        //TODO: Setup catalog if it's empty.
//
//        auditManager = new CatalogAuditManager(catalogDBAdaptorFactory.getCatalogAuditDbAdaptor(), catalogDBAdaptorFactory
//                .getCatalogUserDBAdaptor(), authorizationManager, properties);
//        authenticationManager = new CatalogAuthenticationManager(catalogDBAdaptorFactory.getCatalogUserDBAdaptor(), properties);
//        authorizationManager = new CatalogAuthorizationManager(catalogDBAdaptorFactory, auditManager);
//        userManager = new UserManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
//                catalogIOManagerFactory, properties);
//        fileManager = new FileManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
//                catalogIOManagerFactory, properties);
//        studyManager = new StudyManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
//                catalogIOManagerFactory, properties);
//        projectManager = new ProjectManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
//                catalogIOManagerFactory, properties);
//        jobManager = new JobManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
//                catalogIOManagerFactory, catalogConfiguration);
//        sampleManager = new SampleManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
//                catalogIOManagerFactory, properties);
//        individualManager = new IndividualManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
//                catalogIOManagerFactory, properties);
        throw new NotImplementedException("Configure managers does not take a Properties object any more. "
                + "Use CatalogConfiguration instead");
    }

    private void configureManagers(CatalogConfiguration catalogConfiguration) {
//        catalogClient = new CatalogDBClient(this);
        //TODO: Check if catalog is empty
        //TODO: Setup catalog if it's empty.

        auditManager = new CatalogAuditManager(catalogDBAdaptorFactory.getCatalogAuditDbAdaptor(), catalogDBAdaptorFactory
                .getCatalogUserDBAdaptor(), authorizationManager, catalogConfiguration);
        authenticationManager = new CatalogAuthenticationManager(catalogDBAdaptorFactory, catalogConfiguration);
        authorizationManager = new CatalogAuthorizationManager(catalogDBAdaptorFactory, auditManager);
        sessionManager = new CatalogSessionManager(catalogDBAdaptorFactory, catalogConfiguration);
        userManager = new UserManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        fileManager = new FileManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        studyManager = new StudyManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        projectManager = new ProjectManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        jobManager = new JobManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
                catalogIOManagerFactory, this.catalogConfiguration);
        sampleManager = new SampleManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        individualManager = new IndividualManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
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
        catalogDBAdaptorFactory.installCatalogDB(catalogConfiguration);
    }

    public void installIndexes() throws CatalogException {
        authenticationManager.authenticate("admin", catalogConfiguration.getAdmin().getPassword(), true);
        catalogDBAdaptorFactory.createIndexes();
    }

    public void deleteCatalogDB(boolean force) throws CatalogException {
        if (!force) {
            authenticationManager.authenticate("admin", catalogConfiguration.getAdmin().getPassword(), true);
        }
        catalogDBAdaptorFactory.deleteCatalogDB();
        clearCatalog();
    }

    private void clearCatalog() {
        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : catalogConfiguration.getDatabase().getHosts()) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
        MongoDataStoreManager mongoManager = new MongoDataStoreManager(dataStoreServerAddresses);
        MongoDataStore db = mongoManager.get(catalogConfiguration.getDatabase().getDatabase());
        db.getDb().drop();
        mongoManager.close(catalogConfiguration.getDatabase().getDatabase());

        Path rootdir = Paths.get(URI.create(catalogConfiguration.getDataDir()));
        deleteFolderTree(rootdir.toFile());
        if (!catalogConfiguration.getTempJobsDir().isEmpty()) {
            Path jobsDir = Paths.get(URI.create(catalogConfiguration.getTempJobsDir()));
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
//
//    public void testIndices() {
//        System.out.println("vamos bien");
//        catalogDBAdaptorFactory.getCatalogMongoMetaDBAdaptor().createIndexes();
//    }
//
//    public CatalogClient client() {
//        return client("");
//    }
//
//    public CatalogClient client(String sessionId) {
//        catalogClient.setSessionId(sessionId);
//        return catalogClient;
//    }

    public CatalogIOManagerFactory getCatalogIOManagerFactory() {
        return catalogIOManagerFactory;
    }

    @Deprecated
    private void configureIOManager(Properties properties) throws CatalogIOException {
        catalogIOManagerFactory = new CatalogIOManagerFactory(properties);
    }

    private void configureIOManager(CatalogConfiguration properties) throws CatalogIOException {
        catalogIOManagerFactory = new CatalogIOManagerFactory(properties);
    }

    @Deprecated
    private void configureDBAdaptor(Properties properties)
            throws CatalogDBException {

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", properties.getProperty(CATALOG_DB_USER, null))
                .add("password", properties.getProperty(CATALOG_DB_PASSWORD, null))
                .add("authenticationDatabase", properties.getProperty(CATALOG_DB_AUTHENTICATION_DB, null))
                .build();

        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : properties.getProperty(CATALOG_DB_HOSTS, "localhost").split(",")) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
//        catalogDBAdaptorFactory = new CatalogMongoDBAdaptor(dataStoreServerAddresses, mongoDBConfiguration,
//                properties.getProperty(CATALOG_DB_DATABASE, ""));
        catalogDBAdaptorFactory = new CatalogMongoDBAdaptorFactory(dataStoreServerAddresses, mongoDBConfiguration,
                properties.getProperty(CATALOG_DB_DATABASE, "")) {
        };
    }

    private void configureDBAdaptor(CatalogConfiguration properties) throws CatalogDBException {

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", properties.getDatabase().getUser())
                .add("password", properties.getDatabase().getPassword())
                .add("authenticationDatabase", properties.getDatabase().getOptions().get("authenticationDatabase"))
                .build();

        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : properties.getDatabase().getHosts()) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
//        catalogDBAdaptorFactory = new CatalogMongoDBAdaptor(dataStoreServerAddresses, mongoDBConfiguration,
//                properties.getProperty(CATALOG_DB_DATABASE, ""));
        catalogDBAdaptorFactory = new CatalogMongoDBAdaptorFactory(dataStoreServerAddresses, mongoDBConfiguration,
                properties.getDatabase().getDatabase()) {
        };
    }

    @Override
    public void close() throws CatalogException {
        catalogDBAdaptorFactory.close();
    }

    /*
     * Getter path methods.
     * ***************************
     */

    public URI getUserUri(String userId) throws CatalogIOException {
        return catalogIOManagerFactory.getDefault().getUserUri(userId);
    }

    public URI getProjectUri(String userId, String projectId) throws CatalogIOException {
        return catalogIOManagerFactory.getDefault().getProjectUri(userId, projectId);
    }

    public URI getStudyUri(long studyId)
            throws CatalogException {
        return fileManager.getStudyUri(studyId);
    }

    public URI getFileUri(File file) throws CatalogException {
        return fileManager.getFileUri(file);
    }

    public URI getFileUri(Study study, File file) throws CatalogException {
        return fileManager.getFileUri(study, file);
    }

    public URI getFileUri(long studyId, String relativeFilePath)
            throws CatalogException {
        return fileManager.getFileUri(studyId, relativeFilePath);
    }

    @Deprecated
    public URI getFileUri(URI studyUri, String relativeFilePath)
            throws CatalogException {
        return fileManager.getFileUri(studyUri, relativeFilePath);
    }

    public boolean isExternal(File file) throws CatalogException {
        return fileManager.isExternal(file);
    }

    public long getProjectIdByStudyId(long studyId) throws CatalogException {
        return studyManager.getProjectId(studyId);
    }

    /*
     * Id methods
     * <user>@project:study:directories:filePath
     * ***************************
     */

    public long getProjectId(String id) throws CatalogException {
        return projectManager.getProjectId(id);
    }

    public long getStudyId(String id) throws CatalogException {
        return studyManager.getStudyId(id);
    }

    public long getStudyId(String studyId, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return studyManager.getStudyId(userId, studyId);
    }

    @Deprecated
    public long getFileId(String id) throws CatalogException {
        return fileManager.getFileId(id);
    }

    public long getFileId(String fileId, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return fileManager.getFileId(userId, fileId);
    }

    public List<Long> getFileIds(String fileIds, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return fileManager.getFileIds(userId, fileIds);
    }

    public long getToolId(String id) throws CatalogException {
        return jobManager.getToolId(id);
    }

    /*
     * User methods
     * ***************************
     */

    public QueryResult<User> createUser(String id, String name, String email, String password, String organization, Long diskQuota,
                                        QueryOptions options) throws CatalogException {
//        catalogDBAdaptorFactory.getCatalogMongoMetaDBAdaptor().checkAdmin(catalogConfiguration.getAdmin().getPassword());
        return userManager.create(id, name, email, password, organization, diskQuota, options,
                catalogConfiguration.getAdmin().getPassword());
    }

    @Deprecated
    public QueryResult<User> createUser(String id, String name, String email, String password, String organization, Long diskQuota,
                                        QueryOptions options, String sessionId)
            throws CatalogException {
        return userManager.create(id, name, email, password, organization, diskQuota, options, sessionId);
    }

    @Deprecated
    public QueryResult<ObjectMap> loginAsAnonymous(String sessionIp)
            throws CatalogException, IOException {
        return userManager.loginAsAnonymous(sessionIp);
    }

    public QueryResult<ObjectMap> login(String userId, String password, String sessionIp) throws CatalogException, IOException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(password, "password");
        ParamUtils.checkParameter(sessionIp, "sessionIp");

        authenticationManager.authenticate(userId, password, true);
        return sessionManager.createToken(userId, sessionIp);
    }

    public QueryResult logout(String userId, String sessionId) throws CatalogException {
        return userManager.logout(userId, sessionId);
    }

    @Deprecated
    public QueryResult logoutAnonymous(String sessionId) throws CatalogException {
        return userManager.logoutAnonymous(sessionId);
    }

    public QueryResult changePassword(String userId, String oldPassword, String newPassword, String sessionId)
            throws CatalogException {
        userManager.changePassword(userId, oldPassword, newPassword);
        return new QueryResult("changePassword", 0, 0, 0, "", "", Collections.emptyList());
    }

    public QueryResult changeEmail(String userId, String nEmail, String sessionId) throws CatalogException {
        return userManager.update(userId, new ObjectMap("email", nEmail), null, sessionId);
    }

    public QueryResult resetPassword(String userId, String email) throws CatalogException {
        return userManager.resetPassword(userId, email);
    }

    public QueryResult<User> getUser(String userId, String lastActivity, String sessionId) throws CatalogException {
        return getUser(userId, lastActivity, new QueryOptions(), sessionId);
    }

    public QueryResult<User> getUser(String userId, String lastActivity, QueryOptions options, String sessionId)
            throws CatalogException {
        return userManager.read(userId, lastActivity, options, sessionId);
    }

    public String getUserIdBySessionId(String sessionId) {
        return userManager.getUserId(sessionId);
    }

    public String getUserIdByStudyId(long studyId) throws CatalogException {
        return studyManager.getUserId(studyId);
    }

    public String getUserIdByProjectId(long projectId) throws CatalogException {
        return projectManager.getUserId(projectId);
    }

    public QueryResult<User> modifyUser(String userId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        return userManager.update(userId, parameters, null, sessionId);  //TODO: Add query options
    }

    public QueryResult<User> deleteUser(String userId, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return userManager.delete(userId, queryOptions, sessionId);
    }

    /*
     * Project methods
     * ***************************
     */

    public QueryResult<Project> createProject(String name, String alias, String description, String organization, QueryOptions options,
                                              String sessionId) throws CatalogException {
        return projectManager.create(name, alias, description, organization, options, sessionId);
    }

    public QueryResult<Project> getProject(long projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        return projectManager.read(projectId, options, sessionId);
    }

    public QueryResult<Project> getAllProjects(String ownerId, QueryOptions options, String sessionId) throws CatalogException {
        return projectManager.readAll(new Query("ownerId", ownerId), options, sessionId);
    }

    public QueryResult renameProject(long projectId, String newProjectAlias, String sessionId)
            throws CatalogException {
        return projectManager.update(projectId, new QueryOptions("alias", newProjectAlias), null, sessionId); //TODO: Add query options
    }

    /**
     * Modify some params from the specified project.
     * <p>
     * name
     * description
     * organization
     * status
     * attributes
     *
     * @param projectId  Project identifier
     * @param parameters Parameters to change.
     * @param sessionId  sessionId to check permissions
     * @return QueryResult
     * @throws CatalogException CatalogException
     */
    public QueryResult modifyProject(long projectId, ObjectMap parameters, String sessionId) throws CatalogException {
        return projectManager.update(projectId, parameters, null, sessionId); //TODO: Add query options
    }

    /*
     * Study methods
     * ***************************
     */
    public QueryResult<Study> createStudy(long projectId, String name, String alias, Study.Type type, String description,
                                          String sessionId)
            throws CatalogException {
        return createStudy(projectId, name, alias, type, null, description, null, null, null, null, null, null, null, null,
                sessionId);
    }

    /**
     * Creates a new Study in catalog.
     *
     * @param projectId    Parent project id
     * @param name         Study Name
     * @param alias        Study Alias. Must be unique in the project's studies
     * @param type         Study type: CONTROL_CASE, CONTROL_SET, ... (see org.opencb.opencga.catalog.models.Study.Type)
     * @param creationDate Creation date. If null, now
     * @param description  Study description. If null, empty string
     * @param status       Unused
     * @param cipher       Unused
     * @param uriScheme    UriScheme to select the CatalogIOManager. Default: CatalogIOManagerFactory.DEFAULT_CATALOG_SCHEME
     * @param uri          URI for the folder where to place the study. Scheme must match with the uriScheme. Folder must exist.
     * @param datastores   DataStores information
     * @param stats        Optional stats
     * @param attributes   Optional attributes
     * @param options      QueryOptions
     * @param sessionId    User's sessionId
     * @return Generated study
     * @throws CatalogException CatalogException
     */
    public QueryResult<Study> createStudy(long projectId, String name, String alias, Study.Type type, String creationDate,
                                          String description, Status status, String cipher, String uriScheme, URI uri, Map<File.Bioformat,
            DataStore> datastores, Map<String, Object> stats, Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        QueryResult<Study> result = studyManager.create(projectId, name, alias, type, creationDate, description, status,
                cipher, uriScheme,
                uri, datastores, stats, attributes, options, sessionId);
        //if (uri != null) {
            createFolder(result.getResult().get(0).getId(), Paths.get("data"), true, null, sessionId);
            createFolder(result.getResult().get(0).getId(), Paths.get("analysis"), true, null, sessionId);
        //}
        return result;
    }

    public QueryResult<Study> getStudy(long studyId, String sessionId)
            throws CatalogException {
        return getStudy(studyId, null, sessionId);
    }

    public QueryResult<Study> getStudy(long studyId, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.read(studyId, options, sessionId);
    }

    public QueryResult<StudySummary> getStudySummary(long studyId, String sessionId, QueryOptions queryOptions) throws CatalogException {
        return studyManager.getSummary(studyId, sessionId, queryOptions);
    }

    public QueryResult<Study> getAllStudiesInProject(long projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.readAll(new Query(CatalogStudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), options, sessionId);
    }

    public QueryResult<Study> getAllStudies(Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.readAll(query, options, sessionId);
    }

    public QueryResult renameStudy(long studyId, String newStudyAlias, String sessionId)
            throws CatalogException {
        return studyManager.update(studyId, new ObjectMap("alias", newStudyAlias), null, sessionId);
    }

    public QueryResult addUsersToGroup(long studyId, String groupId, String userIds, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return authorizationManager.addUsersToGroup(userId, studyId, groupId, userIds);
    }

    public QueryResult removeUsersFromGroup(long studyId, String groupId, String userIds, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        authorizationManager.removeUsersFromGroup(userId, studyId, groupId, userIds);
        return new QueryResult("removeUsersFromGroup");
    }

    public QueryResult shareStudy(long studyId, String members, String roleId, boolean override, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return authorizationManager.addMembersToRole(userId, studyId, members, roleId, override);
    }

    public QueryResult unshareStudy(long studyId, String members, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        authorizationManager.removeMembersFromRole(userId, studyId, members);
        return new QueryResult("unshareStudy");
    }

    public QueryResult<StudyAcl> getStudyAcls(String studyStr, List<String> members, String sessionId) throws CatalogException {
        return studyManager.getStudyAcls(studyStr, members, sessionId);
    }

    /**
     * Modify some params from the specified study.
     * <p>
     * name
     * description
     * organization
     * status
     * <p>
     * attributes
     * stats
     *
     * @param studyId    Study identifier
     * @param parameters Parameters to change.
     * @param sessionId  sessionId to check permissions
     * @return QueryResult
     * @throws CatalogException CatalogException
     */
    public QueryResult modifyStudy(long studyId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        return studyManager.update(studyId, parameters, null, sessionId);
    }

    /*
     * File methods
     * ***************************
     */

    public String getFileOwner(long fileId) throws CatalogException {
        return fileManager.getUserId(fileId);
    }

    public long getStudyIdByFileId(long fileId) throws CatalogException {
        return fileManager.getStudyId(fileId);
    }

    //create file with byte[]
    public QueryResult<File> createFile(long studyId, File.Format format, File.Bioformat bioformat, String path, byte[] bytes, String
            description, boolean parents, String sessionId) throws CatalogException, IOException {
        QueryResult<File> queryResult = fileManager.create(studyId, File.Type.FILE, format, bioformat, path, null, null,
                description, new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, parents, null, sessionId);
        new CatalogFileUtils(this).upload(new ByteArrayInputStream(bytes), queryResult.first(), sessionId, false, false, true);
        return getFile(queryResult.first().getId(), sessionId);
    }

    public QueryResult<File> createFile(long studyId, File.Format format, File.Bioformat bioformat, String path, URI fileLocation, String
            description, boolean parents, String sessionId) throws CatalogException, IOException {
        QueryResult<File> queryResult = fileManager.create(studyId, File.Type.FILE, format, bioformat, path, null, null,
                description, new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, parents, null, sessionId);
        new CatalogFileUtils(this).upload(fileLocation, queryResult.first(), null, sessionId, false, false, true, true, Long.MAX_VALUE);
        return getFile(queryResult.first().getId(), sessionId);
    }

    public QueryResult<File> createFile(long studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                        boolean parents, long jobId, String sessionId) throws CatalogException {
        return fileManager.create(studyId, File.Type.FILE, format, bioformat, path, null, null, description, null, 0, -1, null,
                jobId, null, null, parents, null, sessionId);
    }


    public QueryResult<File> createFile(long studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                        String ownerId, String creationDate, String description, File.FileStatus status, long diskUsage,
                                        long experimentId, List<Long> sampleIds, long jobId, Map<String, Object> stats,
                                        Map<String, Object> attributes, boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.create(studyId, type, format, bioformat, path, ownerId, creationDate, description, status,
                diskUsage, experimentId, sampleIds, jobId, stats, attributes, parents, options, sessionId);
    }

    public QueryResult<File> createFolder(long studyId, Path folderPath, boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkPath(folderPath, "folderPath");
        return fileManager.createFolder(studyId, folderPath.toString() + "/", null, parents, null, options, sessionId);
    }

    public QueryResult<File> createFolder(long studyId, Path folderPath, File.FileStatus status, boolean parents, String description,
                                          QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkPath(folderPath, "folderPath");
        return fileManager.createFolder(studyId, folderPath.toString() + "/", status, parents, description, options, sessionId);
    }

    public QueryResult deleteFolder(long folderId, String sessionId) throws CatalogException, IOException {
        return deleteFile(folderId, sessionId);
    }

    public QueryResult deleteFile(long fileId, String sessionId) throws CatalogException, IOException {
        return fileManager.delete(fileId, null, sessionId);
    }

    @Deprecated
    public QueryResult moveFile(long fileId, String newPath, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.move(fileId, newPath, options, sessionId);
    }

    public QueryResult renameFile(long fileId, String newName, String sessionId) throws CatalogException {
        return fileManager.rename(fileId, newName, sessionId);
    }

    /**
     * Modify some params from the specified file.
     * <p>
     * name
     * type
     * format
     * bioformat
     * description
     * status
     * <p>
     * attributes
     * stats
     *
     * @param fileId     File identifier
     * @param parameters Parameters to change.
     * @param sessionId  sessionId to check permissions
     * @return QueryResult QueryResult
     * @throws CatalogException CatalogException
     */
    public QueryResult modifyFile(long fileId, ObjectMap parameters, String sessionId) throws CatalogException {
        return fileManager.update(fileId, parameters, null, sessionId); //TODO: Add query options
    }

    public QueryResult<File> getFileParent(long fileId, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.getParent(fileId, options, sessionId);
    }

    public QueryResult<File> getFileParents(long fileId, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.getParents(fileId, options, sessionId);
    }

    public QueryResult<File> getFile(long fileId, String sessionId) throws CatalogException {
        return getFile(fileId, null, sessionId);
    }

    public QueryResult<File> getFile(long fileId, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.read(fileId, options, sessionId);
    }

    public QueryResult<File> getAllFiles(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.readAll(studyId, query, options, sessionId);
    }

    public QueryResult<File> getAllFilesInFolder(long folderId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkId(folderId, "folderId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        long studyId = getStudyIdByFileId(folderId);
        File folder = getFile(folderId, sessionId).first();
        if (!folder.getType().equals(File.Type.FOLDER)) {
            throw new CatalogDBException("File {id:" + folderId + ", path:'" + folder.getPath() + "'} is not a folder.");
        }
        Query query = new Query(CatalogFileDBAdaptor.QueryParams.DIRECTORY.key(), folder.getPath());
        return fileManager.readAll(studyId, query, options, sessionId);
    }

    public DataInputStream downloadFile(long fileId, String sessionId) throws IOException, CatalogException {
        return downloadFile(fileId, -1, -1, sessionId);
    }

    public DataInputStream downloadFile(long fileId, int start, int limit, String sessionId) throws IOException, CatalogException {
        return fileManager.download(fileId, start, limit, null, sessionId);
    }

    public DataInputStream grepFile(long fileId, String pattern, boolean ignoreCase, boolean multi, String sessionId)
            throws CatalogException {
        QueryOptions options = new QueryOptions("ignoreCase", ignoreCase);
        options.put("multi", multi);
        return fileManager.grep(fileId, pattern, options, sessionId);
    }


    @Deprecated
    public QueryResult shareFile(String fileIds, String userIds, AclEntry acl, String sessionId) throws CatalogException {
        throw new CatalogException("The method being called is deprecated.");
//        return authorizationManager.setFileACL(fileIds, userIds, acl, sessionId);
    }

    public QueryResult shareFile(String fileIds, String members, List<String> permissions, boolean override, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> fileList = fileManager.getFileIds(userId, fileIds);
        return authorizationManager.setFilePermissions(userId, fileList, members, permissions, override);
    }

//    public QueryResult unshareFile(String fileIds, String userIds, String sessionId) throws CatalogException {
//        return authorizationManager.unsetFileACL(fileIds, userIds, sessionId);
//    }

    public QueryResult unshareFile(String fileIds, String members, String permissions, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> fileList = fileManager.getFileIds(userId, fileIds);
        List<String> permissionList = permissions != null && !permissions.isEmpty()
                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
        authorizationManager.unsetFilePermissions(userId, fileList, members, permissionList);
        return new QueryResult("unshareFile");
    }

    /*Require role admin*/
    public QueryResult<File> searchFile(Query query, QueryOptions options, String sessionId) throws CatalogException {
        return searchFile(-1, query, options, sessionId);
    }

    public QueryResult<File> searchFile(long studyId, Query query, String sessionId) throws CatalogException {
        return searchFile(studyId, query, null, sessionId);
    }

    public QueryResult<File> searchFile(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.readAll(studyId, query, options, sessionId);
    }

    public QueryResult<Dataset> createDataset(long studyId, String name, String description, List<Long> files,
                                              Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.createDataset(studyId, name, description, files, attributes, options, sessionId);
    }

    public QueryResult<Dataset> getDataset(long dataSetId, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.readDataset(dataSetId, options, sessionId);
    }


    public QueryResult refreshFolder(final long folderId, final String sessionId)
            throws CatalogDBException, IOException {
        throw new UnsupportedOperationException();
    }

    public QueryResult<File> unlink(long fileId, String sessionId) throws CatalogException, IOException {
        return fileManager.unlink(fileId, sessionId);
    }

    public QueryResult shareDatasets(String datasetIds, String members, List<String> permissions, String sessionId, boolean override)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> datasetList = fileManager.getDatasetIds(userId, datasetIds);
        return authorizationManager.setDatasetPermissions(userId, datasetList, members, permissions, override);
    }

    public QueryResult unshareDatasets(String datasetIds, String userIds, String sessionId, String permissions) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> datasetList = fileManager.getDatasetIds(userId, datasetIds);
        List<String> permissionList = permissions != null && !permissions.isEmpty()
                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
        authorizationManager.unsetDatasetPermissions(userId, datasetList, userIds, permissionList);
        return new QueryResult("unshareDatasets");
    }

    public QueryResult fileGroupBy(Query query, QueryOptions qOptions, String fields, String sessionId) throws CatalogException {
        return fileManager.groupBy(query, Arrays.asList(fields.split(",")), qOptions, sessionId);
    }

    /*
     * **************************
     * Job methods
     * ***************************
     */

    public long getStudyIdByJobId(long jobId) throws CatalogException {
        return jobManager.getStudyId(jobId);
    }

    public QueryResult<Job> createJob(long studyId, String name, String toolName, String description, String executor, Map<String, String>
            params, String commandLine, URI tmpOutDirUri, long outDirId, List<Long> inputFiles, List<Long> outputFiles, Map<String,
            Object> attributes, Map<String, Object> resourceManagerAttributes, Job.JobStatus status, long startTime, long endTime,
                                      QueryOptions options, String sessionId) throws CatalogException {
        return jobManager.create(studyId, name, toolName, description, executor, params, commandLine, tmpOutDirUri, outDirId, inputFiles,
                outputFiles, attributes, resourceManagerAttributes, status, startTime, endTime, options, sessionId);
    }

    public URI createJobOutDir(long studyId, String dirName, String sessionId)
            throws CatalogException {
        return jobManager.createJobOutDir(studyId, dirName, sessionId);
    }

    public QueryResult<ObjectMap> incJobVisites(long jobId, String sessionId) throws CatalogException {
        return jobManager.visit(jobId, sessionId);
    }

    public QueryResult deleteJob(long jobId, String sessionId) throws CatalogException {
        return jobManager.delete(jobId, null, sessionId);
    }


    public QueryResult<Job> getJob(long jobId, QueryOptions options, String sessionId) throws CatalogException {
        return jobManager.read(jobId, options, sessionId);
    }

    public QueryResult<Job> getUnfinishedJobs(String sessionId) throws CatalogException {
        return jobManager.readAll(new Query("status.status",
                Arrays.asList(
                        Job.JobStatus.PREPARED,
                        Job.JobStatus.QUEUED,
                        Job.JobStatus.RUNNING,
                        Job.JobStatus.DONE
                )
        ), null, sessionId);
    }


    public QueryResult<Job> getAllJobs(long studyId, String sessionId) throws CatalogException {
        return jobManager.readAll(studyId, null, null, sessionId);
    }

    public QueryResult<Job> getAllJobs(long studyId, Query query, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return jobManager.readAll(studyId, query, queryOptions, sessionId);
    }


    public QueryResult<Job> modifyJob(long jobId, ObjectMap parameters, String sessionId) throws CatalogException {
        return jobManager.update(jobId, parameters, null, sessionId); //TODO: Add query options
    }

    public QueryResult shareJob(String jobIds, String members, List<String> permissions, boolean override, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> jobList = jobManager.getJobIds(userId, jobIds);
        return authorizationManager.setJobPermissions(userId, jobList, members, permissions, override);
    }

    public QueryResult unshareJob(String jobIds, String userIds, String permissions, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> jobList = jobManager.getJobIds(userId, jobIds);
        List<String> permissionList = permissions != null && !permissions.isEmpty()
                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
        authorizationManager.unsetJobPermissions(userId, jobList, userIds, permissionList);
        return new QueryResult("unshareJob");
    }

    public QueryResult jobGroupBy(Query query, QueryOptions qOptions, String fields, String sessionId) throws CatalogException {
        return jobManager.groupBy(query, Arrays.asList(fields.split(",")), qOptions, sessionId);
    }

    /*
     * Individual methods
     * ***************************
     */

    public QueryResult<Individual> createIndividual(long studyId, String name, String family, long fatherId, long motherId,
                                                    Individual.Gender gender, QueryOptions options, String sessionId)
            throws CatalogException {
        return individualManager.create(studyId, name, family, fatherId, motherId, gender, options, sessionId);
    }

    public QueryResult<Individual> getIndividual(long individualId, QueryOptions options, String sessionId)
            throws CatalogException {
        return individualManager.read(individualId, options, sessionId);
    }

    public QueryResult<Individual> getAllIndividuals(long studyId, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        return individualManager.readAll(studyId, query, options, sessionId);
    }

    public QueryResult<Individual> modifyIndividual(long individualId, QueryOptions options, String sessionId) throws CatalogException {
        return individualManager.update(individualId, options, options, sessionId);
    }

    public QueryResult<Individual> deleteIndividual(long individualId, QueryOptions options, String sessionId) throws CatalogException {
        return individualManager.delete(individualId, options, sessionId);
    }

    public QueryResult shareIndividual(String individualIds, String members, List<String> permissions, boolean override, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> individualList = individualManager.getIndividualIds(userId, individualIds);
        return authorizationManager.setIndividualPermissions(userId, individualList, members, permissions, override);
    }

    public QueryResult unshareIndividual(String individualIds, String userIds, String permissions, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> individualList = individualManager.getIndividualIds(userId, individualIds);
        List<String> permissionList = permissions != null && !permissions.isEmpty()
                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
        authorizationManager.unsetIndividualPermissions(userId, individualList, userIds, permissionList);
        return new QueryResult("unshareIndividual");
    }

    /*
     * Samples methods
     * ***************************
     */

    public QueryResult<Sample> createSample(long studyId, String name, String source, String description,
                                            Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        return sampleManager.create(studyId, name, source, description, attributes, options, sessionId);
    }

    public QueryResult<Sample> getSample(long sampleId, QueryOptions options, String sessionId)
            throws CatalogException {
        return sampleManager.read(sampleId, options, sessionId);
    }

    public QueryResult<Sample> getAllSamples(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.readAll(studyId, query, options, sessionId);
    }

    public QueryResult<Sample> modifySample(long sampleId, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return sampleManager.update(sampleId, queryOptions, queryOptions, sessionId);
    }

    @Deprecated
    public QueryResult shareSample(String sampleIds, String userIds, AclEntry acl, String sessionId)
            throws CatalogException {
        throw new CatalogException("Calling to deprecated shareSample method.");
//        return authorizationManager.setSampleACL(sampleIds, userIds, acl, sessionId);
    }

    public QueryResult shareSample(String sampleIds, String members, List<String> permissions, boolean override, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> sampleList = sampleManager.getSampleIds(userId, sampleIds);
        return authorizationManager.setSamplePermissions(userId, sampleList, members, permissions, override);
    }

    public QueryResult unshareSample(String sampleIds, String userIds, String permissions, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> sampleList = sampleManager.getSampleIds(userId, sampleIds);
        List<String> permissionList = permissions != null && !permissions.isEmpty()
                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
        authorizationManager.unsetSamplePermissions(userId, sampleList, userIds, permissionList);
        return new QueryResult("unshareSample");
    }

    public QueryResult<AnnotationSet> annotateSample(long sampleId, String id, long variableSetId,
                                                     Map<String, Object> annotations,
                                                     Map<String, Object> attributes,
                                                     String sessionId) throws CatalogException {
        return annotateSample(sampleId, id, variableSetId, annotations, attributes, true, sessionId);
    }

    public QueryResult<AnnotationSet> annotateSample(long sampleId, String annotationSetId, long variableSetId,
                                                     Map<String, Object> annotations,
                                                     Map<String, Object> attributes,
                                                     boolean checkAnnotationSet,
                                                     String sessionId)
            throws CatalogException {
        return sampleManager.annotate(sampleId, annotationSetId, variableSetId, annotations, attributes, checkAnnotationSet, sessionId);
    }

    public QueryResult<AnnotationSet> updateSampleAnnotation(long sampleId, String annotationSetId,
                                                             Map<String, Object> annotations,
                                                             String sessionId)
            throws CatalogException {
        return sampleManager.updateAnnotation(sampleId, annotationSetId, annotations, sessionId);
    }

    public QueryResult sampleGroupBy(Query query, QueryOptions qOptions, String fields, String sessionId) throws CatalogException {
        return sampleManager.groupBy(query, Arrays.asList(fields.split(",")), qOptions, sessionId);
    }


    public QueryResult<AnnotationSet> annotateIndividual(long individualId, String annotationSetId, long variableSetId,
                                                         Map<String, Object> annotations,
                                                         Map<String, Object> attributes,
                                                         String sessionId)
            throws CatalogException {
        return individualManager.annotate(individualId, annotationSetId, variableSetId, annotations, attributes, sessionId);
    }

    public QueryResult<AnnotationSet> updateIndividualAnnotation(long individualId, String annotationSetId,
                                                                 Map<String, Object> annotations,
                                                                 String sessionId)
            throws CatalogException {
        return individualManager.updateAnnotation(individualId, annotationSetId, annotations, sessionId);
    }

    public QueryResult<AnnotationSet> deleteSampleAnnotation(long sampleId, String annotationId, String sessionId)
            throws CatalogException {
        return sampleManager.deleteAnnotation(sampleId, annotationId, sessionId);
    }

    public QueryResult<AnnotationSet> deleteIndividualAnnotation(long individualId, String annotationId, String sessionId)
            throws CatalogException {
        return individualManager.deleteAnnotation(individualId, annotationId, sessionId);
    }

    public QueryResult<Sample> deleteSample(long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.delete(sampleId, options, sessionId);
    }

    public QueryResult individualGroupBy(Query query, QueryOptions qOptions, String fields, String sessionId) throws CatalogException {
        return individualManager.groupBy(query, Arrays.asList(fields.split(",")), qOptions, sessionId);
    }

    /*
     * VariableSet methods
     * ***************************
     */

    public QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique,
                                                      String description, Map<String, Object> attributes,
                                                      List<Variable> variables, String sessionId)
            throws CatalogException {
        return studyManager.createVariableSet(studyId, name, unique, description, attributes, variables, sessionId);
    }

    public QueryResult<VariableSet> createVariableSet(long studyId, String name, Boolean unique,
                                                      String description, Map<String, Object> attributes,
                                                      Set<Variable> variables, String sessionId)
            throws CatalogException {
        return studyManager.createVariableSet(studyId, name, unique, description, attributes, variables, sessionId);
    }

    public QueryResult<VariableSet> getVariableSet(long variableSet, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.readVariableSet(variableSet, options, sessionId);
    }

    public QueryResult<VariableSet> getAllVariableSet(long studyId, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.readAllVariableSets(studyId, options, sessionId);
    }

    public QueryResult<VariableSet> deleteVariableSet(long variableSetId, QueryOptions queryOptions, String sessionId)
            throws CatalogException {
        return studyManager.deleteVariableSet(variableSetId, queryOptions, sessionId);
    }

    public QueryResult<VariableSet> addFieldToVariableSet(long variableSetId, Variable variable, String sessionId)
            throws CatalogException {
        return studyManager.addFieldToVariableSet(variableSetId, variable, sessionId);
    }

    public QueryResult<VariableSet> renameFieldFromVariableSet(long variableSetId, String oldName, String newName, String sessionId)
            throws CatalogException {
        return studyManager.renameFieldFromVariableSet(variableSetId, oldName, newName, sessionId);
    }

    public QueryResult<VariableSet> removeFieldFromVariableSet(long variableSetId, String name, String sessionId)
            throws CatalogException {
        return studyManager.removeFieldFromVariableSet(variableSetId, name, sessionId);
    }

    /*
     * Cohort methods
     * ***************************
     */

    public long getStudyIdByCohortId(long cohortId) throws CatalogException {
        return sampleManager.getStudyIdByCohortId(cohortId);
    }

    public QueryResult<Cohort> getCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.readCohort(cohortId, options, sessionId);
    }

    public QueryResult<Cohort> getAllCohorts(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.readAllCohort(studyId, query, options, sessionId);
    }

    public QueryResult<Cohort> createCohort(long studyId, String name, Cohort.Type type, String description, List<Long> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException {
        return sampleManager.createCohort(studyId, name, type, description, sampleIds, attributes, sessionId);
    }

    public QueryResult<Cohort> modifyCohort(long cohortId, ObjectMap updateParams, QueryOptions options, String sessionId)
            throws CatalogException {
        return sampleManager.updateCohort(cohortId, updateParams, options, sessionId);
    }

    public QueryResult<Cohort> deleteCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.deleteCohort(cohortId, options, sessionId);
    }

    public QueryResult shareCohorts(String cohortIds, String members, List<String> permissions, boolean override, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> cohortList = sampleManager.getCohortIds(userId, cohortIds);
        return authorizationManager.setCohortPermissions(userId, cohortList, members, permissions, override);
    }

    public QueryResult unshareCohorts(String cohortIds, String userIds, String permissions, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> cohortList = sampleManager.getCohortIds(userId, cohortIds);
        List<String> permissionList = permissions != null && !permissions.isEmpty()
                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
        authorizationManager.unsetCohortPermissions(userId, cohortList, userIds, permissionList);
        return new QueryResult("unshareCohorts");
    }

    public long getCohortId(String cohortStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return sampleManager.getCohortId(userId, cohortStr);
    }

    public QueryResult<AnnotationSet> annotateCohort(String cohortId, String annotationSetId, long variableSetId,
                                                     Map<String, Object> annotations, Map<String, Object> attributes, String sessionId)
            throws CatalogException {
        return sampleManager.annotateCohort(cohortId, annotationSetId, variableSetId, annotations, attributes, true, sessionId);
    }

    public QueryResult<AnnotationSet> updateCohortAnnotation(String cohortId, String annotationSetId, Map<String, Object> annotations,
                                                             String sessionId) throws CatalogException {
        return sampleManager.updateCohortAnnotation(cohortId, annotationSetId, annotations, sessionId);
    }

    public QueryResult<AnnotationSet> deleteCohortAnnotation(String cohortId, String annotationId, String sessionId)
            throws CatalogException {
        return sampleManager.deleteCohortAnnotation(cohortId, annotationId, sessionId);
    }

    public QueryResult cohortGroupBy(Query query, QueryOptions qOptions, String fields, String sessionId) throws CatalogException {
        return sampleManager.cohortGroupBy(query, Arrays.asList(fields.split(",")), qOptions, sessionId);
    }

    /*
     * Tools methods
     * ***************************
     */

    public QueryResult<Tool> createTool(String alias, String description, Object manifest, Object result,
                                        String path, boolean openTool, String sessionId) throws CatalogException {
        return jobManager.createTool(alias, description, manifest, result, path, openTool, sessionId);
    }

    public QueryResult<Tool> getTool(long id, String sessionId) throws CatalogException {
        return jobManager.readTool(id, sessionId);
    }

    public QueryResult<Tool> getAllTools(Query query, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return jobManager.readAllTools(query, queryOptions, sessionId);
    }

    /*
    * Disease panel methods
    * ***************************
     */

    public QueryResult<DiseasePanel> createDiseasePanel(String studyStr, String name, String disease, String description,
                                                        String genes, String regions, String variants,
                                                        QueryOptions options, String sessionId) throws CatalogException {
        return studyManager.createDiseasePanel(studyStr, name, disease, description, genes, regions, variants, options, sessionId);
    }

    public QueryResult<DiseasePanel> getDiseasePanel(String panelStr, QueryOptions options, String sessionId) throws CatalogException {
        return studyManager.getDiseasePanel(panelStr, options, sessionId);
    }

    public QueryResult sharePanel(String panelIds, String members, List<String> permissions, boolean override, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> panelList = studyManager.getDiseasePanelIds(userId, panelIds);
        return authorizationManager.setDiseasePanelPermissions(userId, panelList, members, permissions, override);
    }

    public QueryResult unsharePanel(String panelIds, String userIds, String sessionId, String permissions) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        List<Long> panelList = studyManager.getDiseasePanelIds(userId, panelIds);
        List<String> permissionList = permissions != null && !permissions.isEmpty()
                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
        authorizationManager.unsetDiseasePanelPermissions(userId, panelList, userIds, permissionList);
        return new QueryResult("unsharePanel");
    }

}
