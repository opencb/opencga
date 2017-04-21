/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.audit.CatalogAuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.*;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.*;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.catalog.session.DefaultSessionManager;
import org.opencb.opencga.catalog.session.SessionManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CatalogManager implements AutoCloseable {

    protected static Logger logger = LoggerFactory.getLogger(CatalogManager.class);

    private DBAdaptorFactory catalogDBAdaptorFactory;
    private CatalogIOManagerFactory catalogIOManagerFactory;
//    private CatalogClient catalogClient;

    private IUserManager userManager;
    private IProjectManager projectManager;
    private IStudyManager studyManager;
    private IFileManager fileManager;
    private IJobManager jobManager;
    private IIndividualManager individualManager;
    private ISampleManager sampleManager;
    private ICohortManager cohortManager;
//    private AuthenticationManager authenticationManager;
    private CatalogAuditManager auditManager;
    private SessionManager sessionManager;
    private AuthorizationManager authorizationManager;

    private Configuration configuration;

    public CatalogManager(Configuration configuration) throws CatalogException {
        this.configuration = configuration;
        logger.debug("CatalogManager configureDBAdaptor");
        configureDBAdaptor(configuration);
        logger.debug("CatalogManager configureIOManager");
        configureIOManager(configuration);
        logger.debug("CatalogManager configureManager");
        configureManagers(configuration);
//        if (!catalogDBAdaptorFactory.isCatalogDBReady()) {
//            catalogDBAdaptorFactory.installCatalogDB(catalogConfiguration);
//            Admin admin = catalogConfiguration.getAdmin();
//            admin.setPassword(CatalogAuthenticationManager.cipherPassword(admin.getPassword()));
//            catalogDBAdaptorFactory.initializeCatalogDB(admin);
//        }
    }

    public String getCatalogDatabase() {
        String database;
        if (StringUtils.isNotEmpty(configuration.getDatabasePrefix())) {
            if (!configuration.getDatabasePrefix().endsWith("_")) {
                database = configuration.getDatabasePrefix() + "_catalog";
            } else {
                database = configuration.getDatabasePrefix() + "catalog";
            }
        } else {
            database = "opencga_catalog";
        }
        return database;
    }

    private void configureManagers(Configuration configuration) throws CatalogException {
//        catalogClient = new CatalogDBClient(this);
        //TODO: Check if catalog is empty
        //TODO: Setup catalog if it's empty.

        auditManager = new CatalogAuditManager(catalogDBAdaptorFactory.getCatalogAuditDbAdaptor(), catalogDBAdaptorFactory
                .getCatalogUserDBAdaptor(), authorizationManager, configuration);
//        authorizationManager = new CatalogAuthorizationManager(catalogDBAdaptorFactory, auditManager);
        authorizationManager = new CatalogAuthorizationManager(catalogDBAdaptorFactory, auditManager, this.configuration);
        sessionManager = new DefaultSessionManager(catalogDBAdaptorFactory);
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
        if (!ioManager.isDirectory(jobsURI) || ioManager.listFiles(jobsURI).size() > 0) {
            throw new CatalogException("Cannot install openCGA. Jobs folder is not empty.\nPlease, empty it first.");
        }
        catalogDBAdaptorFactory.installCatalogDB(configuration);
    }

    public void installIndexes() throws CatalogException {
        userManager.validatePassword("admin", configuration.getAdmin().getPassword(), true);
        catalogDBAdaptorFactory.createIndexes();
    }

    public void deleteCatalogDB(boolean force) throws CatalogException {
        if (!force) {
            userManager.validatePassword("admin", configuration.getAdmin().getPassword(), true);
        }
        catalogDBAdaptorFactory.deleteCatalogDB();
        clearCatalog();
    }

    private void clearCatalog() {
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

        Path rootdir = Paths.get(URI.create(configuration.getDataDir()));
        deleteFolderTree(rootdir.toFile());
        if (!configuration.getTempJobsDir().isEmpty()) {
            Path jobsDir = Paths.get(URI.create(configuration.getTempJobsDir()));
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

    private void configureIOManager(Configuration properties) throws CatalogIOException {
        catalogIOManagerFactory = new CatalogIOManagerFactory(properties);
    }

    private void configureDBAdaptor(Configuration configuration) throws CatalogDBException {

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", configuration.getCatalog().getDatabase().getUser())
                .add("password", configuration.getCatalog().getDatabase().getPassword())
                .add("authenticationDatabase", configuration.getCatalog().getDatabase().getOptions().get("authenticationDatabase"))
                .build();

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
        catalogDBAdaptorFactory = new MongoDBAdaptorFactory(dataStoreServerAddresses, mongoDBConfiguration,
                getCatalogDatabase()) {};
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
        return fileManager.getUri(file);
    }

    public URI getFileUri(Study study, File file) throws CatalogException {
        return fileManager.getUri(study, file);
    }

    public URI getFileUri(long studyId, String relativeFilePath)
            throws CatalogException {
        return fileManager.getUri(studyId, relativeFilePath);
    }

    @Deprecated
    public URI getFileUri(URI studyUri, String relativeFilePath)
            throws CatalogException {
        return fileManager.getUri(studyUri, relativeFilePath);
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
        return projectManager.getId(id);
    }

    @Deprecated
    public long getStudyId(String id) throws CatalogException {
        return studyManager.getId(id);
    }

    public List<Long> getStudyIds(String studyId, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return studyManager.getIds(userId, studyId);
    }

    public long getStudyId(String studyId, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return studyManager.getId(userId, studyId);
    }

    @Deprecated
    public long getFileId(String id) throws CatalogException {
        return fileManager.getId(id);
    }

    public long getFileId(String fileIdStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        AbstractManager.MyResourceId resource = fileManager.getId(fileIdStr, studyStr, sessionId);
        return resource.getResourceId();
    }

    public List<Long> getFileIds(String fileIds, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return fileManager.getIds(userId, fileIds);
    }

    public long getToolId(String id) throws CatalogException {
        return jobManager.getToolId(id);
    }

    /*
     * User methods
     * ***************************
     */

    public QueryResult<User> createUser(String id, String name, String email, String password, String organization, Long quota,
                                        QueryOptions options) throws CatalogException {
        return userManager.create(id, name, email, password, organization, quota, Account.FULL, options);
    }

    @Deprecated
    public QueryResult<ObjectMap> loginAsAnonymous(String sessionIp)
            throws CatalogException, IOException {
        return userManager.loginAsAnonymous(sessionIp);
    }

    public QueryResult<Session> login(String userId, String password, String sessionIp) throws CatalogException, IOException {
        return userManager.login(userId, password, sessionIp);
    }

    public QueryResult logout(String userId, String sessionId) throws CatalogException {
        return userManager.logout(userId, sessionId);
    }

    @Deprecated
    public QueryResult logoutAnonymous(String sessionId) throws CatalogException {
        return userManager.logoutAnonymous(sessionId);
    }

    public QueryResult changePassword(String userId, String oldPassword, String newPassword)
            throws CatalogException {
        userManager.changePassword(userId, oldPassword, newPassword);
        return new QueryResult("changePassword", 0, 0, 0, "", "", Collections.emptyList());
    }

    public QueryResult changeEmail(String userId, String nEmail, String sessionId) throws CatalogException {
        return userManager.update(userId, new ObjectMap("email", nEmail), null, sessionId);
    }

    public QueryResult<User> getUser(String userId, String lastModified, String sessionId) throws CatalogException {
        return getUser(userId, lastModified, new QueryOptions(), sessionId);
    }

    public QueryResult<User> getUser(String userId, String lastModified, QueryOptions options, String sessionId)
            throws CatalogException {
        return userManager.get(userId, lastModified, options, sessionId);
    }

    public String getUserIdBySessionId(String sessionId) throws CatalogException {
        return userManager.getId(sessionId);
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

    /*
     * Project methods
     * ***************************
     */

    @Deprecated
    public List<Long> getProjectIds(List<String> projectIds, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return projectManager.getIds(userId, projectIds);
    }

    public long getProjectId(String projectId, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return projectManager.getId(userId, projectId);
    }

    public QueryResult<Project> getProject(long projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        return projectManager.get(projectId, options, sessionId);
    }

    public QueryResult<Project> getAllProjects(String ownerId, QueryOptions options, String sessionId) throws CatalogException {
        return projectManager.get(new Query("ownerId", ownerId), options, sessionId);
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
                                          String sessionId) throws CatalogException {
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
        QueryResult<Study> result = studyManager.create(projectId, name, alias, type, creationDate, description, status, cipher, uriScheme,
                uri, datastores, stats, attributes, options, sessionId);
        return result;
    }

    public QueryResult<Study> getStudy(long studyId, String sessionId)
            throws CatalogException {
        return getStudy(studyId, null, sessionId);
    }

    public QueryResult<Study> getStudy(long studyId, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.get(studyId, options, sessionId);
    }

    public QueryResult<StudySummary> getStudySummary(long studyId, String sessionId, QueryOptions queryOptions) throws CatalogException {
        return studyManager.getSummary(studyId, sessionId, queryOptions);
    }

    public QueryResult<Study> getAllStudiesInProject(long projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.get(new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId), options, sessionId);
    }

    public QueryResult<Study> getAllStudies(Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.get(query, options, sessionId);
    }

    public QueryResult renameStudy(long studyId, String newStudyAlias, String sessionId)
            throws CatalogException {
        return studyManager.update(studyId, new ObjectMap("alias", newStudyAlias), null, sessionId);
    }

    public QueryResult createGroup(String studyId, String groupId, String userList, String sessionId) throws CatalogException {
        return studyManager.createGroup(studyId, groupId, userList, sessionId);
    }

    public QueryResult<Group> getGroup(String studyStr, String groupId, String sessionId) throws CatalogException {
        return studyManager.getGroup(studyStr, groupId, sessionId);
    }

    public QueryResult<Group> getAllGroups(String studyStr, String sessionId) throws CatalogException {
        return studyManager.getAllGroups(studyStr, sessionId);
    }

    public QueryResult<Group> updateGroup(String studyStr, String groupId, @Nullable String addUsers, @Nullable String removeUsers,
                                          @Nullable String setUsers, String sessionId) throws CatalogException {
        return studyManager.updateGroup(studyStr, groupId, addUsers, removeUsers, setUsers, sessionId);
    }

    public QueryResult<Group> deleteGroup(String studyStr, String groupId, String sessionId) throws CatalogException {
        return studyManager.deleteGroup(studyStr, groupId, sessionId);
    }

//    @Deprecated
//    public QueryResult addUsersToGroup(long studyId, String groupId, String userIds, String sessionId) throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        return authorizationManager.addUsersToGroup(userId, studyId, groupId, userIds);
//    }
//
//    @Deprecated
//    public QueryResult removeUsersFromGroup(long studyId, String groupId, String userIds, String sessionId) throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        authorizationManager.removeUsersFromGroup(userId, studyId, groupId, userIds);
//        return new QueryResult("removeUsersFromGroup");
//    }

    @Deprecated
    public QueryResult<StudyAclEntry> createStudyAcls(String studyStr, String members, String permissions, @Nullable String templateId,
                                                      String sessionId) throws CatalogException {
        Study.StudyAclParams aclParams = new Study.StudyAclParams(permissions, AclParams.Action.ADD, templateId);
        return studyManager.updateAcl(studyStr, members, aclParams, sessionId).get(0);
    }

    public QueryResult<StudyAclEntry> getAllStudyAcls(String studyStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);
        return authorizationManager.getAllStudyAcls(userId, studyId);
    }

    public QueryResult<StudyAclEntry> getStudyAcl(String studyStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);
        return authorizationManager.getStudyAcl(userId, studyId, member);
    }

    public QueryResult<StudyAclEntry> removeStudyAcl(String studyStr, String member, String sessionId) throws CatalogException {
        Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
        return studyManager.updateAcl(studyStr, member, aclParams, sessionId).get(0);
    }

//    @Deprecated
//    public QueryResult unshareStudy(long studyId, String members, String sessionId) throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        authorizationManager.removeStudyPermissions(userId, studyId, members);
//        return new QueryResult("unshareStudy");
//    }

    public List<QueryResult<SampleAclEntry>> getAllSampleAcls(String sampleIdsStr, @Nullable String studyStr, String sessionId)
            throws CatalogException {
        AbstractManager.MyResourceIds resourceId = sampleManager.getIds(sampleIdsStr, studyStr, sessionId);
        List<QueryResult<SampleAclEntry>> sampleAclList = new ArrayList<>(resourceId.getResourceIds().size());
        for (int i = 0; i < resourceId.getResourceIds().size(); i++) {
            Long sampleId = resourceId.getResourceIds().get(i);
            QueryResult<SampleAclEntry> allSampleAcls = authorizationManager.getAllSampleAcls(resourceId.getUser(), sampleId);
            allSampleAcls.setId(Long.toString(resourceId.getResourceIds().get(i)));
            sampleAclList.add(allSampleAcls);
        }
        return sampleAclList;
    }

    public QueryResult<SampleAclEntry> getSampleAcl(String sampleIdStr, @Nullable String studyStr, String member, String sessionId)
            throws CatalogException {
        AbstractManager.MyResourceId resourceId = sampleManager.getId(sampleIdStr, studyStr, sessionId);
        return authorizationManager.getSampleAcl(resourceId.getUser(), resourceId.getResourceId(), member);
    }

//    public QueryResult<SampleAclEntry> updateSampleAcl(String sampleIdStr, @Nullable String studyStr, String member,
//                                                       @Nullable String addPermissions, @Nullable String removePermissions,
//                                                       @Nullable String setPermissions, String sessionId) throws CatalogException {
//        AbstractManager.MyResourceId resourceId = sampleManager.getId(sampleIdStr, studyStr, sessionId);
//        return authorizationManager.updateSampleAcl(resourceId.getUser(), resourceId.getResourceId(), member, addPermissions,
//                removePermissions, setPermissions);
//    }

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

    public long getStudyIdByFileId(long fileId) throws CatalogException {
        return fileManager.getStudyId(fileId);
    }

    //create file with byte[]
    public QueryResult<File> createFile(long studyId, File.Format format, File.Bioformat bioformat, String path, byte[] bytes, String
            description, boolean parents, String sessionId) throws CatalogException, IOException {
        QueryResult<File> queryResult = fileManager.create(Long.toString(studyId), File.Type.FILE, format, bioformat, path, null,
                description, new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, parents, null, null, sessionId);
        new CatalogFileUtils(this).upload(new ByteArrayInputStream(bytes), queryResult.first(), sessionId, false, false, true);
        return getFile(queryResult.first().getId(), sessionId);
    }

    public QueryResult<File> createFile(long studyId, File.Format format, File.Bioformat bioformat, String path, URI fileLocation, String
            description, boolean parents, String sessionId) throws CatalogException, IOException {
        QueryResult<File> queryResult = fileManager.create(Long.toString(studyId), File.Type.FILE, format, bioformat, path, null,
                description, new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, parents, null, null, sessionId);
        new CatalogFileUtils(this).upload(fileLocation, queryResult.first(), null, sessionId, false, false, true, true, Long.MAX_VALUE);
        return getFile(queryResult.first().getId(), sessionId);
    }

    public QueryResult<File> createFile(long studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                        boolean parents, long jobId, String sessionId) throws CatalogException {
        return fileManager.create(Long.toString(studyId), File.Type.FILE, format, bioformat, path, null, description, null, 0, -1, null,
                jobId, null, null, parents, null, null, sessionId);
    }


    public QueryResult<File> createFile(long studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                        String creationDate, String description, File.FileStatus status, long size, long experimentId,
                                        List<Long> sampleIds, long jobId, Map<String, Object> stats, Map<String, Object> attributes,
                                        boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.create(Long.toString(studyId), type, format, bioformat, path, creationDate, description, status,
                size, experimentId, sampleIds, jobId, stats, attributes, parents, null, options, sessionId);
    }

    @Deprecated
    public QueryResult moveFile(long fileId, String newPath, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.move(fileId, newPath, options, sessionId);
    }

    @Deprecated
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
    @Deprecated
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
        return fileManager.get(fileId, options, sessionId);
    }

    public QueryResult<File> getAllFiles(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.get(studyId, query, options, sessionId);
    }

    public QueryResult<File> getAllFilesInFolder(long folderId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkId(folderId, "folderId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        long studyId = getStudyIdByFileId(folderId);
        File folder = getFile(folderId, sessionId).first();
        if (!folder.getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogDBException("File {id:" + folderId + ", path:'" + folder.getPath() + "'} is not a folder.");
        }
        Query query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), folder.getPath());
        return fileManager.get(studyId, query, options, sessionId);
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

//    @Deprecated
//    public QueryResult shareFile(String fileIds, String members, List<String> permissions, boolean override, String sessionId)
//            throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        List<Long> fileList = fileManager.getFileIds(userId, fileIds);
//        return authorizationManager.setFilePermissions(userId, fileList, members, permissions, override);
//    }

//    public QueryResult unshareFile(String fileIds, String userIds, String sessionId) throws CatalogException {
//        return authorizationManager.unsetFileACL(fileIds, userIds, sessionId);
//    }

//    @Deprecated
//    public QueryResult unshareFile(String fileIds, String members, String permissions, String sessionId) throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        List<Long> fileList = fileManager.getFileIds(userId, fileIds);
//        List<String> permissionList = permissions != null && !permissions.isEmpty()
//                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
//        authorizationManager.unsetFilePermissions(userId, fileList, members, permissionList);
//        return new QueryResult("unshareFile");
//    }

    public List<QueryResult<FileAclEntry>> getAllFileAcls(String fileIdsStr, @Nullable String studyStr, String sessionId)
            throws CatalogException {
        AbstractManager.MyResourceIds resource = fileManager.getIds(fileIdsStr, studyStr, sessionId);
        List<QueryResult<FileAclEntry>> aclList = new ArrayList<>(resource.getResourceIds().size());
        for (int i = 0; i < resource.getResourceIds().size(); i++) {
            Long fileId = resource.getResourceIds().get(i);
            QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(resource.getUser(), fileId);
            allFileAcls.setId(Long.toString(resource.getResourceIds().get(i)));
            aclList.add(allFileAcls);
        }
        return aclList;
    }

    public QueryResult<FileAclEntry> getFileAcl(String fileIdStr, @Nullable String studyStr, String member, String sessionId)
            throws CatalogException {
        AbstractManager.MyResourceId resource = fileManager.getId(fileIdStr, studyStr, sessionId);
        return authorizationManager.getFileAcl(resource.getUser(), resource.getResourceId(), member);
    }

    /*Require role admin*/
    public QueryResult<File> searchFile(Query query, QueryOptions options, String sessionId) throws CatalogException {
        return searchFile(-1, query, options, sessionId);
    }

    public QueryResult<File> searchFile(long studyId, Query query, String sessionId) throws CatalogException {
        return searchFile(studyId, query, null, sessionId);
    }

    public QueryResult<File> searchFile(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.get(studyId, query, options, sessionId);
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

    public QueryResult<File> link(URI uriOrigin, String pathDestiny, String studyIdStr, ObjectMap params, String sessionId)
            throws CatalogException, IOException {
        String userId = userManager.getId(sessionId);
        long studyId = studyManager.getId(userId, studyIdStr);
        return fileManager.link(uriOrigin, pathDestiny, studyId, params, sessionId);
    }

//    public QueryResult shareDatasets(String datasetIds, String members, List<String> permissions, String sessionId, boolean override)
//            throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        List<Long> datasetList = fileManager.getDatasetIds(userId, datasetIds);
//        return authorizationManager.setDatasetPermissions(userId, datasetList, members, permissions, override);
//    }
//
//    public QueryResult unshareDatasets(String datasetIds, String userIds, String sessionId, String permissions) throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        List<Long> datasetList = fileManager.getDatasetIds(userId, datasetIds);
//        List<String> permissionList = permissions != null && !permissions.isEmpty()
//                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
//        authorizationManager.unsetDatasetPermissions(userId, datasetList, userIds, permissionList);
//        return new QueryResult("unshareDatasets");
//    }

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

    public QueryResult<Job> getJob(long jobId, QueryOptions options, String sessionId) throws CatalogException {
        return jobManager.get(jobId, options, sessionId);
    }

    public QueryResult<Job> getUnfinishedJobs(String sessionId) throws CatalogException {
        return jobManager.get(new Query("status.name",
                Arrays.asList(
                        Job.JobStatus.PREPARED,
                        Job.JobStatus.QUEUED,
                        Job.JobStatus.RUNNING,
                        Job.JobStatus.DONE
                )
        ), null, sessionId);
    }


    public QueryResult<Job> getAllJobs(long studyId, String sessionId) throws CatalogException {
        return jobManager.get(studyId, null, null, sessionId);
    }

    public QueryResult<Job> getAllJobs(long studyId, Query query, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return jobManager.get(studyId, query, queryOptions, sessionId);
    }


    public QueryResult<Job> modifyJob(long jobId, ObjectMap parameters, String sessionId) throws CatalogException {
        return jobManager.update(jobId, parameters, null, sessionId); //TODO: Add query options
    }

    public QueryResult jobGroupBy(@Nullable String studyStr, Query query, QueryOptions qOptions, String fields, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return jobManager.groupBy(studyStr, query, Arrays.asList(fields.split(",")), qOptions, sessionId);
    }

    public List<QueryResult<JobAclEntry>> getAllJobAcls(String jobIdsStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] jobNameSplit = jobIdsStr.split(",");
        List<Long> jobIds = jobManager.getIds(userId, jobIdsStr);
        List<QueryResult<JobAclEntry>> aclList = new ArrayList<>(jobIds.size());
        for (int i = 0; i < jobIds.size(); i++) {
            Long jobId = jobIds.get(i);
            QueryResult<JobAclEntry> allJobAcls = authorizationManager.getAllJobAcls(userId, jobId);
            allJobAcls.setId(jobNameSplit[i]);
            aclList.add(allJobAcls);
        }
        return aclList;
    }

    public QueryResult<JobAclEntry> getJobAcl(String jobIdStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long jobId = jobManager.getId(userId, jobIdStr);
        return authorizationManager.getJobAcl(userId, jobId, member);
    }

    /*
     * Individual methods
     * ***************************
     */

    public long getIndividualId(String individualStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return individualManager.getId(userId, individualStr);
    }

    public List<Long> getIndividualIds(String individualStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return individualManager.getIds(userId, individualStr);
    }

    @Deprecated
    public QueryResult<Individual> createIndividual(long studyId, String name, String family, long fatherId, long motherId,
                                                    Individual.Sex sex, QueryOptions options, String sessionId)
            throws CatalogException {
        return individualManager.create(studyId, name, family, fatherId, motherId, sex, "", "", "", "", "", "", "",
                Individual.KaryotypicSex.UNKNOWN, Individual.LifeStatus.UNKNOWN, Individual.AffectationStatus.UNKNOWN, options, sessionId);
    }

    public QueryResult<Individual> getIndividual(long individualId, QueryOptions options, String sessionId)
            throws CatalogException {
        return individualManager.get(individualId, options, sessionId);
    }

    public QueryResult<Individual> getAllIndividuals(long studyId, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        return individualManager.get(studyId, query, options, sessionId);
    }

    public QueryResult<Individual> modifyIndividual(long individualId, QueryOptions options, String sessionId) throws CatalogException {
        return individualManager.update(individualId, options, options, sessionId);
    }

    public List<QueryResult<IndividualAclEntry>> getAllIndividualAcls(String individualIdsStr, @Nullable String studyStr, String sessionId)
            throws CatalogException {
        AbstractManager.MyResourceIds resource = individualManager.getIds(individualIdsStr, studyStr, sessionId);
        List<Long> individualIds = resource.getResourceIds();

        List<QueryResult<IndividualAclEntry>> aclList = new ArrayList<>(individualIds.size());
        for (int i = 0; i < individualIds.size(); i++) {
            Long individualId = individualIds.get(i);
            QueryResult<IndividualAclEntry> allIndividualAcls = authorizationManager.getAllIndividualAcls(resource.getUser(), individualId);
            allIndividualAcls.setId(Long.toString(individualId));
            aclList.add(allIndividualAcls);
        }
        return aclList;
    }

    public QueryResult<IndividualAclEntry> getIndividualAcl(String individualIdStr, @Nullable String studyStr, String member,
                                                            String sessionId) throws CatalogException {
        AbstractManager.MyResourceId resource = individualManager.getId(individualIdStr, studyStr, sessionId);
        return authorizationManager.getIndividualAcl(resource.getUser(), resource.getResourceId(), member);
    }

    /*
     * Samples methods
     * ***************************
     */

    @Deprecated
    public QueryResult<Sample> createSample(long studyId, String name, String source, String description,
                                            Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        return sampleManager.create(Long.toString(studyId), name, source, description, null, attributes, options, sessionId);
    }

    public long getSampleId(String sampleId, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return sampleManager.getId(userId, sampleId);
    }

    public List<Long> getSampleIds(String sampleIds, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return sampleManager.getIds(userId, sampleIds);
    }

    public QueryResult<Sample> getSample(long sampleId, QueryOptions options, String sessionId)
            throws CatalogException {
        return sampleManager.get(sampleId, options, sessionId);
    }

    public QueryResult<Sample> getAllSamples(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.get(studyId, query, options, sessionId);
    }

    @Deprecated
    public QueryResult<Sample> modifySample(long sampleId, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return sampleManager.update(sampleId, queryOptions, queryOptions, sessionId);
    }

    @Deprecated
    public QueryResult<AnnotationSet> annotateSample(long sampleId, String id, long variableSetId,
                                                     Map<String, Object> annotations,
                                                     Map<String, Object> attributes,
                                                     String sessionId) throws CatalogException {
        return annotateSample(sampleId, id, variableSetId, annotations, attributes, true, sessionId);
    }

    @Deprecated
    public QueryResult<AnnotationSet> annotateSample(long sampleId, String annotationSetName, long variableSetId,
                                                     Map<String, Object> annotations,
                                                     Map<String, Object> attributes,
                                                     boolean checkAnnotationSet,
                                                     String sessionId)
            throws CatalogException {
        return sampleManager.annotate(sampleId, annotationSetName, variableSetId, annotations, attributes, checkAnnotationSet, sessionId);
    }

    @Deprecated
    public QueryResult<AnnotationSet> updateSampleAnnotation(long sampleId, String annotationSetName,
                                                             Map<String, Object> annotations,
                                                             String sessionId)
            throws CatalogException {
        return sampleManager.updateAnnotation(sampleId, annotationSetName, annotations, sessionId);
    }

    public QueryResult<AnnotationSet> annotateIndividual(long individualId, String annotationSetName, long variableSetId,
                                                         Map<String, Object> annotations,
                                                         Map<String, Object> attributes,
                                                         String sessionId)
            throws CatalogException {
        return individualManager.annotate(individualId, annotationSetName, variableSetId, annotations, attributes, sessionId);
    }

    public QueryResult<AnnotationSet> updateIndividualAnnotation(long individualId, String annotationSetName,
                                                                 Map<String, Object> annotations,
                                                                 String sessionId)
            throws CatalogException {
        return individualManager.updateAnnotation(individualId, annotationSetName, annotations, sessionId);
    }

    @Deprecated
    public QueryResult<Sample> deleteSample(long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        return null;
//        return sampleManager.delete(sampleId, options, sessionId);
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
        return cohortManager.getStudyId(cohortId);
    }

    public QueryResult<Cohort> getCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        return cohortManager.get(cohortId, options, sessionId);
    }

    public QueryResult<Cohort> getAllCohorts(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return cohortManager.get(studyId, query, options, sessionId);
    }

    public QueryResult<Cohort> createCohort(long studyId, String name, Study.Type type, String description, List<Long> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException {
        return cohortManager.create(studyId, name, type, description, sampleIds, attributes, sessionId);
    }

    public QueryResult<Cohort> modifyCohort(long cohortId, ObjectMap updateParams, QueryOptions options, String sessionId)
            throws CatalogException {
        return cohortManager.update(cohortId, updateParams, options, sessionId);
    }

    public List<QueryResult<CohortAclEntry>> getAllCohortAcls(String cohortIdsStr, @Nullable String studyStr, String sessionId)
            throws CatalogException {
        AbstractManager.MyResourceIds resource = cohortManager.getIds(cohortIdsStr, studyStr, sessionId);
        List<Long> cohortIds = resource.getResourceIds();
        List<QueryResult<CohortAclEntry>> aclList = new ArrayList<>(cohortIds.size());
        for (int i = 0; i < cohortIds.size(); i++) {
            Long cohortId = cohortIds.get(i);
            QueryResult<CohortAclEntry> allCohortAcls = authorizationManager.getAllCohortAcls(resource.getUser(), cohortId);
            allCohortAcls.setId(Long.toString(cohortId));
            aclList.add(allCohortAcls);
        }
        return aclList;
    }

    public QueryResult<CohortAclEntry> getCohortAcl(String cohortIdStr, @Nullable String studyStr, String member, String sessionId)
            throws CatalogException {
        AbstractManager.MyResourceId resource = cohortManager.getId(cohortIdStr, studyStr, sessionId);
        return authorizationManager.getCohortAcl(resource.getUser(), resource.getResourceId(), member);
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

    public IUserManager getUserManager() {
        return userManager;
    }

    public IProjectManager getProjectManager() {
        return projectManager;
    }

    public IStudyManager getStudyManager() {
        return studyManager;
    }

    public IFileManager getFileManager() {
        return fileManager;
    }

    public IJobManager getJobManager() {
        return jobManager;
    }

    public IIndividualManager getIndividualManager() {
        return individualManager;
    }

    public ISampleManager getSampleManager() {
        return sampleManager;
    }

    public ICohortManager getCohortManager() {
        return cohortManager;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public AuthorizationManager getAuthorizationManager() {
        return authorizationManager;
    }
}
