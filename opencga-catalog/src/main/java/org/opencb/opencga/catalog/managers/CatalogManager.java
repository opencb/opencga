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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.audit.CatalogAuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.*;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.*;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.catalog.session.DefaultSessionManager;
import org.opencb.opencga.catalog.session.SessionManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
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

    private CatalogConfiguration catalogConfiguration;

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
//            Admin admin = catalogConfiguration.getAdmin();
//            admin.setPassword(CatalogAuthenticationManager.cipherPassword(admin.getPassword()));
//            catalogDBAdaptorFactory.initializeCatalogDB(admin);
//        }
    }

    public String getCatalogDatabase() {
        String database;
        if (StringUtils.isNotEmpty(catalogConfiguration.getDatabasePrefix())) {
            if (!catalogConfiguration.getDatabasePrefix().endsWith("_")) {
                database = catalogConfiguration.getDatabasePrefix() + "_catalog";
            } else {
                database = catalogConfiguration.getDatabasePrefix() + "catalog";
            }
        } else {
            database = "opencga_catalog";
        }
        return database;
    }

    private void configureManagers(CatalogConfiguration catalogConfiguration) {
//        catalogClient = new CatalogDBClient(this);
        //TODO: Check if catalog is empty
        //TODO: Setup catalog if it's empty.

        auditManager = new CatalogAuditManager(catalogDBAdaptorFactory.getCatalogAuditDbAdaptor(), catalogDBAdaptorFactory
                .getCatalogUserDBAdaptor(), authorizationManager, catalogConfiguration);
        authorizationManager = new CatalogAuthorizationManager(catalogDBAdaptorFactory, auditManager);
        sessionManager = new DefaultSessionManager(catalogDBAdaptorFactory);
        userManager = new UserManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        fileManager = new FileManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        studyManager = new StudyManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        projectManager = new ProjectManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        jobManager = new JobManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, this.catalogConfiguration);
        sampleManager = new SampleManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        individualManager = new IndividualManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
                catalogIOManagerFactory, catalogConfiguration);
        cohortManager = new CohortManager(authorizationManager, auditManager, this, catalogDBAdaptorFactory,
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
        userManager.validatePassword("admin", catalogConfiguration.getAdmin().getPassword(), true);
        catalogDBAdaptorFactory.createIndexes();
    }

    public void deleteCatalogDB(boolean force) throws CatalogException {
        if (!force) {
            userManager.validatePassword("admin", catalogConfiguration.getAdmin().getPassword(), true);
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
//        MongoDataStore db = mongoManager.get(catalogConfiguration.getDatabase().getDatabase());
        MongoDataStore db = mongoManager.get(getCatalogDatabase());
        db.getDb().drop();
//        mongoManager.close(catalogConfiguration.getDatabase().getDatabase());
        mongoManager.close(getCatalogDatabase());

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

    private void configureIOManager(CatalogConfiguration properties) throws CatalogIOException {
        catalogIOManagerFactory = new CatalogIOManagerFactory(properties);
    }

    private void configureDBAdaptor(CatalogConfiguration catalogConfiguration) throws CatalogDBException {

        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder()
                .add("username", catalogConfiguration.getDatabase().getUser())
                .add("password", catalogConfiguration.getDatabase().getPassword())
                .add("authenticationDatabase", catalogConfiguration.getDatabase().getOptions().get("authenticationDatabase"))
                .build();

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

    public long getFileId(String fileId, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return fileManager.getId(userId, fileId);
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

    public QueryResult resetPassword(String userId) throws CatalogException {
        return userManager.resetPassword(userId);
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

    public QueryResult<Project> createProject(String name, String alias, String description, String organization, QueryOptions options,
                                              String sessionId) throws CatalogException {
        return projectManager.create(name, alias, description, organization, options, sessionId);
    }

    public List<Long> getProjectIds(String projectIds, String sessionId) throws CatalogException {
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
//        createFolder(result.getResult().get(0).getId(), Paths.get("data"), true, null, sessionId);
//        createFolder(result.getResult().get(0).getId(), Paths.get("analysis"), true, null, sessionId);
        //}
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

    public QueryResult<StudyAclEntry> createStudyAcls(String studyStr, String members, String permissions, @Nullable String templateId,
                                                      String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);
        return authorizationManager.createStudyAcls(userId, studyId, members, permissions, templateId);
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

    public QueryResult<StudyAclEntry> updateStudyAcl(String studyStr, String member, @Nullable String addPermissions,
                                                     @Nullable String removePermissions, @Nullable String setPermissions, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);
        return authorizationManager.updateStudyAcl(userId, studyId, member, addPermissions, removePermissions, setPermissions);

    }

    public QueryResult<StudyAclEntry> removeStudyAcl(String studyStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long studyId = studyManager.getId(userId, studyStr);
        return authorizationManager.removeStudyAcl(userId, studyId, member);
    }

//    @Deprecated
//    public QueryResult unshareStudy(long studyId, String members, String sessionId) throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        authorizationManager.removeStudyPermissions(userId, studyId, members);
//        return new QueryResult("unshareStudy");
//    }

    @Deprecated
    public QueryResult<StudyAclEntry> getStudyAcls(String studyStr, List<String> members, String sessionId) throws CatalogException {
        return studyManager.getAcls(studyStr, members, sessionId);
    }

    public List<QueryResult<SampleAclEntry>> getAllSampleAcls(String sampleIdsStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] sampleNameSplit = sampleIdsStr.split(",");
        List<Long> sampleIds = sampleManager.getIds(userId, sampleIdsStr);
        List<QueryResult<SampleAclEntry>> sampleAclList = new ArrayList<>(sampleIds.size());
        for (int i = 0; i < sampleIds.size(); i++) {
            Long sampleId = sampleIds.get(i);
            QueryResult<SampleAclEntry> allSampleAcls = authorizationManager.getAllSampleAcls(userId, sampleId);
            allSampleAcls.setId(sampleNameSplit[i]);
            sampleAclList.add(allSampleAcls);
        }
        return sampleAclList;
    }

    public List<QueryResult<SampleAclEntry>> createSampleAcls(String sampleIdsStr, String members, String permissions, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] sampleNameSplit = sampleIdsStr.split(",");
        List<Long> sampleIds = sampleManager.getIds(userId, sampleIdsStr);
        List<QueryResult<SampleAclEntry>> sampleAclList = new ArrayList<>(sampleIds.size());
        for (int i = 0; i < sampleIds.size(); i++) {
            Long sampleId = sampleIds.get(i);
            QueryResult<SampleAclEntry> sampleAcls = authorizationManager.createSampleAcls(userId, sampleId, members, permissions);
            sampleAcls.setId(sampleNameSplit[i]);
            sampleAclList.add(sampleAcls);
        }
        return sampleAclList;
    }

    public List<QueryResult<SampleAclEntry>> removeSampleAcl(String sampleIdsStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] sampleNameSplit = sampleIdsStr.split(",");
        List<Long> sampleIds = sampleManager.getIds(userId, sampleIdsStr);
        List<QueryResult<SampleAclEntry>> sampleAclList = new ArrayList<>(sampleIds.size());
        for (int i = 0; i < sampleIds.size(); i++) {
            Long sampleId = sampleIds.get(i);
            QueryResult<SampleAclEntry> sampleAcls = authorizationManager.removeSampleAcl(userId, sampleId, member);
            sampleAcls.setId(sampleNameSplit[i]);
            sampleAclList.add(sampleAcls);
        }
        return sampleAclList;
    }

    public QueryResult<SampleAclEntry> getSampleAcl(String sampleIdStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long sampleId = sampleManager.getId(userId, sampleIdStr);
        return authorizationManager.getSampleAcl(userId, sampleId, member);
    }

    public QueryResult<SampleAclEntry> updateSampleAcl(String sampleIdStr, String member, @Nullable String addPermissions,
                                                       @Nullable String removePermissions, @Nullable String setPermissions,
                                                       String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long sampleId = sampleManager.getId(userId, sampleIdStr);
        return authorizationManager.updateSampleAcl(userId, sampleId, member, addPermissions, removePermissions, setPermissions);

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

    public long getStudyIdByFileId(long fileId) throws CatalogException {
        return fileManager.getStudyId(fileId);
    }

    //create file with byte[]
    public QueryResult<File> createFile(long studyId, File.Format format, File.Bioformat bioformat, String path, byte[] bytes, String
            description, boolean parents, String sessionId) throws CatalogException, IOException {
        QueryResult<File> queryResult = fileManager.create(studyId, File.Type.FILE, format, bioformat, path, null,
                description, new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, parents, null, sessionId);
        new CatalogFileUtils(this).upload(new ByteArrayInputStream(bytes), queryResult.first(), sessionId, false, false, true);
        return getFile(queryResult.first().getId(), sessionId);
    }

    public QueryResult<File> createFile(long studyId, File.Format format, File.Bioformat bioformat, String path, URI fileLocation, String
            description, boolean parents, String sessionId) throws CatalogException, IOException {
        QueryResult<File> queryResult = fileManager.create(studyId, File.Type.FILE, format, bioformat, path, null,
                description, new File.FileStatus(File.FileStatus.STAGE), 0, -1, null, -1, null, null, parents, null, sessionId);
        new CatalogFileUtils(this).upload(fileLocation, queryResult.first(), null, sessionId, false, false, true, true, Long.MAX_VALUE);
        return getFile(queryResult.first().getId(), sessionId);
    }

    public QueryResult<File> createFile(long studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                        boolean parents, long jobId, String sessionId) throws CatalogException {
        return fileManager.create(studyId, File.Type.FILE, format, bioformat, path, null, description, null, 0, -1, null,
                jobId, null, null, parents, null, sessionId);
    }


    public QueryResult<File> createFile(long studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                        String creationDate, String description, File.FileStatus status, long diskUsage, long experimentId,
                                        List<Long> sampleIds, long jobId, Map<String, Object> stats, Map<String, Object> attributes,
                                        boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.create(studyId, type, format, bioformat, path, creationDate, description, status,
                diskUsage, experimentId, sampleIds, jobId, stats, attributes, parents, options, sessionId);
    }

    public QueryResult<File> createFolder(long studyId, Path folderPath, boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.createFolder(studyId, folderPath.toString(), null, parents, null, options, sessionId);
    }

    public QueryResult<File> createFolder(long studyId, Path folderPath, File.FileStatus status, boolean parents, String description,
                                          QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkPath(folderPath, "folderPath");
        return fileManager.createFolder(studyId, folderPath.toString(), status, parents, description, options, sessionId);
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
        return fileManager.get(fileId, options, sessionId);
    }

    public QueryResult<File> getAllFiles(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.get(studyId, query, options, sessionId);
    }

    public QueryResult<File> getAllFilesInFolder(long folderId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
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

    public List<QueryResult<FileAclEntry>> getAllFileAcls(String fileIdsStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] fileNameSplit = fileIdsStr.split(",");
        List<Long> fileIds = fileManager.getIds(userId, fileIdsStr);
        List<QueryResult<FileAclEntry>> aclList = new ArrayList<>(fileIds.size());
        for (int i = 0; i < fileIds.size(); i++) {
            Long fileId = fileIds.get(i);
            QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(userId, fileId);
            allFileAcls.setId(fileNameSplit[i]);
            aclList.add(allFileAcls);
        }
        return aclList;
    }

    public List<QueryResult<FileAclEntry>> createFileAcls(String fileIdsStr, String members, String permissions, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] fileNameSplit = fileIdsStr.split(",");
        List<Long> fileIds = fileManager.getIds(userId, fileIdsStr);
        List<QueryResult<FileAclEntry>> fileAclList = new ArrayList<>(fileIds.size());
        for (int i = 0; i < fileIds.size(); i++) {
            Long fileId = fileIds.get(i);
            QueryResult<FileAclEntry> fileAcls = authorizationManager.createFileAcls(userId, fileId, members, permissions);
            fileAcls.setId(fileNameSplit[i]);
            fileAclList.add(fileAcls);
        }
        return fileAclList;
    }

    public List<QueryResult<FileAclEntry>> removeFileAcl(String fileIdsStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] fileNameSplit = fileIdsStr.split(",");
        List<Long> fileIds = fileManager.getIds(userId, fileIdsStr);
        List<QueryResult<FileAclEntry>> fileAclList = new ArrayList<>(fileIds.size());
        for (int i = 0; i < fileIds.size(); i++) {
            Long fileId = fileIds.get(i);
            QueryResult<FileAclEntry> fileAcls = authorizationManager.removeFileAcl(userId, fileId, member);
            fileAcls.setId(fileNameSplit[i]);
            fileAclList.add(fileAcls);
        }
        return fileAclList;
    }

    public QueryResult<FileAclEntry> getFileAcl(String fileIdStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long fileId = fileManager.getId(userId, fileIdStr);
        return authorizationManager.getFileAcl(userId, fileId, member);
    }

    public QueryResult<FileAclEntry> updateFileAcl(String fileIdStr, String member, @Nullable String addPermissions,
                                                   @Nullable String removePermissions, @Nullable String setPermissions, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long fileId = fileManager.getId(userId, fileIdStr);
        return authorizationManager.updateFileAcl(userId, fileId, member, addPermissions, removePermissions, setPermissions);

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

    public QueryResult<File> unlink(String fileIdStr, QueryOptions qOptions, String sessionId) throws CatalogException, IOException {
        return fileManager.unlink(fileIdStr, qOptions, sessionId);
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

    public QueryResult jobGroupBy(Query query, QueryOptions qOptions, String fields, String sessionId) throws CatalogException {
        return jobManager.groupBy(query, Arrays.asList(fields.split(",")), qOptions, sessionId);
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

    public List<QueryResult<JobAclEntry>> createJobAcls(String jobIdsStr, String members, String permissions,
                                                        String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] jobNameSplit = jobIdsStr.split(",");
        List<Long> jobIds = jobManager.getIds(userId, jobIdsStr);
        List<QueryResult<JobAclEntry>> jobAclList = new ArrayList<>(jobIds.size());
        for (int i = 0; i < jobIds.size(); i++) {
            Long jobId = jobIds.get(i);
            QueryResult<JobAclEntry> jobAcls = authorizationManager.createJobAcls(userId, jobId, members,
                    permissions);
            jobAcls.setId(jobNameSplit[i]);
            jobAclList.add(jobAcls);
        }
        return jobAclList;
    }

    public List<QueryResult<JobAclEntry>> removeJobAcl(String jobIdsStr, String member, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] jobNameStr = jobIdsStr.split(",");
        List<Long> jobIds = jobManager.getIds(userId, jobIdsStr);
        List<QueryResult<JobAclEntry>> jobAclList = new ArrayList<>(jobIds.size());
        for (int i = 0; i < jobIds.size(); i++) {
            Long jobId = jobIds.get(i);
            QueryResult<JobAclEntry> jobAcls = authorizationManager.removeJobAcl(userId, jobId, member);
            jobAcls.setId(jobNameStr[i]);
            jobAclList.add(jobAcls);
        }
        return jobAclList;
    }

    public QueryResult<JobAclEntry> getJobAcl(String jobIdStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long jobId = jobManager.getId(userId, jobIdStr);
        return authorizationManager.getJobAcl(userId, jobId, member);
    }

    public QueryResult<JobAclEntry> updateJobAcl(String jobIdStr, String member, @Nullable String addPermissions,
                                                 @Nullable String removePermissions, @Nullable String setPermissions, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long jobId = jobManager.getId(userId, jobIdStr);
        return authorizationManager.updateJobAcl(userId, jobId, member, addPermissions, removePermissions, setPermissions);
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

    public QueryResult<Individual> createIndividual(long studyId, String name, String family, long fatherId, long motherId,
                                                    Individual.Sex sex, QueryOptions options, String sessionId)
            throws CatalogException {
        return individualManager.create(studyId, name, family, fatherId, motherId, sex, options, sessionId);
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

//    @Deprecated
//    public QueryResult shareIndividual(String individualIds, String members, List<String> permissions, boolean override, String sessionId)
//            throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        List<Long> individualList = individualManager.getIndividualIds(userId, individualIds);
//        return authorizationManager.setIndividualPermissions(userId, individualList, members, permissions, override);
//    }
//
//    @Deprecated
//    public QueryResult unshareIndividual(String individualIds, String userIds, String permissions, String sessionId)
//            throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        List<Long> individualList = individualManager.getIndividualIds(userId, individualIds);
//        List<String> permissionList = permissions != null && !permissions.isEmpty()
//                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
//        authorizationManager.unsetIndividualPermissions(userId, individualList, userIds, permissionList);
//        return new QueryResult("unshareIndividual");
//    }

    public List<QueryResult<IndividualAclEntry>> getAllIndividualAcls(String individualIdsStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] individualNameSplit = individualIdsStr.split(",");
        List<Long> individualIds = individualManager.getIds(userId, individualIdsStr);
        List<QueryResult<IndividualAclEntry>> aclList = new ArrayList<>(individualIds.size());
        for (int i = 0; i < individualIds.size(); i++) {
            Long individualId = individualIds.get(i);
            QueryResult<IndividualAclEntry> allIndividualAcls = authorizationManager.getAllIndividualAcls(userId, individualId);
            allIndividualAcls.setId(individualNameSplit[i]);
            aclList.add(allIndividualAcls);
        }
        return aclList;
    }

    public List<QueryResult<IndividualAclEntry>> createIndividualAcls(String individualIdsStr, String members, String permissions,
                                                                      String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] individualNameSplit = individualIdsStr.split(",");
        List<Long> individualIds = individualManager.getIds(userId, individualIdsStr);
        List<QueryResult<IndividualAclEntry>> individualAclList = new ArrayList<>(individualIds.size());
        for (int i = 0; i < individualIds.size(); i++) {
            Long individualId = individualIds.get(i);
            QueryResult<IndividualAclEntry> individualAcls = authorizationManager.createIndividualAcls(userId, individualId, members,
                    permissions);
            individualAcls.setId(individualNameSplit[i]);
            individualAclList.add(individualAcls);
        }
        return individualAclList;
    }

    public List<QueryResult<IndividualAclEntry>> removeIndividualAcl(String individualIdsStr, String member, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] individualNameStr = individualIdsStr.split(",");
        List<Long> individualIds = individualManager.getIds(userId, individualIdsStr);
        List<QueryResult<IndividualAclEntry>> individualAclList = new ArrayList<>(individualIds.size());
        for (int i = 0; i < individualIds.size(); i++) {
            Long individualId = individualIds.get(i);
            QueryResult<IndividualAclEntry> individualAcls = authorizationManager.removeIndividualAcl(userId, individualId, member);
            individualAcls.setId(individualNameStr[i]);
            individualAclList.add(individualAcls);
        }
        return individualAclList;
    }

    public QueryResult<IndividualAclEntry> getIndividualAcl(String individualIdStr, String member, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long individualId = individualManager.getId(userId, individualIdStr);
        return authorizationManager.getIndividualAcl(userId, individualId, member);
    }

    public QueryResult<IndividualAclEntry> updateIndividualAcl(String individualIdStr, String member, @Nullable String addPermissions,
                                                               @Nullable String removePermissions, @Nullable String setPermissions,
                                                               String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long individualId = individualManager.getId(userId, individualIdStr);
        return authorizationManager.updateIndividualAcl(userId, individualId, member, addPermissions, removePermissions, setPermissions);
    }

    public QueryResult<AnnotationSet> createIndividualAnnotationSet(String individualIdStr, long variableSetId, String annotationSetName,
                                                                    Map<String, Object> annotations, Map<String, Object> attributes,
                                                                    String sessionId) throws CatalogException {
        return individualManager.createAnnotationSet(individualIdStr, variableSetId, annotationSetName, annotations, attributes, sessionId);
    }

    public QueryResult<AnnotationSet> getAllIndividualAnnotationSets(String individualIdStr, String sessionId) throws CatalogException {
        return individualManager.getAllAnnotationSets(individualIdStr, sessionId);
    }

    public QueryResult<AnnotationSet> getIndividualAnnotationSet(String individualIdStr, String annotationSetName, String sessionId)
            throws CatalogException {
        return individualManager.getAnnotationSet(individualIdStr, annotationSetName, sessionId);
    }

    public QueryResult<AnnotationSet> updateIndividualAnnotationSet(String individualIdStr, String annotationSetName,
                                                                    Map<String, Object> newAnnotations, String sessionId)
            throws CatalogException {
        return individualManager.updateAnnotationSet(individualIdStr, annotationSetName, newAnnotations, sessionId);
    }

    public QueryResult<AnnotationSet> deleteIndividualAnnotationSet(String individualIdStr, String annotationSetName, String sessionId)
            throws CatalogException {
        return individualManager.deleteAnnotationSet(individualIdStr, annotationSetName, sessionId);
    }

    public QueryResult<AnnotationSet> deleteIndividualAnnotations(String individualIdStr, String annotationSetName, String annotations,
                                                                  String sessionId) throws CatalogException {
        return individualManager.deleteAnnotations(individualIdStr, annotationSetName, annotations, sessionId);
    }

    public QueryResult<AnnotationSet> searchIndividualAnnotationSets(String individualIdStr, long variableSetId,
                                                                     @Nullable String annotation, String sessionId)
            throws CatalogException {
        return individualManager.searchAnnotationSet(individualIdStr, variableSetId, annotation, sessionId);
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

    public QueryResult<Sample> modifySample(long sampleId, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return sampleManager.update(sampleId, queryOptions, queryOptions, sessionId);
    }

    @Deprecated
    public QueryResult shareSample(String sampleIds, String userIds, AclEntry acl, String sessionId)
            throws CatalogException {
        throw new CatalogException("Calling to deprecated shareSample method.");
//        return authorizationManager.setSampleACL(sampleIds, userIds, acl, sessionId);
    }

//    public QueryResult shareSample(String sampleIds, String members, List<String> permissions, boolean override, String sessionId)
//            throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        List<Long> sampleList = sampleManager.getSampleIds(userId, sampleIds);
//        return authorizationManager.setSamplePermissions(userId, sampleList, members, permissions, override);
//    }
//
//    public QueryResult unshareSample(String sampleIds, String userIds, String permissions, String sessionId)
//            throws CatalogException {
//        String userId = getUserIdBySessionId(sessionId);
//        List<Long> sampleList = sampleManager.getSampleIds(userId, sampleIds);
//        List<String> permissionList = permissions != null && !permissions.isEmpty()
//                ? Arrays.asList(permissions.split(",")) : Collections.emptyList();
//        authorizationManager.unsetSamplePermissions(userId, sampleList, userIds, permissionList);
//        return new QueryResult("unshareSample");
//    }

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

    public QueryResult<AnnotationSet> createSampleAnnotationSet(String sampleIdStr, long variableSetId, String annotationSetName,
                                                                Map<String, Object> annotations, Map<String, Object> attributes,
                                                                String sessionId) throws CatalogException {
        return sampleManager.createAnnotationSet(sampleIdStr, variableSetId, annotationSetName, annotations, attributes, sessionId);
    }

    public QueryResult<AnnotationSet> getAllSampleAnnotationSets(String sampleIdStr, String sessionId) throws CatalogException {
        return sampleManager.getAllAnnotationSets(sampleIdStr, sessionId);
    }

    public QueryResult<AnnotationSet> getSampleAnnotationSet(String sampleIdStr, String annotationSetName, String sessionId)
            throws CatalogException {
        return sampleManager.getAnnotationSet(sampleIdStr, annotationSetName, sessionId);
    }

    public QueryResult<AnnotationSet> updateSampleAnnotationSet(String sampleIdStr, String annotationSetName,
                                                                Map<String, Object> newAnnotations, String sessionId)
            throws CatalogException {
        return sampleManager.updateAnnotationSet(sampleIdStr, annotationSetName, newAnnotations, sessionId);
    }

    public QueryResult<AnnotationSet> deleteSampleAnnotationSet(String sampleIdStr, String annotationSetName, String sessionId)
            throws CatalogException {
        return sampleManager.deleteAnnotationSet(sampleIdStr, annotationSetName, sessionId);
    }

    public QueryResult<AnnotationSet> deleteSampleAnnotations(String sampleIdStr, String annotationSetName, String annotations,
                                                              String sessionId) throws CatalogException {
        return sampleManager.deleteAnnotations(sampleIdStr, annotationSetName, annotations, sessionId);
    }

    public QueryResult<AnnotationSet> searchSampleAnnotationSets(String sampleIdStr, long variableSetId,
                                                                 @Nullable String annotation, String sessionId) throws CatalogException {
        return sampleManager.searchAnnotationSet(sampleIdStr, variableSetId, annotation, sessionId);
    }

    public QueryResult sampleGroupBy(Query query, QueryOptions qOptions, String fields, String sessionId) throws CatalogException {
        return sampleManager.groupBy(query, Arrays.asList(fields.split(",")), qOptions, sessionId);
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

    public QueryResult<AnnotationSet> deleteSampleAnnotation(long sampleId, String annotationId, String sessionId)
            throws CatalogException {
        return sampleManager.deleteAnnotation(sampleId, annotationId, sessionId);
    }

    public QueryResult<AnnotationSet> deleteIndividualAnnotation(long individualId, String annotationId, String sessionId)
            throws CatalogException {
        return individualManager.deleteAnnotation(individualId, annotationId, sessionId);
    }

    @Deprecated
    public QueryResult<Sample> deleteSample(long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        return null;
//        return sampleManager.delete(sampleId, options, sessionId);
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

    public List<QueryResult<CohortAclEntry>> getAllCohortAcls(String cohortIdsStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] cohortNameSplit = cohortIdsStr.split(",");
        List<Long> cohortIds = cohortManager.getIds(userId, cohortIdsStr);
        List<QueryResult<CohortAclEntry>> aclList = new ArrayList<>(cohortIds.size());
        for (int i = 0; i < cohortIds.size(); i++) {
            Long cohortId = cohortIds.get(i);
            QueryResult<CohortAclEntry> allCohortAcls = authorizationManager.getAllCohortAcls(userId, cohortId);
            allCohortAcls.setId(cohortNameSplit[i]);
            aclList.add(allCohortAcls);
        }
        return aclList;
    }

    public List<QueryResult<CohortAclEntry>> createCohortAcls(String cohortIdsStr, String members, String permissions,
                                                              String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] cohortNameSplit = cohortIdsStr.split(",");
        List<Long> cohortIds = cohortManager.getIds(userId, cohortIdsStr);
        List<QueryResult<CohortAclEntry>> cohortAclList = new ArrayList<>(cohortIds.size());
        for (int i = 0; i < cohortIds.size(); i++) {
            Long cohortId = cohortIds.get(i);
            QueryResult<CohortAclEntry> cohortAcls = authorizationManager.createCohortAcls(userId, cohortId, members,
                    permissions);
            cohortAcls.setId(cohortNameSplit[i]);
            cohortAclList.add(cohortAcls);
        }
        return cohortAclList;
    }

    public List<QueryResult<CohortAclEntry>> removeCohortAcl(String cohortIdsStr, String member, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] cohortNameStr = cohortIdsStr.split(",");
        List<Long> cohortIds = cohortManager.getIds(userId, cohortIdsStr);
        List<QueryResult<CohortAclEntry>> cohortAclList = new ArrayList<>(cohortIds.size());
        for (int i = 0; i < cohortIds.size(); i++) {
            Long cohortId = cohortIds.get(i);
            QueryResult<CohortAclEntry> cohortAcls = authorizationManager.removeCohortAcl(userId, cohortId, member);
            cohortAcls.setId(cohortNameStr[i]);
            cohortAclList.add(cohortAcls);
        }
        return cohortAclList;
    }

    public QueryResult<CohortAclEntry> getCohortAcl(String cohortIdStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long cohortId = cohortManager.getId(userId, cohortIdStr);
        return authorizationManager.getCohortAcl(userId, cohortId, member);
    }

    public QueryResult<CohortAclEntry> updateCohortAcl(String cohortIdStr, String member, @Nullable String addPermissions,
                                                       @Nullable String removePermissions, @Nullable String setPermissions,
                                                       String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long cohortId = cohortManager.getId(userId, cohortIdStr);
        return authorizationManager.updateCohortAcl(userId, cohortId, member, addPermissions, removePermissions, setPermissions);
    }

    public long getCohortId(String cohortStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return cohortManager.getId(userId, cohortStr);
    }

    public List<Long> getCohortIds(String cohortStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        return cohortManager.getIds(userId, cohortStr);
    }

    public QueryResult cohortGroupBy(Query query, QueryOptions qOptions, String fields, String sessionId) throws CatalogException {
        return cohortManager.groupBy(query, Arrays.asList(fields.split(",")), qOptions, sessionId);
    }

    public QueryResult<AnnotationSet> createCohortAnnotationSet(String cohortIdStr, long variableSetId, String annotationSetName,
                                                                Map<String, Object> annotations, Map<String, Object> attributes,
                                                                String sessionId) throws CatalogException {
        return cohortManager.createAnnotationSet(cohortIdStr, variableSetId, annotationSetName, annotations, attributes, sessionId);
    }

    public QueryResult<AnnotationSet> getAllCohortAnnotationSets(String cohortIdStr, String sessionId) throws CatalogException {
        return cohortManager.getAllAnnotationSets(cohortIdStr, sessionId);
    }

    public QueryResult<AnnotationSet> getCohortAnnotationSet(String cohortIdStr, String annotationSetName, String sessionId)
            throws CatalogException {
        return cohortManager.getAnnotationSet(cohortIdStr, annotationSetName, sessionId);
    }

    public QueryResult<AnnotationSet> updateCohortAnnotationSet(String cohortIdStr, String annotationSetName,
                                                                Map<String, Object> newAnnotations, String sessionId)
            throws CatalogException {
        return cohortManager.updateAnnotationSet(cohortIdStr, annotationSetName, newAnnotations, sessionId);
    }

    public QueryResult<AnnotationSet> deleteCohortAnnotationSet(String cohortIdStr, String annotationSetName, String sessionId)
            throws CatalogException {
        return cohortManager.deleteAnnotationSet(cohortIdStr, annotationSetName, sessionId);
    }

    public QueryResult<AnnotationSet> deleteCohortAnnotations(String cohortIdStr, String annotationSetName, String annotations,
                                                              String sessionId) throws CatalogException {
        return cohortManager.deleteAnnotations(cohortIdStr, annotationSetName, annotations, sessionId);
    }

    public QueryResult<AnnotationSet> searchCohortAnnotationSets(String cohortIdStr, long variableSetId,
                                                                 @Nullable String annotation, String sessionId) throws CatalogException {
        return cohortManager.searchAnnotationSet(cohortIdStr, variableSetId, annotation, sessionId);
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
        return jobManager.getTool(id, sessionId);
    }

    public QueryResult<Tool> getAllTools(Query query, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return jobManager.getTools(query, queryOptions, sessionId);
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

    public List<QueryResult<DiseasePanelAclEntry>> getAllPanelAcls(String panelIdsStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] panelNameSplit = panelIdsStr.split(",");
        List<Long> panelIds = studyManager.getDiseasePanelIds(userId, panelIdsStr);
        List<QueryResult<DiseasePanelAclEntry>> aclList = new ArrayList<>(panelIds.size());
        for (int i = 0; i < panelIds.size(); i++) {
            Long panelId = panelIds.get(i);
            QueryResult<DiseasePanelAclEntry> allPanelAcls = authorizationManager.getAllPanelAcls(userId, panelId);
            allPanelAcls.setId(panelNameSplit[i]);
            aclList.add(allPanelAcls);
        }
        return aclList;
    }

    public List<QueryResult<DiseasePanelAclEntry>> createPanelAcls(String panelIdsStr, String members, String permissions,
                                                                   String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] panelNameSplit = panelIdsStr.split(",");
        List<Long> panelIds = studyManager.getDiseasePanelIds(userId, panelIdsStr);
        List<QueryResult<DiseasePanelAclEntry>> panelAclList = new ArrayList<>(panelIds.size());
        for (int i = 0; i < panelIds.size(); i++) {
            Long panelId = panelIds.get(i);
            QueryResult<DiseasePanelAclEntry> panelAcls = authorizationManager.createPanelAcls(userId, panelId, members,
                    permissions);
            panelAcls.setId(panelNameSplit[i]);
            panelAclList.add(panelAcls);
        }
        return panelAclList;
    }

    public List<QueryResult<DiseasePanelAclEntry>> removePanelAcl(String panelIdsStr, String member, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] panelNameStr = panelIdsStr.split(",");
        List<Long> panelIds = studyManager.getDiseasePanelIds(userId, panelIdsStr);
        List<QueryResult<DiseasePanelAclEntry>> panelAclList = new ArrayList<>(panelIds.size());
        for (int i = 0; i < panelIds.size(); i++) {
            Long panelId = panelIds.get(i);
            QueryResult<DiseasePanelAclEntry> panelAcls = authorizationManager.removePanelAcl(userId, panelId, member);
            panelAcls.setId(panelNameStr[i]);
            panelAclList.add(panelAcls);
        }
        return panelAclList;
    }

    public QueryResult<DiseasePanelAclEntry> getPanelAcl(String panelIdStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long panelId = studyManager.getDiseasePanelId(userId, panelIdStr);
        return authorizationManager.getPanelAcl(userId, panelId, member);
    }

    public QueryResult<DiseasePanelAclEntry> updatePanelAcl(String panelIdStr, String member, @Nullable String addPermissions,
                                                            @Nullable String removePermissions, @Nullable String setPermissions,
                                                            String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long panelId = studyManager.getDiseasePanelId(userId, panelIdStr);
        return authorizationManager.updatePanelAcl(userId, panelId, member, addPermissions, removePermissions, setPermissions);
    }

    /*
    Dataset methods
     */

    public List<QueryResult<DatasetAclEntry>> getAllDatasetAcls(String datasetIdsStr, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] datasetNameSplit = datasetIdsStr.split(",");
        List<Long> datasetIds = fileManager.getDatasetIds(userId, datasetIdsStr);
        List<QueryResult<DatasetAclEntry>> aclList = new ArrayList<>(datasetIds.size());
        for (int i = 0; i < datasetIds.size(); i++) {
            Long datasetId = datasetIds.get(i);
            QueryResult<DatasetAclEntry> allDatasetAcls = authorizationManager.getAllDatasetAcls(userId, datasetId);
            allDatasetAcls.setId(datasetNameSplit[i]);
            aclList.add(allDatasetAcls);
        }
        return aclList;
    }

    public List<QueryResult<DatasetAclEntry>> createDatasetAcls(String datasetIdsStr, String members, String permissions,
                                                                String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] datasetNameSplit = datasetIdsStr.split(",");
        List<Long> datasetIds = fileManager.getDatasetIds(userId, datasetIdsStr);
        List<QueryResult<DatasetAclEntry>> datasetAclList = new ArrayList<>(datasetIds.size());
        for (int i = 0; i < datasetIds.size(); i++) {
            Long datasetId = datasetIds.get(i);
            QueryResult<DatasetAclEntry> datasetAcls = authorizationManager.createDatasetAcls(userId, datasetId, members,
                    permissions);
            datasetAcls.setId(datasetNameSplit[i]);
            datasetAclList.add(datasetAcls);
        }
        return datasetAclList;
    }

    public List<QueryResult<DatasetAclEntry>> removeDatasetAcl(String datasetIdsStr, String member, String sessionId)
            throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        String[] datasetNameStr = datasetIdsStr.split(",");
        List<Long> datasetIds = fileManager.getDatasetIds(userId, datasetIdsStr);
        List<QueryResult<DatasetAclEntry>> datasetAclList = new ArrayList<>(datasetIds.size());
        for (int i = 0; i < datasetIds.size(); i++) {
            Long datasetId = datasetIds.get(i);
            QueryResult<DatasetAclEntry> datasetAcls = authorizationManager.removeDatasetAcl(userId, datasetId, member);
            datasetAcls.setId(datasetNameStr[i]);
            datasetAclList.add(datasetAcls);
        }
        return datasetAclList;
    }

    public QueryResult<DatasetAclEntry> getDatasetAcl(String datasetIdStr, String member, String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long datasetId = fileManager.getDatasetId(userId, datasetIdStr);
        return authorizationManager.getDatasetAcl(userId, datasetId, member);
    }

    public QueryResult<DatasetAclEntry> updateDatasetAcl(String datasetIdStr, String member, @Nullable String addPermissions,
                                                         @Nullable String removePermissions, @Nullable String setPermissions,
                                                         String sessionId) throws CatalogException {
        String userId = getUserIdBySessionId(sessionId);
        long datasetId = fileManager.getDatasetId(userId, datasetIdStr);
        return authorizationManager.updateDatasetAcl(userId, datasetId, member, addPermissions, removePermissions, setPermissions);
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

    public CatalogConfiguration getCatalogConfiguration() {
        return catalogConfiguration;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }
}
