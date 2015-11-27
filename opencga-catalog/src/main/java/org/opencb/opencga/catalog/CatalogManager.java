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

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.opencga.catalog.audit.CatalogAuditManager;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogStudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.*;
import org.opencb.opencga.catalog.managers.api.*;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authentication.CatalogAuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.client.CatalogClient;
import org.opencb.opencga.catalog.client.CatalogDBClient;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;

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

    private CatalogDBAdaptorFactory catalogDBAdaptorFactory;
    private CatalogIOManagerFactory catalogIOManagerFactory;
    private CatalogClient catalogClient;

    private IUserManager userManager;
    private IProjectManager projectManager;
    private IStudyManager studyManager;
    private IFileManager fileManager;
    private IJobManager jobManager;
    private IIndividualManager individualManager;
    private ISampleManager sampleManager;

    private Properties properties;

    protected static Logger logger = LoggerFactory.getLogger(CatalogManager.class);
    private AuthenticationManager authenticationManager;
    private AuthorizationManager authorizationManager;
    private CatalogAuditManager auditManager;

    public CatalogManager(CatalogDBAdaptorFactory catalogDBAdaptorFactory, Properties catalogProperties)
            throws IOException, CatalogIOException {
        this.catalogDBAdaptorFactory = catalogDBAdaptorFactory;
        this.properties = catalogProperties;

        configureIOManager(properties);
        configureManagers(properties);
    }

    public CatalogManager(Properties catalogProperties)
            throws CatalogException {
        this.properties = catalogProperties;
        logger.debug("CatalogManager configureDBAdaptor");
        configureDBAdaptor(properties);
        logger.debug("CatalogManager configureIOManager");
        configureIOManager(properties);
        logger.debug("CatalogManager configureManager");
        configureManagers(properties);

        if (!catalogDBAdaptorFactory.isCatalogDBReady()) {
            catalogDBAdaptorFactory.initializeCatalogDB();
            User admin = new User("admin", "admin", "admin@email.com", "", "openCB", User.Role.ADMIN, "active");
            catalogDBAdaptorFactory.getCatalogUserDBAdaptor().insertUser(admin, null);
            authenticationManager.newPassword("admin", "admin");
        }
    }

    private void configureManagers(Properties properties) {
        catalogClient = new CatalogDBClient(this);
        //TODO: Check if catalog is empty
        //TODO: Setup catalog if it's empty.

        auditManager = new CatalogAuditManager(catalogDBAdaptorFactory.getCatalogAuditDbAdaptor(), catalogDBAdaptorFactory.getCatalogUserDBAdaptor(), authorizationManager, properties);
        authenticationManager = new CatalogAuthenticationManager(catalogDBAdaptorFactory.getCatalogUserDBAdaptor(), properties);
        authorizationManager = new CatalogAuthorizationManager(catalogDBAdaptorFactory, auditManager);
        userManager = new UserManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, catalogIOManagerFactory, properties);
        fileManager = new FileManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, catalogIOManagerFactory, properties);
        studyManager = new StudyManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, catalogIOManagerFactory, properties);
        projectManager = new ProjectManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, catalogIOManagerFactory, properties);
        jobManager = new JobManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, catalogIOManagerFactory, properties);
        sampleManager = new SampleManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, catalogIOManagerFactory, properties);
        individualManager = new IndividualManager(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, catalogIOManagerFactory, properties);
    }

    public CatalogClient client() {
        return client("");
    }

    public CatalogClient client(String sessionId) {
        catalogClient.setSessionId(sessionId);
        return catalogClient;
    }

    public CatalogIOManagerFactory getCatalogIOManagerFactory() {
        return catalogIOManagerFactory;
    }

    private void configureIOManager(Properties properties)
            throws CatalogIOException {
        catalogIOManagerFactory = new CatalogIOManagerFactory(properties);
    }

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
        catalogDBAdaptorFactory = new CatalogMongoDBAdaptor(dataStoreServerAddresses, mongoDBConfiguration, properties.getProperty(CATALOG_DB_DATABASE, ""));
    }

    @Override
    public void close() throws CatalogException {
        catalogDBAdaptorFactory.close();
    }

    /**
     * Getter path methods
     * ***************************
     */

    public URI getUserUri(String userId) throws CatalogIOException {
        return catalogIOManagerFactory.getDefault().getUserUri(userId);
    }

    public URI getProjectUri(String userId, String projectId) throws CatalogIOException {
        return catalogIOManagerFactory.getDefault().getProjectUri(userId, projectId);
    }

    public URI getStudyUri(int studyId)
            throws CatalogException {
        return fileManager.getStudyUri(studyId);
    }

    public URI getFileUri(File file) throws CatalogException {
        return fileManager.getFileUri(file);
    }

    public URI getFileUri(Study study, File file) throws CatalogException {
        return fileManager.getFileUri(study, file);
    }

    public URI getFileUri(int studyId, String relativeFilePath)
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

    public int getProjectIdByStudyId(int studyId) throws CatalogException {
        return studyManager.getProjectId(studyId);
    }

    /**
     * Id methods
     * <user>@project:study:directories:filePath
     * ***************************
     */

    public int getProjectId(String id) throws CatalogException {
        return projectManager.getProjectId(id);
    }

    public int getStudyId(String id) throws CatalogException {
        return studyManager.getStudyId(id);
    }

    public int getFileId(String id) throws CatalogException {
        return fileManager.getFileId(id);
    }

    public int getToolId(String id) throws CatalogException {
        return jobManager.getToolId(id);
    }

    /**
     * User methods
     * ***************************
     */

    public QueryResult<User> createUser(String id, String name, String email, String password, String organization, QueryOptions options)
            throws CatalogException {
        return createUser(id, name, email, password, organization, options, null);
    }

    public QueryResult<User> createUser(String id, String name, String email, String password, String organization, QueryOptions options, String sessionId)
            throws CatalogException {
        return userManager.create(id, name, email, password, organization, options, sessionId);
    }

    public QueryResult<ObjectMap> loginAsAnonymous(String sessionIp)
            throws CatalogException, IOException {
        return userManager.loginAsAnonymous(sessionIp);
    }

    public QueryResult<ObjectMap> login(String userId, String password, String sessionIp)
            throws CatalogException, IOException {
        return userManager.login(userId, password, sessionIp);
    }

    public QueryResult logout(String userId, String sessionId) throws CatalogException {
        return userManager.logout(userId, sessionId);
    }

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

    public String getUserIdByStudyId(int studyId) throws CatalogException {
        return studyManager.getUserId(studyId);
    }

    public String getUserIdByProjectId(int projectId) throws CatalogException {
        return projectManager.getUserId(projectId);
    }

    public QueryResult modifyUser(String userId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        return userManager.update(userId, parameters, null, sessionId);  //TODO: Add query options
    }

    public void deleteUser(String userId, String sessionId) throws CatalogException {
        userManager.delete(userId, null, sessionId);
    }

    /**
     * Project methods
     * ***************************
     */

    public QueryResult<Project> createProject(String ownerId, String name, String alias, String description,
                                              String organization, QueryOptions options, String sessionId)
            throws CatalogException {
        return projectManager.create(ownerId, name, alias, description, organization, options, sessionId);
    }

    public QueryResult<Project> getProject(int projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        return projectManager.read(projectId, options, sessionId);
    }

    public QueryResult<Project> getAllProjects(String ownerId, QueryOptions options, String sessionId)
            throws CatalogException {
        return projectManager.readAll(new QueryOptions("ownerId", ownerId), options, sessionId);
    }

    public QueryResult renameProject(int projectId, String newProjectAlias, String sessionId)
            throws CatalogException {
        return projectManager.update(projectId, new QueryOptions("alias", newProjectAlias), null, sessionId);//TODO: Add query options
    }

    /**
     * Modify some params from the specified project:
     * <p/>
     * name
     * description
     * organization
     * status
     * attributes
     *
     * @param projectId  Project identifier
     * @param parameters Parameters to change.
     * @param sessionId  sessionId to check permissions
     * @return
     * @throws org.opencb.opencga.catalog.exceptions.CatalogDBException
     */
    public QueryResult modifyProject(int projectId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        return projectManager.update(projectId, parameters, null, sessionId);//TODO: Add query options
    }

    /**
     * Study methods
     * ***************************
     */
    public QueryResult<Study> createStudy(int projectId, String name, String alias, Study.Type type, String description,
                                          String sessionId)
            throws CatalogException {
        return createStudy(projectId, name, alias, type, null, null, description, null, null, null, null, null, null, null, null, sessionId);
    }

    /**
     * Creates a new Study in catalog
     * @param projectId     Parent project id
     * @param name          Study Name
     * @param alias         Study Alias. Must be unique in the project's studies
     * @param type          Study type: CONTROL_CASE, CONTROL_SET, ... (see org.opencb.opencga.catalog.models.Study.Type)
     * @param creatorId     Creator user id. If null, user by sessionId
     * @param creationDate  Creation date. If null, now
     * @param description   Study description. If null, empty string
     * @param status        Unused
     * @param cipher        Unused
     * @param uriScheme     UriScheme to select the CatalogIOManager. Default: CatalogIOManagerFactory.DEFAULT_CATALOG_SCHEME
     * @param uri           URI for the folder where to place the study. Scheme must match with the uriScheme. Folder must exist.
     * @param datastores    DataStores information
     * @param stats         Optional stats
     * @param attributes    Optional attributes
     * @param options       QueryOptions
     * @param sessionId     User's sessionId
     * @return              Generated study
     * @throws CatalogException
     * @throws IOException
     */
    public QueryResult<Study> createStudy(int projectId, String name, String alias, Study.Type type,
                                          String creatorId, String creationDate, String description, String status,
                                          String cipher, String uriScheme, URI uri,
                                          Map<File.Bioformat, DataStore> datastores, Map<String, Object> stats,
                                          Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        QueryResult<Study> result = studyManager.create(projectId, name, alias, type, creatorId, creationDate, description, status, cipher, uriScheme,
                uri, datastores, stats, attributes, options, sessionId);
        createFolder(result.getResult().get(0).getId(), Paths.get("data"), true, null, sessionId);
        createFolder(result.getResult().get(0).getId(), Paths.get("analysis"), true, null, sessionId);
        return result;
    }

    public QueryResult<Study> getStudy(int studyId, String sessionId)
            throws CatalogException {
        return getStudy(studyId, sessionId, null);
    }

    public QueryResult<Study> getStudy(int studyId, String sessionId, QueryOptions options)
            throws CatalogException {
        return studyManager.read(studyId, options, sessionId);
    }

    public QueryResult<Study> getAllStudiesInProject(int projectId, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.readAll(new QueryOptions(CatalogStudyDBAdaptor.StudyFilterOptions.projectId.toString(), projectId), options, sessionId);
    }

    public QueryResult<Study> getAllStudies(QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.readAll(options, options, sessionId);
    }

    public QueryResult renameStudy(int studyId, String newStudyAlias, String sessionId)
            throws CatalogException {
        return studyManager.update(studyId, new ObjectMap("alias", newStudyAlias), null, sessionId);
    }

    /**
     * Modify some params from the specified study:
     * <p/>
     * name
     * description
     * organization
     * status
     * <p/>
     * attributes
     * stats
     *
     * @param studyId    Study identifier
     * @param parameters Parameters to change.
     * @param sessionId  sessionId to check permissions
     * @return
     * @throws org.opencb.opencga.catalog.exceptions.CatalogDBException
     */
    public QueryResult modifyStudy(int studyId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        return studyManager.update(studyId, parameters, null, sessionId);
    }

    public QueryResult addMemberToGroup(int studyId, String groupId, String userId, String sessionId) throws CatalogException {
        return authorizationManager.addMember(studyId, groupId, userId, sessionId);
    }

    public QueryResult removeMemberFromGroup(int studyId, String groupId, String userId, String sessionId) throws CatalogException {
        return authorizationManager.removeMember(studyId, groupId, userId, sessionId);
    }

    /**
     * File methods
     * ***************************
     */

    public String getFileOwner(int fileId) throws CatalogException {
        return fileManager.getUserId(fileId);
    }

    public int getStudyIdByFileId(int fileId) throws CatalogException {
        return fileManager.getStudyId(fileId);
    }

    //create file with byte[]
    public QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, byte[] bytes, String description,
                                        boolean parents, String sessionId)
            throws CatalogException, IOException {
        QueryResult<File> queryResult = fileManager.create(studyId, File.Type.FILE, format, bioformat, path, null, null,
                description, File.Status.STAGE, 0, -1, null, -1, null, null, parents, null, sessionId);
        new CatalogFileUtils(this).upload(new ByteArrayInputStream(bytes), queryResult.first(), sessionId, false, false, true);
        return getFile(queryResult.first().getId(), sessionId);
    }

    public QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, URI fileLocation, String description,
                                        boolean parents, String sessionId)
            throws CatalogException, IOException {
        QueryResult<File> queryResult = fileManager.create(studyId, File.Type.FILE, format, bioformat, path, null, null,
                description, File.Status.STAGE, 0, -1, null, -1, null, null, parents, null, sessionId);
        new CatalogFileUtils(this).upload(fileLocation, queryResult.first(), null, sessionId, false, false, true, true, Integer.MAX_VALUE);
        return getFile(queryResult.first().getId(), sessionId);
    }

    public QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                        boolean parents, int jobId, String sessionId)
            throws CatalogException {
        return fileManager.create(studyId, File.Type.FILE, format, bioformat, path, null, null, description, null, 0, -1, null,
                jobId, null, null, parents, null, sessionId);
    }


    public QueryResult<File> createFile(int studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                        String ownerId, String creationDate, String description, File.Status status,
                                        long diskUsage, int experimentId, List<Integer> sampleIds, int jobId,
                                        Map<String, Object> stats, Map<String, Object> attributes,
                                        boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.create(studyId, type, format, bioformat, path, ownerId, creationDate, description, status,
                diskUsage, experimentId, sampleIds, jobId, stats, attributes, parents, options, sessionId);
    }

    public QueryResult<File> createFolder(int studyId, Path folderPath, boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkPath(folderPath, "folderPath");
        return fileManager.createFolder(studyId, folderPath.toString() + "/", null, parents, null, options, sessionId);
    }

    public QueryResult<File> createFolder(int studyId, Path folderPath, File.Status status, boolean parents, String description, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkPath(folderPath, "folderPath");
        return fileManager.createFolder(studyId, folderPath.toString() + "/", status, parents, description, options, sessionId);
    }

    public QueryResult deleteFolder(int folderId, String sessionId)
            throws CatalogException, IOException {
        return deleteFile(folderId, sessionId);
    }

    public QueryResult deleteFile(int fileId, String sessionId)
            throws CatalogException, IOException {
        return fileManager.delete(fileId, null, sessionId);
    }

    public QueryResult moveFile(int fileId, String newPath, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.move(fileId, newPath, options, sessionId);
    }

    public QueryResult renameFile(int fileId, String newName, String sessionId)
            throws CatalogException {
        return fileManager.rename(fileId, newName, sessionId);
    }

    /**
     * Modify some params from the specified file:
     * <p/>
     * name
     * type
     * format
     * bioformat
     * description
     * status
     * <p/>
     * attributes
     * stats
     *
     * @param fileId     File identifier
     * @param parameters Parameters to change.
     * @param sessionId  sessionId to check permissions
     * @return
     * @throws org.opencb.opencga.catalog.exceptions.CatalogDBException
     */
    public QueryResult modifyFile(int fileId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        return fileManager.update(fileId, parameters, null, sessionId); //TODO: Add query options
    }

    public QueryResult<File> getFileParent(int fileId, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.getParent(fileId, options, sessionId);
    }

    public QueryResult<File> getFileParents(int fileId, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.getParents(fileId, options, sessionId);
    }

    public QueryResult<File> getFile(int fileId, String sessionId)
            throws CatalogException {
        return getFile(fileId, null, sessionId);
    }

    public QueryResult<File> getFile(int fileId, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.read(fileId, options, sessionId);
    }

    public QueryResult<File> getAllFiles(int studyId, QueryOptions options, String sessionId) throws CatalogException {
        return fileManager.readAll(studyId, options, options, sessionId);
    }

    public QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkId(folderId, "folderId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        int studyId = getStudyIdByFileId(folderId);
        File folder = getFile(folderId, sessionId).first();
        if (!folder.getType().equals(File.Type.FOLDER)) {
            throw new CatalogDBException("File {id:" + folderId + ", path:'" + folder.getPath() + "'} is not a folder.");
        }
        options.put("directory", folder.getPath());
        return fileManager.readAll(studyId, options, options, sessionId);
    }

    public DataInputStream downloadFile(int fileId, String sessionId)
            throws IOException, CatalogException {
        return downloadFile(fileId, -1, -1, sessionId);
    }

    public DataInputStream downloadFile(int fileId, int start, int limit, String sessionId)
            throws IOException, CatalogException {
        return fileManager.download(fileId, start, limit, null, sessionId);
    }

    public DataInputStream grepFile(int fileId, String pattern, boolean ignoreCase, boolean multi, String sessionId)
            throws CatalogException {
        QueryOptions options = new QueryOptions("ignoreCase", ignoreCase);
        options.put("multi", multi);
        return fileManager.grep(fileId, pattern, options, sessionId);
    }


    public QueryResult shareFile(int fileId, AclEntry acl, String sessionId)
            throws CatalogException {
        return authorizationManager.setFileACL(fileId, acl, sessionId);
    }

    public QueryResult unshareFile(int fileId, String userId, String sessionId)
            throws CatalogException {
        return authorizationManager.unsetFileACL(fileId, userId, sessionId);
    }

    /*Require role admin*/
    public QueryResult<File> searchFile(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        return searchFile(-1, query, options, sessionId);
    }

    public QueryResult<File> searchFile(int studyId, QueryOptions query, String sessionId)
            throws CatalogException {
        return searchFile(studyId, query, null, sessionId);
    }

    public QueryResult<File> searchFile(int studyId, QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.readAll(studyId, query, options, sessionId);
    }

    public QueryResult<Dataset> createDataset(int studyId, String name, String description, List<Integer> files,
                                              Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.createDataset(studyId, name, description, files, attributes, options, sessionId);
    }

    public QueryResult<Dataset> getDataset(int dataSetId, QueryOptions options, String sessionId)
            throws CatalogException {
        return fileManager.readDataset(dataSetId, options, sessionId);
    }


    public QueryResult refreshFolder(final int folderId, final String sessionId)
            throws CatalogDBException, IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * **************************
     * Job methods
     * ***************************
     */

    public int getStudyIdByJobId(int jobId) throws CatalogException {
        return jobManager.getStudyId(jobId);
    }

    public QueryResult<Job> createJob(int studyId, String name, String toolName, String description, String executor, Map<String, String> params, String commandLine,
                                      URI tmpOutDirUri, int outDirId, List<Integer> inputFiles, List<Integer> outputFiles, Map<String, Object> attributes,
                                      Map<String, Object> resourceManagerAttributes, Job.Status status,
                                      long startTime, long endTime, QueryOptions options, String sessionId)
            throws CatalogException {
        return jobManager.create(studyId, name, toolName, description, executor, params, commandLine, tmpOutDirUri, outDirId, inputFiles,
                outputFiles, attributes, resourceManagerAttributes, status, startTime, endTime, options, sessionId);
    }

    public URI createJobOutDir(int studyId, String dirName, String sessionId)
            throws CatalogException {
        return jobManager.createJobOutDir(studyId, dirName, sessionId);
    }

    public QueryResult<ObjectMap> incJobVisites(int jobId, String sessionId) throws CatalogException {
        return jobManager.visit(jobId, sessionId);
    }

    public QueryResult deleteJob(int jobId, String sessionId) throws CatalogException {
        return jobManager.delete(jobId, null, sessionId);
    }


    public QueryResult<Job> getJob(int jobId, QueryOptions options, String sessionId) throws CatalogException {
        return jobManager.read(jobId, options, sessionId);
    }

    public QueryResult<Job> getUnfinishedJobs(String sessionId) throws CatalogException {
        return jobManager.readAll(new QueryOptions("status",
                Arrays.asList(
                        Job.Status.PREPARED.toString(),
                        Job.Status.QUEUED.toString(),
                        Job.Status.RUNNING.toString(),
                        Job.Status.DONE.toString()
                )
        ), null, sessionId);
    }


    public QueryResult<Job> getAllJobs(int studyId, String sessionId) throws CatalogException {
        return jobManager.readAll(studyId, null, null, sessionId);
    }

    public QueryResult<Job> getAllJobs(int studyId, QueryOptions query, String sessionId) throws CatalogException {
        return jobManager.readAll(studyId, query, query, sessionId);
    }


    public QueryResult<Job> modifyJob(int jobId, ObjectMap parameters, String sessionId) throws CatalogException {
        return jobManager.update(jobId, parameters, null, sessionId); //TODO: Add query options
    }

    /**
     * Project methods
     * ***************************
     */

    public QueryResult<Individual> createIndividual(int studyId, String name, String family, int fatherId, int motherId,
                                                    Individual.Gender gender, QueryOptions options, String sessionId)
            throws CatalogException {
        return individualManager.create(studyId, name, family, fatherId, motherId, gender, options, sessionId);
    }

    public QueryResult<Individual> getIndividual(int individualId, QueryOptions options, String sessionId)
            throws CatalogException {
        return individualManager.read(individualId, options, sessionId);
    }

    public QueryResult<Individual> getAllIndividuals(int studyId, QueryOptions options, String sessionId) throws CatalogException {
        return individualManager.readAll(studyId, options, sessionId);
    }

    public QueryResult<Individual> modifyIndividual(int individualId, QueryOptions options, String sessionId) throws CatalogException {
        return individualManager.update(individualId, options, options, sessionId);
    }

    public QueryResult<Individual> deleteIndividual(int individualId, QueryOptions options, String sessionId) throws CatalogException {
        return individualManager.delete(individualId, options, sessionId);
    }

    /**
     * Samples methods
     * ***************************
     */

    public QueryResult<Sample> createSample(int studyId, String name, String source, String description,
                                            Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        return sampleManager.create(studyId, name, source, description, attributes, options, sessionId);
    }

    public QueryResult<Sample> getSample(int sampleId, QueryOptions options, String sessionId)
            throws CatalogException {
        return sampleManager.read(sampleId, options, sessionId);
    }

    public QueryResult<Sample> getAllSamples(int studyId, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.readAll(studyId, options, options, sessionId);
    }

    public QueryResult<Sample> modifySample(int sampleId, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return sampleManager.update(sampleId, queryOptions, queryOptions, sessionId);
    }

    public QueryResult shareSample(int sampleId, AclEntry acl, String sessionId)
            throws CatalogException {
        return authorizationManager.setSampleACL(sampleId, acl, sessionId);
    }

    public QueryResult unshareSample(int sampleId, String userId, String sessionId)
            throws CatalogException {
        return authorizationManager.unsetSampleACL(sampleId, userId, sessionId);
    }

    public QueryResult<AnnotationSet> annotateSample(int sampleId, String id, int variableSetId,
                                                     Map<String, Object> annotations,
                                                     Map<String, Object> attributes,
                                                     String sessionId) throws CatalogException {
        return annotateSample(sampleId, id, variableSetId, annotations, attributes, true, sessionId);
    }

    public QueryResult<AnnotationSet> annotateSample(int sampleId, String annotationSetId, int variableSetId,
                                                            Map<String, Object> annotations,
                                                            Map<String, Object> attributes,
                                                            boolean checkAnnotationSet,
                                                            String sessionId)
            throws CatalogException {
        return sampleManager.annotate(sampleId, annotationSetId, variableSetId, annotations, attributes, checkAnnotationSet, sessionId);
    }

    public QueryResult<AnnotationSet> updateSampleAnnotation(int sampleId, String annotationSetId,
                                                                 Map<String, Object> annotations,
                                                                 String sessionId)
            throws CatalogException {
        return sampleManager.updateAnnotation(sampleId, annotationSetId, annotations, sessionId);
    }

    public QueryResult<AnnotationSet> annotateIndividual(int individualId, String annotationSetId, int variableSetId,
                                                            Map<String, Object> annotations,
                                                            Map<String, Object> attributes,
                                                            String sessionId)
            throws CatalogException {
        return individualManager.annotate(individualId, annotationSetId, variableSetId, annotations, attributes, sessionId);
    }

    public QueryResult<AnnotationSet> updateIndividualAnnotation(int individualId, String annotationSetId,
                                                            Map<String, Object> annotations,
                                                            String sessionId)
            throws CatalogException {
        return individualManager.updateAnnotation(individualId, annotationSetId, annotations, sessionId);
    }

    public QueryResult<AnnotationSet> deleteSampleAnnotation(int sampleId, String annotationId, String sessionId)
            throws CatalogException {
        return sampleManager.deleteAnnotation(sampleId, annotationId, sessionId);
    }

    public QueryResult<AnnotationSet> deleteIndividualAnnotation(int individualId, String annotationId, String sessionId)
            throws CatalogException {
        return individualManager.deleteAnnotation(individualId, annotationId, sessionId);
    }

    public QueryResult<Sample> deleteSample(int sampleId, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.delete(sampleId, options, sessionId);
    }

    /**
     * VariableSet methods
     * ***************************
     */

    public QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                                      String description, Map<String, Object> attributes,
                                                      List<Variable> variables, String sessionId)
            throws CatalogException {
        return studyManager.createVariableSet(studyId, name, unique, description, attributes, variables, sessionId);
    }

    public QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                                      String description, Map<String, Object> attributes,
                                                      Set<Variable> variables, String sessionId)
            throws CatalogException {
        return studyManager.createVariableSet(studyId, name, unique, description, attributes, variables, sessionId);
    }

    public QueryResult<VariableSet> getVariableSet(int variableSet, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.readVariableSet(variableSet, options, sessionId);
    }

    public QueryResult<VariableSet> getAllVariableSet(int studyId, QueryOptions options, String sessionId)
            throws CatalogException {
        return studyManager.readAllVariableSets(studyId, options, sessionId);
    }

    public QueryResult<VariableSet> deleteVariableSet(int variableSetId, QueryOptions queryOptions, String sessionId)
            throws CatalogException {
        return studyManager.deleteVariableSet(variableSetId, queryOptions, sessionId);
    }

    /**
     * Cohort methods
     * ***************************
     */

    public int getStudyIdByCohortId(int cohortId) throws CatalogException {
        return sampleManager.getStudyIdByCohortId(cohortId);
    }

    public QueryResult<Cohort> getCohort(int cohortId, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.readCohort(cohortId, options, sessionId);
    }

    public QueryResult<Cohort> getAllCohorts(int studyId, QueryOptions options, String sessionId) throws CatalogException {
        return sampleManager.readAllCohort(studyId, options, sessionId);
    }

    public QueryResult<Cohort> createCohort(int studyId, String name, Cohort.Type type, String description, List<Integer> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException {
        return sampleManager.createCohort(studyId, name, type, description, sampleIds, attributes, sessionId);
    }

    public QueryResult<Cohort> modifyCohort(int cohortId, ObjectMap updateParams, String sessionId) throws CatalogException {
        return sampleManager.updateCohort(cohortId, updateParams, sessionId);
    }

    public QueryResult<Cohort> deleteCohort(int cohortId, ObjectMap options, String sessionId) throws CatalogException {
        return sampleManager.deleteCohort(cohortId, options, sessionId);
    }

    /**
     * Tools methods
     * ***************************
     */

    public QueryResult<Tool> createTool(String alias, String description, Object manifest, Object result,
                                        String path, boolean openTool, String sessionId) throws CatalogException {
        return jobManager.createTool(alias, description, manifest, result, path, openTool, sessionId);
    }

    public QueryResult<Tool> getTool(int id, String sessionId) throws CatalogException {
        return jobManager.readTool(id, sessionId);
    }

    public QueryResult<Tool> getAllTools(QueryOptions queryOptions, String sessionId) throws CatalogException {
        return jobManager.readAllTools(queryOptions, sessionId);
    }
}
