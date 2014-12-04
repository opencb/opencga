package org.opencb.opencga.catalog.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.WriteResult;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.*;

import java.io.IOException;
import java.net.URI;

public interface CatalogDBAdaptor {


    void disconnect();
    /**
     * User methods
     * ***************************
     */
    boolean checkUserCredentials(String userId, String sessionId);

    boolean userExists(String userId);

    QueryResult<User> createUser(User user) throws CatalogManagerException;

    QueryResult<Integer> deleteUser(String userId) throws CatalogManagerException;

    QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogManagerException, IOException;

    QueryResult logout(String userId, String sessionId) throws CatalogManagerException;

    QueryResult<ObjectMap> loginAsAnonymous(Session session) throws CatalogManagerException;

    QueryResult logoutAnonymous(String sessionId) throws CatalogManagerException;

    QueryResult<User> getUser(String userId, QueryOptions options, String lastActivity) throws CatalogManagerException;

    QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogManagerException;

    QueryResult changeEmail(String userId, String newEmail) throws CatalogManagerException;

    void updateUserLastActivity(String userId) throws CatalogManagerException;

    QueryResult modifyUser(String userId, ObjectMap parameters) throws CatalogManagerException;

    QueryResult resetPassword(String userId, String email, String newCryptPass) throws CatalogManagerException;

    QueryResult getSession(String userId, String sessionId) throws CatalogManagerException;

    String getUserIdBySessionId(String sessionId);

    /**
     * Project methods
     * ***************************
     */
    QueryResult<Project> createProject(String userId, Project project) throws CatalogManagerException, JsonProcessingException;

    QueryResult<Project> getAllProjects(String userId) throws CatalogManagerException;

    QueryResult<Project> getProject(String userId, String projectAlias) throws CatalogManagerException;
    QueryResult<Project> getProject(int project, QueryOptions options) throws CatalogManagerException;

    QueryResult<Integer> deleteProject(int projecetId) throws CatalogManagerException;

    QueryResult renameProjectAlias(int projectId, String newprojectName) throws CatalogManagerException;

    QueryResult modifyProject(int projectId, ObjectMap parameters) throws CatalogManagerException;

    int getProjectId(String userId, String projectAlias) throws CatalogManagerException;

    String getProjectOwnerId(int projectId) throws CatalogManagerException;

    QueryResult<Acl> getProjectAcl(int projectId, String userId) throws CatalogManagerException;

    QueryResult setProjectAcl(int projectId, Acl newAcl) throws CatalogManagerException;

    /**
     * Study methods
     * ***************************
     */
    QueryResult<Study> createStudy(int projectId, Study study) throws CatalogManagerException;

    QueryResult<Study> getAllStudies(int projectId) throws CatalogManagerException;
//    QueryResult<Study> getAllStudies(String userId, String projectAlias) throws CatalogManagerException;

    QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogManagerException;

    QueryResult renameStudy(String userId, String projectAlias, String studyAlias, String newStudyName) throws CatalogManagerException;

    QueryResult renameStudy(int studyId, String newStudyName) throws CatalogManagerException;

//    QueryResult modifyStudy(int studyId, Map<String, String> parameters, Map<String, Object> attributes, Map<String, Object> stats) throws CatalogManagerException;

    void updateStudyLastActivity(int studyId) throws CatalogManagerException;

    QueryResult modifyStudy(int studyId, ObjectMap params) throws CatalogManagerException;

    QueryResult<Integer> deleteStudy(String userId, String projectAlias, String studyAlias) throws CatalogManagerException;

    QueryResult<Integer> deleteStudy(int studyId) throws CatalogManagerException;

    int getStudyId(int projectId, String studyAlias) throws CatalogManagerException;

    int getStudyId(String userId, String projectAlias, String studyAlias) throws CatalogManagerException;

    int getProjectIdByStudyId(int studyId) throws CatalogManagerException;

    String getStudyOwnerId(int studyId) throws CatalogManagerException;

    QueryResult<Acl> getStudyAcl(int projectId, String userId) throws CatalogManagerException;
    QueryResult setStudyAcl(int projectId, Acl newAcl) throws CatalogManagerException;
    /**
     * File methods
     * ***************************
     */

    // add file to study
    QueryResult<File> createFileToStudy(String userId, String projectAlias, String studyAlias, File file) throws CatalogManagerException;
    QueryResult<File> createFileToStudy(int studyId, File file) throws CatalogManagerException;

    QueryResult<Integer> deleteFile(String userId, String projectAlias, String studyAlias, String path) throws CatalogManagerException, IOException;
    QueryResult<Integer> deleteFile(int studyId, String path) throws CatalogManagerException;
    QueryResult<Integer> deleteFile(int fileId) throws CatalogManagerException;

    QueryResult deleteFilesFromStudy(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException;
    QueryResult deleteFilesFromStudy(int studyId, String studyAlias, String sessionId) throws CatalogManagerException;

    int getFileId(String userId, String projectAlias, String studyAlias, String path) throws CatalogManagerException;
    int getFileId(int studyId, String path) throws CatalogManagerException;

    QueryResult<File> getAllFiles(int studyId) throws CatalogManagerException;

    QueryResult<File> getAllFilesInFolder(int folderId) throws CatalogManagerException;

    QueryResult<File> getFile(int fileId) throws CatalogManagerException;
    QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogManagerException;

    QueryResult setFileStatus(String userId, String projectAlias, String studyAlias, String path, String status) throws CatalogManagerException, IOException;
    QueryResult setFileStatus(int studyId, String path, String status) throws CatalogManagerException, IOException;
    QueryResult setFileStatus(int fileId, String status) throws CatalogManagerException, IOException;

    QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogManagerException;

//    QueryResult setIndexFile(int fileId, String backend, Index index) throws CatalogManagerException;

    QueryResult<WriteResult> renameFile(int fileId, String name) throws CatalogManagerException;

    int getStudyIdByFileId(int fileId) throws CatalogManagerException;
    String getFileOwnerId(int fileId) throws CatalogManagerException;

    QueryResult<Acl> getFileAcl(int fileId, String userId) throws CatalogManagerException;
    QueryResult setFileAcl(int fileId, Acl newAcl) throws CatalogManagerException;

    QueryResult<File> searchFile(QueryOptions query, QueryOptions options) throws CatalogManagerException;

    /**
     * Analysis methods
     * ***************************
     */

//    QueryResult<Analysis> getAllAnalysis(String userId, String projectAlias, String studyAlias) throws CatalogManagerException;
//    QueryResult<Analysis> getAllAnalysis(int studyId) throws CatalogManagerException;
//    int getAnalysisId(int studyId, String analysisAlias) throws CatalogManagerException;
//    QueryResult<Analysis> getAnalysis(int analysisId) throws CatalogManagerException;
//
//    QueryResult<Analysis> createAnalysis(String userId, String projectAlias, String studyAlias, Analysis analysis) throws CatalogManagerException;
//    QueryResult<Analysis> createAnalysis(int studyId, Analysis analysis) throws CatalogManagerException;
//
//    QueryResult modifyAnalysis(int analysisId, ObjectMap parameters) throws CatalogManagerException;
//
//    int getStudyIdByAnalysisId(int analysisId) throws CatalogManagerException;
//    String getAnalysisOwner(int analysisId) throws CatalogManagerException;

    /**
     * Job methods
     * ***************************
     */

    QueryResult<Job> createJob(int studyId, Job job) throws CatalogManagerException;

    QueryResult<Integer> deleteJob(int jobId) throws CatalogManagerException;

    QueryResult<Job> getJob(int jobId) throws CatalogManagerException;

    QueryResult<Job> getAllJobs(int studyId) throws CatalogManagerException;

    String getJobStatus(int jobId, String sessionId) throws CatalogManagerException, IOException;

    QueryResult<ObjectMap> incJobVisits(int jobId) throws CatalogManagerException;

    QueryResult modifyJob(int jobId, ObjectMap parameters) throws CatalogManagerException;

    void setJobCommandLine(int jobId, String commandLine, String sessionId) throws CatalogManagerException, IOException;

    int getStudyIdByJobId(int jobId);

    QueryResult<Job> searchJob(QueryOptions options) throws CatalogManagerException;

    /**
     * Tool methods
     * ***************************
     */

    QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogManagerException;

    QueryResult<Tool> getTool(int id) throws CatalogManagerException;

    int getToolId(String userId, String toolAlias) throws CatalogManagerException;

//    QueryResult<Tool> searchTool(QueryOptions options);

//    int getJobIndex(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;

//    Project getJobProject(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;




    /**
     * Util methods
     * ***************************
     */
//    List<AnalysisPlugin> getUserAnalysis(String sessionId) throws CatalogManagerException, IOException;
//
//    List<Bucket> jsonToBucketList(String json) throws IOException;
//
//    ObjectItem getObjectFromBucket(String userId, String bucketId, Path objectId, String sessionId) throws CatalogManagerException, IOException;



}
