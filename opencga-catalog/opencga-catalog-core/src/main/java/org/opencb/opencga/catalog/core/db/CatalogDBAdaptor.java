package org.opencb.opencga.catalog.core.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.*;

import java.io.IOException;
import java.nio.file.Path;

public interface CatalogDBAdaptor {

    /**
     * User methods ···
     * ***************************
     */
    boolean checkUserCredentials(String userId, String sessionId);

    QueryResult createUser(User user) throws CatalogManagerException, JsonProcessingException;

    QueryResult deleteUser(String userId) throws CatalogManagerException, JsonProcessingException;


    QueryResult login(String userId, String password, Session session) throws CatalogManagerException, IOException;

    QueryResult logout(String userId, String sessionId) throws CatalogManagerException, IOException;

    QueryResult loginAsAnonymous(Session session) throws CatalogManagerException, IOException;

    QueryResult logoutAnonymous(String sessionId);


    // public String getUserByAccountId(String accountId, String sessionId);

    // public String getUserByEmail(String email, String sessionId);

    QueryResult getUser(String userId, String lastActivity, String sessionId) throws CatalogManagerException;

    QueryResult changePassword(String userId, String password, String password1, String sessionId) throws CatalogManagerException;

    QueryResult changeEmail(String userId, String nEmail, String sessionId) throws CatalogManagerException;

    QueryResult resetPassword(String userId, String email) throws CatalogManagerException;

    // public boolean checkSessionId(String userId, String sessionId);

    QueryResult getSession(String userId, String sessionId) throws IOException;

    String getUserIdBySessionId(String sessionId);

    /**
     * Project methods ···
     * ***************************
     */
    QueryResult createProject(String userId, Project project, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult getProject(String userId, String project, String sessionId) throws CatalogManagerException;

    QueryResult getAllProjects(String userId, String sessionId) throws CatalogManagerException;

    int getProjectId(String userId, String project);
    /**
     * Study methods ···
     * ***************************
     */

    QueryResult createStudy(String userId, String project, Study study, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult getAllStudies(String userId, String project, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult getStudy(int studyId, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult renameStudy(String userId, String projectAlias, String studyAlias, String newStudyName, String sessionId) throws CatalogManagerException;

    QueryResult renameStudy(int studyId, String newStudyName, String sessionId) throws CatalogManagerException;

    QueryResult deleteStudy(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException;

    QueryResult deleteStudy(int studyId, String sessionId) throws CatalogManagerException;

    int getStudyId(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException, IOException;


    /**
     * File methods ···
     * ***************************
     */

    // add file to study
    QueryResult createFileToStudy(String userId, String projectAlias, String studyAlias, File file, String sessionId) throws CatalogManagerException, JsonProcessingException;
    QueryResult createFileToStudy(int studyId, File file, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult deleteFile(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId) throws CatalogManagerException;
    QueryResult deleteFile(int studyId, Path filePath, String sessionId) throws CatalogManagerException;
    QueryResult deleteFile(int fileId, String sessionId) throws CatalogManagerException;

    QueryResult deleteFilesFromStudy(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException;
    QueryResult deleteFilesFromStudy(int studyId, String studyAlias, String sessionId) throws CatalogManagerException;

    int getFileId(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId) throws CatalogManagerException, IOException;
    int getFileId(int studyId, Path filePath, String sessionId) throws CatalogManagerException, IOException;

    QueryResult getFile(String userId, String projectAlias, String studyAlias, Path filePath, String sessionId) throws CatalogManagerException, IOException;
    QueryResult getFile(int studyId, Path filePath, String sessionId) throws CatalogManagerException, IOException;
    QueryResult getFile(int fileId, String sessionId) throws CatalogManagerException, IOException;

    QueryResult setFileStatus(String userId, String projectAlias, String studyAlias, Path filePath, String status, String sessionId) throws CatalogManagerException, IOException;
    QueryResult setFileStatus(int studyId, Path filePath, String status, String sessionId) throws CatalogManagerException, IOException;
    QueryResult setFileStatus(int fileId, String status, String sessionId) throws CatalogManagerException, IOException;

    // TODO: void shareObject(String userId, String bucketId, Path objectId, Acl acl, String sessionId) throws CatalogManagerException;



    /**
     * Analysis methods ···
     * ***************************
     */

    QueryResult getAnalysisList(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException;
    QueryResult getAnalysisList(int studyId, String sessionId) throws CatalogManagerException;

    QueryResult createAnalysis(String userId, String projectAlias, String studyAlias, Analysis analysis, String sessionId) throws CatalogManagerException, JsonProcessingException;
    QueryResult createAnalysis(int studyId, Analysis analysis, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult<Job> createJob(int studyId, String analysisName, Job job, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult deleteJob(int jobId, String sessionId) throws CatalogManagerException;

    Job getJob(int jobId, String sessionId) throws CatalogManagerException, IOException;

    String getJobStatus(int jobId, String sessionId) throws CatalogManagerException, IOException;

    void incJobVisites(int jobId, String sessionId) throws CatalogManagerException, IOException;

    void setJobCommandLine(int jobId, String commandLine, String sessionId) throws CatalogManagerException, IOException;

//    int getJobIndex(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;

//    Project getJobProject(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;




    /**
     * Util methods ···
     * ***************************
     */
//    List<AnalysisPlugin> getUserAnalysis(String sessionId) throws CatalogManagerException, IOException;
//
//    List<Bucket> jsonToBucketList(String json) throws IOException;
//
//    ObjectItem getObjectFromBucket(String userId, String bucketId, Path objectId, String sessionId) throws CatalogManagerException, IOException;



}
