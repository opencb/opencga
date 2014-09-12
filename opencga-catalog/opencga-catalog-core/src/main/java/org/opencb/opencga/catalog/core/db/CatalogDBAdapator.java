package org.opencb.opencga.catalog.core.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface CatalogDBAdapator {

    /**
     * User methods ···
     * ***************************
     */
    boolean checkUserCredentials(String userId, String sessionId);

    QueryResult<ObjectMap> createUser(String userId, String password, String userName, String role, String email, Session session) throws CatalogManagerException, JsonProcessingException;

    QueryResult<ObjectMap> deleteUser(String userId) throws CatalogManagerException, JsonProcessingException;

    QueryResult<ObjectMap> createAnonymousUser(String userId, String password, Session session) throws CatalogManagerException, IOException;

    QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogManagerException, IOException;

    QueryResult<ObjectMap> logout(String userId, String sessionId) throws CatalogManagerException, IOException;

    QueryResult<ObjectMap> logoutAnonymous(String userId, String sessionId);

    // public String getUserByAccountId(String accountId, String sessionId);

    // public String getUserByEmail(String email, String sessionId);

    QueryResult<ObjectMap> getUserInfo(String userId, String sessionId, String lastActivity) throws CatalogManagerException;

    QueryResult<ObjectMap> changePassword(String userId, String sessionId, String password, String nPassword1, String nPassword2) throws CatalogManagerException;

    QueryResult<ObjectMap> changeEmail(String userId, String sessionId, String nEmail) throws CatalogManagerException;

    QueryResult<ObjectMap> resetPassword(String userId, String email) throws CatalogManagerException;

    /**
     * Study methods ···
     * ***************************
     */

    // public boolean checkSessionId(String userId, String sessionId);

    Session getSession(String userId, String sessionId) throws IOException;

    QueryResult<ObjectMap> getStudiesList(String userId, String projectId, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult<ObjectMap> createStudy(String userId, Study study, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult<ObjectMap> renameStudy(String userId, String studyId, String newstudyId, String sessionId) throws CatalogManagerException;

    QueryResult<ObjectMap> deleteStudy(String userId, String studyId, String sessionId) throws CatalogManagerException;

    // add file to project
    QueryResult<ObjectMap> createFileToStudy(String userId, String studyId, File file, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult<ObjectMap> deleteObjectFromBucket(String userId, String bucketId, Path objectId, String sessionId) throws CatalogManagerException;

    QueryResult<ObjectMap> deleteObjectsFromBucket(String userId, String bucketId, String sessionId) throws CatalogManagerException;

    int getObjectIndex(String userId, String bucketId, Path objectId, String sessionId) throws CatalogManagerException, IOException;

    QueryResult<ObjectMap> setObjectStatus(String userId, String bucketId, Path objectId, String status, String sessionId) throws CatalogManagerException, IOException;

    void shareObject(String userId, String bucketId, Path objectId, Acl acl, String sessionId) throws CatalogManagerException;

    /**
     * Project methods ···
     * ***************************
     */
    QueryResult<ObjectMap> getProjectsList(String userId, String sessionId) throws CatalogManagerException;

    QueryResult<ObjectMap> createProject(String userId, Project project, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult<Job> createJob(String userId, String projectId, Job job, String sessionId) throws CatalogManagerException, JsonProcessingException;

    QueryResult<ObjectMap> deleteJobFromProject(String userId, String projectId, String jobId, String sessionId) throws CatalogManagerException;

    Job getJob(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;

    Path getJobPath(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;

    String getJobStatus(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;

    void incJobVisites(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;

    void setJobCommandLine(String userId, String jobId, String commandLine, String sessionId) throws CatalogManagerException, IOException;

    int getJobIndex(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;

    Project getJobProject(String userId, String jobId, String sessionId) throws CatalogManagerException, IOException;

    /**
     * Util methods ···
     * ***************************
     */
    List<AnalysisPlugin> getUserAnalysis(String sessionId) throws CatalogManagerException, IOException;

    List<Bucket> jsonToBucketList(String json) throws IOException;

    ObjectItem getObjectFromBucket(String userId, String bucketId, Path objectId, String sessionId) throws CatalogManagerException, IOException;

    String getUserIdBySessionId(String sessionId);

}
