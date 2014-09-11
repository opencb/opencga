package org.opencb.opencga.catalog.core.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.cellbase.core.lib.dbquery.QueryResult;
import org.opencb.commons.containers.map.ObjectMap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface CatalogManager {

    /**
     * Account methods ···
     * ***************************
     */
    boolean checkAccountCredentials(String accountId, String sessionId);

    org.opencb.commons.containers.QueryResult<ObjectMap> createAccount(String accountId, String password, String accountName, String role, String email, Session session) throws AccountManagementException, JsonProcessingException;

    org.opencb.commons.containers.QueryResult<ObjectMap> deleteAccount(String accountId) throws AccountManagementException, JsonProcessingException;

    org.opencb.commons.containers.QueryResult<ObjectMap> createAnonymousAccount(String accountId, String password, Session session) throws AccountManagementException, IOException;

    org.opencb.commons.containers.QueryResult<ObjectMap> login(String accountId, String password, Session session) throws AccountManagementException, IOException;

    org.opencb.commons.containers.QueryResult<ObjectMap> logout(String accountId, String sessionId) throws AccountManagementException, IOException;

    org.opencb.commons.containers.QueryResult<ObjectMap> logoutAnonymous(String accountId, String sessionId);

    // public String getUserByAccountId(String accountId, String sessionId);

    // public String getUserByEmail(String email, String sessionId);

    org.opencb.commons.containers.QueryResult<ObjectMap> getAccountInfo(String accountId, String sessionId, String lastActivity) throws AccountManagementException;

    org.opencb.commons.containers.QueryResult<ObjectMap> changePassword(String accountId, String sessionId, String password, String nPassword1, String nPassword2) throws AccountManagementException;

    org.opencb.commons.containers.QueryResult<ObjectMap> changeEmail(String accountId, String sessionId, String nEmail) throws AccountManagementException;

    org.opencb.commons.containers.QueryResult<ObjectMap> resetPassword(String acccountId, String email) throws AccountManagementException;

    /**
     * Bucket methods ···
     * ***************************
     */

    // public boolean checkSessionId(String accountId, String sessionId);

    Session getSession(String accountId, String sessionId) throws IOException;

    org.opencb.commons.containers.QueryResult<ObjectMap> getBucketsList(String accountId, String sessionId) throws AccountManagementException, JsonProcessingException;

    org.opencb.commons.containers.QueryResult<ObjectMap> createBucket(String accountId, Bucket bucket, String sessionId) throws AccountManagementException, JsonProcessingException;

    org.opencb.commons.containers.QueryResult<ObjectMap> renameBucket(String accountId, String bucketId, String newBucketId, String sessionId) throws AccountManagementException;

    org.opencb.commons.containers.QueryResult<ObjectMap> deleteBucket(String accountId, String bucketId, String sessionId) throws AccountManagementException;

    // add file to project
    org.opencb.commons.containers.QueryResult<ObjectMap> createObjectToBucket(String accountId, String bucketId, ObjectItem objectItem, String sessionId) throws AccountManagementException, JsonProcessingException;

    org.opencb.commons.containers.QueryResult<ObjectMap> deleteObjectFromBucket(String accountId, String bucketId, Path objectId, String sessionId) throws AccountManagementException;

    org.opencb.commons.containers.QueryResult<ObjectMap> deleteObjectsFromBucket(String accountId, String bucketId, String sessionId) throws AccountManagementException;

    int getObjectIndex(String accountId, String bucketId, Path objectId, String sessionId) throws AccountManagementException, IOException;

    org.opencb.commons.containers.QueryResult<ObjectMap> setObjectStatus(String accountId, String bucketId, Path objectId, String status, String sessionId) throws AccountManagementException, IOException;

    void shareObject(String accountId, String bucketId, Path objectId, Acl acl, String sessionId) throws AccountManagementException;

    /**
     * Project methods ···
     * ***************************
     */
    org.opencb.commons.containers.QueryResult<ObjectMap> getProjectsList(String accountId, String sessionId) throws AccountManagementException;

    org.opencb.commons.containers.QueryResult<ObjectMap> createProject(String accountId, Project project, String sessionId) throws AccountManagementException, JsonProcessingException;

    org.opencb.commons.containers.QueryResult<Job> createJob(String accountId, String projectId, Job job, String sessionId) throws AccountManagementException, JsonProcessingException;

    org.opencb.commons.containers.QueryResult<ObjectMap> deleteJobFromProject(String accountId, String projectId, String jobId, String sessionId) throws AccountManagementException;

    Job getJob(String accountId, String jobId, String sessionId) throws AccountManagementException, IOException;

    Path getJobPath(String accountId, String jobId, String sessionId) throws AccountManagementException, IOException;

    String getJobStatus(String accountId, String jobId, String sessionId) throws AccountManagementException, IOException;

    void incJobVisites(String accountId, String jobId, String sessionId) throws AccountManagementException, IOException;

    void setJobCommandLine(String accountId, String jobId, String commandLine, String sessionId) throws AccountManagementException, IOException;

    int getJobIndex(String accountId, String jobId, String sessionId) throws AccountManagementException, IOException;

    Project getJobProject(String accountId, String jobId, String sessionId) throws AccountManagementException, IOException;

    /**
     * Util methods ···
     * ***************************
     */
    List<AnalysisPlugin> getUserAnalysis(String sessionId) throws AccountManagementException, IOException;

    List<Bucket> jsonToBucketList(String json) throws IOException;

    ObjectItem getObjectFromBucket(String accountId, String bucketId, Path objectId, String sessionId) throws AccountManagementException, IOException;

    String getAccountIdBySessionId(String sessionId);

}
