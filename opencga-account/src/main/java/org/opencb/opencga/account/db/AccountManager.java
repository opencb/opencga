package org.opencb.opencga.account.db;

import org.opencb.opencga.account.beans.*;

import java.nio.file.Path;
import java.util.List;

public interface AccountManager {

    /**
     * Account methods ···
     * ***************************
     */
    void createAccount(String accountId, String password, String accountName, String role, String email, Session session) throws AccountManagementException;

    String createAnonymousAccount(String accountId, String password, Session session) throws AccountManagementException;

    String login(String accountId, String password, Session session) throws AccountManagementException;

    void logout(String accountId, String sessionId) throws AccountManagementException;

    void logoutAnonymous(String accountId, String sessionId);

    // public String getUserByAccountId(String accountId, String sessionId);

    // public String getUserByEmail(String email, String sessionId);

    String getAccountInfo(String accountId, String sessionId, String lastActivity) throws AccountManagementException;

    void changePassword(String accountId, String sessionId, String password, String nPassword1, String nPassword2) throws AccountManagementException;

    void changeEmail(String accountId, String sessionId, String nEmail) throws AccountManagementException;

    void resetPassword(String acccountId, String email) throws AccountManagementException;

    /**
     * Bucket methods ···
     * ***************************
     */

    // public boolean checkSessionId(String accountId, String sessionId);

    Session getSession(String accountId, String sessionId);

    String getBucketsList(String accountId, String sessionId) throws AccountManagementException;

    void createBucket(String accountId, Bucket bucket, String sessionId) throws AccountManagementException;

    void renameBucket(String accountId, String bucketId, String newBucketId, String sessionId) throws AccountManagementException;

    void deleteBucket(String accountId, String bucketId, String sessionId) throws AccountManagementException;

    // add file to project
    void createObjectToBucket(String accountId, String bucketId, ObjectItem objectItem, String sessionId) throws AccountManagementException;

    void deleteObjectFromBucket(String accountId, String bucketId, Path objectId, String sessionId) throws AccountManagementException;

    void deleteObjectsFromBucket(String accountId, String bucketId, String sessionId) throws AccountManagementException;

    int getObjectIndex(String accountId, String bucketId, Path objectId, String sessionId) throws AccountManagementException;

    void setObjectStatus(String accountId, String bucketId, Path objectId, String status, String sessionId) throws AccountManagementException;

    void shareObject(String accountId, String bucketId, Path objectId, Acl acl, String sessionId) throws AccountManagementException;

    /**
     * Project methods ···
     * ***************************
     */
    String getProjectsList(String accountId, String sessionId) throws AccountManagementException;

    void createProject(String accountId, Project project, String sessionId) throws AccountManagementException;

    void createJob(String accountId, String projectId, Job job, String sessionId) throws AccountManagementException;

    void deleteJobFromProject(String accountId, String projectId, String jobId, String sessionId) throws AccountManagementException;

    Job getJob(String accountId, String jobId, String sessionId) throws AccountManagementException;

    Path getJobPath(String accountId, String jobId, String sessionId) throws AccountManagementException;

    String getJobStatus(String accountId, String jobId, String sessionId) throws AccountManagementException;

    void incJobVisites(String accountId, String jobId, String sessionId) throws AccountManagementException;

    void setJobCommandLine(String accountId, String jobId, String commandLine, String sessionId) throws AccountManagementException;

    int getJobIndex(String accountId, String jobId, String sessionId) throws AccountManagementException;

    Project getJobProject(String accountId, String jobId, String sessionId) throws AccountManagementException;

    /**
     * Util methods ···
     * ***************************
     */
    List<AnalysisPlugin> getUserAnalysis(String sessionId) throws AccountManagementException;

    List<Bucket> jsonToBucketList(String json);

    ObjectItem getObjectFromBucket(String accountId, String bucketId, Path objectId, String sessionId) throws AccountManagementException;

    String getAccountIdBySessionId(String sessionId);

}
