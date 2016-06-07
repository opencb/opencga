package org.opencb.opencga.catalog.authorization.old;


import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface AuthorizationManager {

    String ADMINS_ROLE = "admins";
    String DATA_MANAGERS_ROLE = "dataManagers";
    String MEMBERS_ROLE = "members";

    /**
     * Get tree default roles.
     * admins : Full permissions
     * dataManagers: Full data permissions. No study permissions
     * members: Just launch jobs permission.
     *
     * @param adminUsers Users to add to the admin group by default.
     * @return List<Role>
     */
    static List<Role> getDefaultRoles(Collection<String> adminUsers) {
        return Arrays.asList(
                new Role(ADMINS_ROLE, new ArrayList<>(adminUsers), new StudyPermissions(true, true, true, true, true, true, true)),
                new Role(DATA_MANAGERS_ROLE, Collections.emptyList(), new StudyPermissions(true, true, true, true, true, true, false)),
                new Role(MEMBERS_ROLE, Collections.emptyList(), new StudyPermissions(false, false, false, true, false, false, false)));
    }

    void checkProjectPermission(long projectId, String userId, CatalogPermission permission) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyPermission permission) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyPermission permission, String message) throws CatalogException;

    void checkFilePermission(long fileId, String userId, CatalogPermission permission) throws CatalogException;

    void checkSamplePermission(long sampleId, String userId, CatalogPermission permission) throws CatalogException;

    /**
     * Can read to an individual if.
     * a) User is SAMPLE_MANAGER, role:ADMIN or studyOwner
     * b) User can read some related sample
     * Any other permission require to be SAMPLE_MANAGER, role:ADMIN or studyOwner
     *
     * @param individualId individualId
     * @param userId       userId
     * @param permission   Permission
     * @throws CatalogException CatalogException
     */
    void checkIndividualPermission(long individualId, String userId, CatalogPermission permission) throws CatalogException;

    void checkReadJob(String userId, long jobId) throws CatalogException;

    void checkReadJob(String userId, Job job) throws CatalogException;

    void checkReadCohort(String userId, Cohort cohort) throws CatalogException;

    User.Role getUserRole(String userId) throws CatalogException;

    /***
     * Set the ACL given for all the users and file ids given.
     *
     * @param fileIds File ids separated by commas where the permissions have to be applied.
     * @param userIds User ids for which the permissions will be set.
     * @param acl ACL containing the permission that are going to be set. User ACL will be set for each of the userIds.
     * @param sessionId Session ID of the user ordering the update.
     * @return A queryResult containing all the ACL entries that have been set.
     * @throws CatalogException when the user corresponding to the sessionId does not have permission in the files.
     */
    QueryResult setFileACL(String fileIds, String userIds, AclEntry acl, String sessionId) throws CatalogException;

    /***
     * Unset the ACL given for all the users and sample ids given.
     *
     * @param fileIds File ids separated by commas where the ACLs will be taken out.
     * @param userIds User ids for which the permissions will be unset.
     * @param sessionId Session ID of the user ordering the update.
     * @return A queryResult containing all the ACL entries that have been set.
     * @throws CatalogException when the user corresponding to the sessionId does not have permission in the files.
     */
    QueryResult unsetFileACL(String fileIds, String userIds, String sessionId) throws CatalogException;

    /***
     * Set the ACL given for all the users and sample ids given.
     *
     * @param sampleIds Sample ids separated by commas where the permissions have to be applied.
     * @param userIds User ids for which the permissions will be set.
     * @param acl ACL containing the permission that are going to be set. User ACL will be set for each of the userIds.
     * @param sessionId Session ID of the user ordering the update.
     * @return A queryResult containing all the ACL entries that have been set.
     * @throws CatalogException when the user corresponding to the sessionId does not have permission in the samples.
     */
    @Deprecated
    QueryResult<AclEntry> setSampleACL(String sampleIds, String userIds, AclEntry acl, String sessionId) throws CatalogException;

    /***
     * Unset the ACL given for all the users and sample ids given.
     *
     * @param sampleIds Sample ids separated by commas where the ACLs will be taken out.
     * @param userIds User ids for which the permissions will be unset.
     * @param sessionId Session ID of the user ordering the update.
     * @return A queryResult containing all the ACL entries that have been set.
     * @throws CatalogException when the user corresponding to the sessionId does not have permission in the samples.
     */
    QueryResult unsetSampleACL(String sampleIds, String userIds, String sessionId) throws CatalogException;

    /**
     * Removes from the list the projects that the user can not read.
     * From the remaining projects, filters the studies and files.
     *
     * @param userId   UserId
     * @param projects Projects list
     * @throws CatalogException CatalogException
     */
    void filterProjects(String userId, List<Project> projects) throws CatalogException;

    /**
     * Removes from the list the studies that the user can not read.
     * From the remaining studies, filters the files.
     *
     * @param userId  UserId
     * @param studies Studies list
     * @throws CatalogException CatalogException
     */
    void filterStudies(String userId, List<Study> studies) throws CatalogException;

    /**
     * Removes from the list the files that the user can not read.
     *
     * @param userId  UserId
     * @param studyId StudyId
     * @param files   Files list
     * @throws CatalogException CatalogException
     */
    void filterFiles(String userId, long studyId, List<File> files) throws CatalogException;

    /**
     * Removes from the list the samples that the user can not read.
     *
     * @param userId  UserId
     * @param studyId StudyId
     * @param samples Samples
     * @throws CatalogException CatalogException
     */
    void filterSamples(String userId, long studyId, List<Sample> samples) throws CatalogException;

    /*--------------------------*/
    // Group management methods
    /*--------------------------*/

    void filterJobs(String userId, List<Job> jobs) throws CatalogException;

    void filterJobs(String userId, List<Job> jobs, Long studyId) throws CatalogException;

    void filterCohorts(String userId, long studyId, List<Cohort> cohorts) throws CatalogException;

    void filterIndividuals(String userId, long studyId, List<Individual> individuals) throws CatalogException;

    Group getGroupBelonging(long studyId, String userId) throws CatalogException;

    Role getRoleBelonging(long studyId, String userId, String groupId) throws CatalogException;

//    Group createGroup(int studyId, String groupId, GroupPermissions groupPermissions, String sessionId) throws CatalogException;

//    void deleteGroup(int studyId, String groupId, String sessionId) throws CatalogException;

    @Deprecated
    QueryResult<Group> addMember(long studyId, String groupId, String userId, String sessionId) throws CatalogException;

    @Deprecated
    QueryResult<Group> removeMember(long studyId, String groupId, String userId, String sessionId) throws CatalogException;
}
