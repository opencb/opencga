package org.opencb.opencga.catalog.authorization;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

/**
 * Created by pfurio on 12/05/16.
 */
public interface AuthorizationManager {

    String ADMINS_ROLE = "admins";
    String DATA_MANAGERS_ROLE = "dataManagers";
    String MEMBERS_ROLE = "members";

    /**
     * Get the default Acls for the default roles.
     *
     * admins : Full permissions
     * dataManagers: Full data permissions. No study permissions
     * members: Just launch jobs permission.
     *
     * @param adminUsers Users to add to the admin group by default.
     * @return List<Role>
     */
    static List<StudyAcl> getDefaultAcls(Collection<String> adminUsers) {
        List<StudyAcl> studyAcls = new ArrayList<>(3);
        studyAcls.add(new StudyAcl("admin", new ArrayList<>(adminUsers), EnumSet.allOf(StudyAcl.StudyPermissions.class)));
        // TODO: Add all the default roles and permissions.
        return studyAcls;
    }

    void checkProjectPermission(long projectId, String userId, StudyAcl.StudyPermissions permission) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyAcl.StudyPermissions permission) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyAcl.StudyPermissions permission, String message) throws CatalogException;

    void checkFilePermission(long fileId, String userId, FileAcl.FilePermissions permission) throws CatalogException;

    void checkSamplePermission(long sampleId, String userId, SampleAcl.SamplePermissions permission) throws CatalogException;

    void checkIndividualPermission(long individualId, String userId, IndividualAcl.IndividualPermissions permission)
            throws CatalogException;

    void checkJobPermission(long jobId, String userId, JobAcl.JobPermissions permission) throws CatalogException;

    void checkCohortPermission(long cohortId, String userId, CohortAcl.CohortPermissions permission) throws CatalogException;

    void checkDatasetPermission(long datasetId, String userId, StudyAcl.StudyPermissions permission) throws CatalogException;

    //User.Role getUserRole(String userId) throws CatalogException;

    /**
     * Set the permissions given for all the users and file ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param fileIds Comma separated list of file ids.
     * @param userIds Comma separated list of user ids which the files will be shared with.
     * @param permissions List of file permissions.
     * @return A queryResult containing the FileAcl applied to the different file ids.
     * @throws CatalogException when the user ordering the action does not have permission to share the files.
     */
    QueryResult<FileAcl> setFilePermissions(String userId, String fileIds, String userIds, List<FileAcl.FilePermissions> permissions)
            throws CatalogException;

    /**
     * Remove the permissions given for all the users in the file ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param fileIds Comma separated list of file ids.
     * @param userIds Comma separated list of user ids from whom the permissions will be removed.
     * @throws CatalogException when the user ordering the action does not have permission to share the files.
     */
    void unsetFilePermissions(String userId, String fileIds, String userIds) throws CatalogException;

    /**
     * Set the permissions given for all the users and sample ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param sampleIds Comma separated list of sample ids.
     * @param userIds Comma separated list of user ids which the samples will be shared with.
     * @param permissions List of sample permissions.
     * @return A queryResult containing the SampleAcl applied to the different sample ids.
     * @throws CatalogException when the user ordering the action does not have permission to share the samples.
     */
    QueryResult<SampleAcl> setSamplePermissions(String userId, String sampleIds, String userIds,
                                              List<SampleAcl.SamplePermissions> permissions) throws CatalogException;

    /**
     * Remove the permissions given for all the users in the sample ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param sampleIds Comma separated list of sample ids.
     * @param userIds Comma separated list of user ids from whom the permissions will be removed.
     * @throws CatalogException when the user ordering the action does not have permission to share the samples.
     */
    void unsetSamplePermissions(String userId, String sampleIds, String userIds) throws CatalogException;

    /**
     * Set the permissions given for all the users and cohort ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param cohortIds Comma separated list of cohort ids.
     * @param userIds Comma separated list of user ids which the cohort will be shared with.
     * @param permissions List of cohort permissions.
     * @return A queryResult containing the CohortAcl applied to the different cohort ids.
     * @throws CatalogException when the user ordering the action does not have permission to share the cohorts.
     */
    QueryResult<CohortAcl> setCohortPermissions(String userId, String cohortIds, String userIds,
                                                List<CohortAcl.CohortPermissions> permissions) throws CatalogException;

    /**
     * Remove the permissions given for all the users in the cohort ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param cohortIds Comma separated list of cohort ids.
     * @param userIds Comma separated list of user ids from whom the permissions will be removed.
     * @throws CatalogException when the user ordering the action does not have permission to share the cohorts.
     */
    void unsetCohortPermissions(String userId, String cohortIds, String userIds) throws CatalogException;

    /**
     * Set the permissions given for all the users and individual ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param individualIds Comma separated list of individual ids.
     * @param userIds Comma separated list of user ids which the individual will be shared with.
     * @param permissions List of individual permissions.
     * @return A queryResult containing the IndividualAcl applied to the different individual ids.
     * @throws CatalogException when the user ordering the action does not have permission to share the individuals.
     */
    QueryResult<IndividualAcl> setIndividualPermissions(String userId, String individualIds, String userIds,
                                                List<IndividualAcl.IndividualPermissions> permissions) throws CatalogException;

    /**
     * Remove the permissions given for all the users in the individual ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param individualIds Comma separated list of individual ids.
     * @param userIds Comma separated list of user ids from whom the permissions will be removed.
     * @throws CatalogException when the user ordering the action does not have permission to share the individuals.
     */
    void unsetIndividualPermissions(String userId, String individualIds, String userIds) throws CatalogException;

    /**
     * Set the permissions given for all the users and job ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param jobIds Comma separated list of job ids.
     * @param userIds Comma separated list of user ids which the job will be shared with.
     * @param permissions List of job permissions.
     * @return A queryResult containing the JobAcl applied to the different job ids.
     * @throws CatalogException when the user ordering the action does not have permission to share the jobs.
     */
    QueryResult<JobAcl> setJobPermissions(String userId, String jobIds, String userIds, List<JobAcl.JobPermissions> permissions)
            throws CatalogException;

    /**
     * Remove the permissions given for all the users in the job ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param jobIds Comma separated list of job ids.
     * @param userIds Comma separated list of user ids from whom the permissions will be removed.
     * @throws CatalogException when the user ordering the action does not have permission to share the jobs.
     */
    void unsetJobPermissions(String userId, String jobIds, String userIds) throws CatalogException;

    /**
     * Removes from the list the projects that the user can not read.
     * From the remaining projects, filters the studies.
     *
     * @param userId  UserId.
     * @param projects Project list.
     * @throws CatalogException CatalogException
     */
    void filterProjects(String userId, List<Project> projects) throws CatalogException;

    /**
     * Removes from the list the studies that the user can not read.
     * From the remaining studies, filters the files.
     *
     * @param userId  UserId.
     * @param studies Studies list.
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

    /**
     * Removes from the list the individuals that the user can not read.
     *
     * @param userId  UserId
     * @param studyId StudyId
     * @param individuals Individuals
     * @throws CatalogException CatalogException
     */
    void filterIndividuals(String userId, long studyId, List<Individual> individuals) throws CatalogException;

    /**
     * Removes from the list the cohorts that the user can not read.
     *
     * @param userId  UserId.
     * @param studyId StudyId.
     * @param cohorts Cohorts.
     * @throws CatalogException CatalogException.
     */
    void filterCohorts(String userId, long studyId, List<Cohort> cohorts) throws CatalogException;

    /**
     * Removes from the list the jobs that the user can not read.
     *
     * @param userId  UserId.
     * @param studyId StudyId.
     * @param jobs Jobs.
     * @throws CatalogException CatalogException.
     */
    void filterJobs(String userId, long studyId, List<Job> jobs) throws CatalogException;

    /**
     * Retrieves the groupId where the user belongs to.
     *
     * @param studyId study id.
     * @param userId user id.
     * @return the group id of the user.
     * @throws CatalogException when there is any database error.
     */
    String getGroupBelonging(long studyId, String userId) throws CatalogException;

    /**
     * Retrieves the StudyAcl where the user/group belongs to.
     *
     * @param studyId study id.
     * @param userId user or group id. For groups, the string starts with @.
     * @return the studyAcl where the user/group belongs to.
     * @throws CatalogException when there is any database error.
     */
    StudyAcl getStudyAclBelonging(long studyId, String userId) throws CatalogException;

    /**
     * Adds the newUser to the groupId specified.
     *
     * @param userId User id of the user ordering the action.
     * @param studyId Study id under which the newUserId will be added to the group.
     * @param groupId Group id where the userId wants to add the newUser.
     * @param newUserId User id that will be added to the group.
     * @return a queryResult containing the group created.
     * @throws CatalogException when the userId does not have the proper permissions to add other users to groups.
     */
    QueryResult<Group> addMember(String userId, long studyId, String groupId, String newUserId) throws CatalogException;

    /**
     * Removes the oldUserId from the groupId specified.
     *
     * @param userId User id of the user ordering the action.
     * @param studyId Study id under which the oldUserId will be removed from the groupId.
     * @param groupId Group id where the userId wants to remove the oldUserId from.
     * @param oldUserId User id that will be taken out from the group.
     * @return a queryResult containing the group.
     * @throws CatalogException when the userId does not have the proper permissions to remove other users from groups.
     */
    QueryResult<Group> removeMember(String userId, long studyId, String groupId, String oldUserId) throws CatalogException;

}
