package org.opencb.opencga.catalog.authorization;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.*;

import java.util.*;

/**
 * Created by pfurio on 12/05/16.
 */
public interface AuthorizationManager {

    String FILTER_ROUTE_STUDIES = "projects.studies.";
    String FILTER_ROUTE_COHORTS = "projects.studies.cohorts.";
    String FILTER_ROUTE_DATASETS = "projects.studies.datasets.";
    String FILTER_ROUTE_INDIVIDUALS = "projects.studies.individuals.";
    String FILTER_ROUTE_SAMPLES = "projects.studies.samples.";
    String FILTER_ROUTE_FILES = "projects.studies.files.";
    String FILTER_ROUTE_JOBS = "projects.studies.jobs.";

    String ADMINS_ROLE = "admins";
    String DATA_MANAGERS_ROLE = "dataManagers";
    String MEMBERS_ROLE = "members";

    String OTHER_USERS_ID = "*";

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

    void checkDatasetPermission(long datasetId, String userId, DatasetAcl.DatasetPermissions permission) throws CatalogException;

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
    QueryResult<FileAcl> setFilePermissions(String userId, String fileIds, String userIds, List<String> permissions)
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
    QueryResult<SampleAcl> setSamplePermissions(String userId, String sampleIds, String userIds, List<String> permissions)
            throws CatalogException;

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
    QueryResult<CohortAcl> setCohortPermissions(String userId, String cohortIds, String userIds, List<String> permissions)
            throws CatalogException;

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
    QueryResult<IndividualAcl> setIndividualPermissions(String userId, String individualIds, String userIds, List<String> permissions)
            throws CatalogException;

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
    QueryResult<JobAcl> setJobPermissions(String userId, String jobIds, String userIds, List<String> permissions)
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
     * Set the permissions given for all the users and dataset ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param datasetIds Comma separated list of dataset ids.
     * @param userIds Comma separated list of user ids which the dataset will be shared with.
     * @param permissions List of dataset permissions.
     * @return A queryResult containing the DatasetAcl applied to the different dataset ids.
     * @throws CatalogException when the user ordering the action does not have permission to share the datasets.
     */
    QueryResult<DatasetAcl> setDatasetPermissions(String userId, String datasetIds, String userIds, List<String> permissions)
            throws CatalogException;

    /**
     * Remove the permissions given for all the users in the dataset ids given.
     *
     * @param userId User id of the user that is performing the action.
     * @param datasetIds Comma separated list of dataset ids.
     * @param userIds Comma separated list of user ids from whom the permissions will be removed.
     * @throws CatalogException when the user ordering the action does not have permission to share the datasets.
     */
    void unsetDatasetPermissions(String userId, String datasetIds, String userIds) throws CatalogException;

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
     * Removes from the list the datasets that the user can not read.
     *
     * @param userId  UserId.
     * @param studyId StudyId.
     * @param datasets datasets.
     * @throws CatalogException CatalogException.
     */
    void filterDatasets(String userId, long studyId, List<Dataset> datasets) throws CatalogException;

    /**
     * Adds the members to the groupId specified.
     *
     * @param userId User id of the user ordering the action.
     * @param studyId Study id under which the newUserId will be added to the group.
     * @param groupId Group id where the userId wants to add the newUser.
     * @param members List of user ids that will be added to the group.
     * @return a queryResult containing the group created.
     * @throws CatalogException when the userId does not have the proper permissions to add other users to groups.
     */
    QueryResult<Group> addUsersToGroup(String userId, long studyId, String groupId, List<String> members) throws CatalogException;
    default QueryResult<Group> addUsersToGroup(String userId, long studyId, String groupId, String members) throws CatalogException {
        return addUsersToGroup(userId, studyId, groupId, Arrays.asList(members.split(",")));
    }

    /**
     * Removes the members from the groupId specified.
     *
     * @param userId User id of the user ordering the action.
     * @param studyId Study id under which the oldUserId will be removed from the groupId.
     * @param groupId Group id where the userId wants to remove the oldUserId from.
     * @param members List of user ids that will be taken out from the group.
     * @throws CatalogException when the userId does not have the proper permissions to remove other users from groups.
     */
    void removeUsersFromGroup(String userId, long studyId, String groupId, List<String> members) throws CatalogException;
    default void removeUsersFromGroup(String userId, long studyId, String groupId, String members) throws CatalogException {
        removeUsersFromGroup(userId, studyId, groupId, Arrays.asList(members.split(",")));
    }

    /**
     * Adds the list of members to the roleId specified.
     *  @param userId User id of the user ordering the action.
     * @param studyId Study id under which the members will be added to the role.
     * @param members List of member ids (users and/or groups).
     * @param roleId Role id where the members will be added to.
     * @return a queryResult containing the complete studyAcl where the members have been added to.
     * @throws CatalogException when the userId does not have the proper permissions or the members or the roleId do not exist.
     */
    QueryResult<StudyAcl> addMembersToRole(String userId, long studyId, List<String> members, String roleId) throws CatalogException;
    default QueryResult<StudyAcl> addMembersToRole(String userId, long studyId, String members, String roleId)
            throws CatalogException {
        return addMembersToRole(userId, studyId, Arrays.asList(members.split(",")), roleId);
    }
    /**
     * Removes the members from the roleId specified.
     *
     * @param userId User id of the user ordering the action.
     * @param studyId Study id under which the members will be removed from the groupId.
     * @param members List of member ids (users and/or groups).
     * @throws CatalogException when the userId does not have the proper permissions to remove other users from roles, or the members or
     * roleId do not exist.
     */
    void removeMembersFromRole(String userId, long studyId, List<String> members) throws CatalogException;
    default void removeMembersFromRole(String userId, long studyId, String members)
            throws CatalogException {
        removeMembersFromRole(userId, studyId, Arrays.asList(members.split(",")));
    }

    /**
     * Checks if the userId belongs to one role or not.
     *
     * @param studyId study id.
     * @param userId User id.
     * @return true if the user belongs to one role. False otherwise.
     * @throws CatalogException CatalogException.
     */
    boolean userHasPermissionsInStudy(long studyId, String userId) throws CatalogException;
}
