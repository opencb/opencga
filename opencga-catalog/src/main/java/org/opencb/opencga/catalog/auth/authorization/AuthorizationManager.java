package org.opencb.opencga.catalog.auth.authorization;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.*;

import javax.annotation.Nullable;
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

    String ROLE_ADMIN = "admin";
    String ROLE_ANALYST = "analyst";
    String ROLE_LOCKED = "locked";

    String OTHER_USERS_ID = "*";

    static EnumSet<StudyAclEntry.StudyPermissions> getAdminAcls() {
        return EnumSet.allOf(StudyAclEntry.StudyPermissions.class);
    }

    static EnumSet<StudyAclEntry.StudyPermissions> getAnalystAcls() {
        return EnumSet.of(StudyAclEntry.StudyPermissions.VIEW_STUDY,
                StudyAclEntry.StudyPermissions.UPDATE_STUDY, StudyAclEntry.StudyPermissions.CREATE_VARIABLE_SET,
                StudyAclEntry.StudyPermissions.VIEW_VARIABLE_SET, StudyAclEntry.StudyPermissions.UPDATE_VARIABLE_SET,
                StudyAclEntry.StudyPermissions.CREATE_FILES, StudyAclEntry.StudyPermissions.VIEW_FILE_HEADERS,
                StudyAclEntry.StudyPermissions.VIEW_FILE_CONTENTS, StudyAclEntry.StudyPermissions.VIEW_FILES,
                StudyAclEntry.StudyPermissions.UPDATE_FILES, StudyAclEntry.StudyPermissions.DOWNLOAD_FILES,
                StudyAclEntry.StudyPermissions.CREATE_JOBS, StudyAclEntry.StudyPermissions.VIEW_JOBS,
                StudyAclEntry.StudyPermissions.UPDATE_JOBS, StudyAclEntry.StudyPermissions.CREATE_SAMPLES,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLES, StudyAclEntry.StudyPermissions.UPDATE_SAMPLES,
                StudyAclEntry.StudyPermissions.CREATE_SAMPLE_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.UPDATE_SAMPLE_ANNOTATIONS, StudyAclEntry.StudyPermissions.CREATE_INDIVIDUALS,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS, StudyAclEntry.StudyPermissions.UPDATE_INDIVIDUALS,
                StudyAclEntry.StudyPermissions.CREATE_INDIVIDUAL_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.UPDATE_INDIVIDUAL_ANNOTATIONS, StudyAclEntry.StudyPermissions.CREATE_COHORTS,
                StudyAclEntry.StudyPermissions.VIEW_COHORTS, StudyAclEntry.StudyPermissions.UPDATE_COHORTS,
                StudyAclEntry.StudyPermissions.CREATE_COHORT_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.UPDATE_COHORT_ANNOTATIONS, StudyAclEntry.StudyPermissions.CREATE_DATASETS,
                StudyAclEntry.StudyPermissions.VIEW_DATASETS, StudyAclEntry.StudyPermissions.UPDATE_DATASETS,
                StudyAclEntry.StudyPermissions.CREATE_PANELS, StudyAclEntry.StudyPermissions.VIEW_PANELS,
                StudyAclEntry.StudyPermissions.UPDATE_PANELS);
    }

    static EnumSet<StudyAclEntry.StudyPermissions> getLockedAcls() {
        return EnumSet.noneOf(StudyAclEntry.StudyPermissions.class);
    }

    void checkProjectPermission(long projectId, String userId, StudyAclEntry.StudyPermissions permission) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyAclEntry.StudyPermissions permission) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyAclEntry.StudyPermissions permission, String message)
            throws CatalogException;

    void checkFilePermission(long fileId, String userId, FileAclEntry.FilePermissions permission) throws CatalogException;

    void checkSamplePermission(long sampleId, String userId, SampleAclEntry.SamplePermissions permission) throws CatalogException;

    void checkIndividualPermission(long individualId, String userId, IndividualAclEntry.IndividualPermissions permission)
            throws CatalogException;

    void checkJobPermission(long jobId, String userId, JobAclEntry.JobPermissions permission) throws CatalogException;

    void checkCohortPermission(long cohortId, String userId, CohortAclEntry.CohortPermissions permission) throws CatalogException;

    void checkDatasetPermission(long datasetId, String userId, DatasetAclEntry.DatasetPermissions permission) throws CatalogException;

    void checkDiseasePanelPermission(long panelId, String userId, DiseasePanelAclEntry.DiseasePanelPermissions permission)
            throws CatalogException;

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

    //------------------------- Study ACL -----------------------------

    /**
     * Adds the list of members to the roleId specified.
     *
     * @param userId User id of the user ordering the action.
     * @param studyId Study id under which the members will be added to the role.
     * @param members List of member ids (users and/or groups).
     * @param permissions List of permissions to be added to the members. If a template is provided, the permissions present here will be
     *                    added to the list of permissions present in the template.
     * @param template Template to be used to get the default permissions from. Might be null.
     * @return a queryResult containing the complete studyAcl where the members have been added to.
     * @throws CatalogException when the userId does not have the proper permissions or the members or the roleId do not exist.
     */
    QueryResult<StudyAclEntry> createStudyAcls(String userId, long studyId, List<String> members, List<String> permissions,
                                               @Nullable String template) throws CatalogException;
    default QueryResult<StudyAclEntry> createStudyAcls(String userId, long studyId, String members, String permissions,
                                                       @Nullable String template) throws CatalogException {
        List<String> permissionList;
        if (permissions != null && !permissions.isEmpty()) {
            permissionList = Arrays.asList(permissions.split(","));
        } else {
            permissionList = Collections.emptyList();
        }

        List<String> memberList;
        if (members != null && !members.isEmpty()) {
            memberList = Arrays.asList(members.split(","));
        } else {
            memberList = Collections.emptyList();
        }

        return createStudyAcls(userId, studyId, memberList, permissionList, template);
    }

    /**
     * Return all the ACLs defined in the study.
     *
     * @param userId user id asking for the ACLs.
     * @param studyId study id.
     * @return a list of studyAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the study does not have proper permissions.
     */
    QueryResult<StudyAclEntry> getAllStudyAcls(String userId, long studyId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param studyId study id.
     * @param member member whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<StudyAclEntry> getStudyAcl(String userId, long studyId, String member) throws CatalogException;

    /**
     * Removes the ACLs defined for the member.
     *
     * @param userId user asking to remove the ACLs.
     * @param studyId study id.
     * @param member member whose permissions will be taken out.
     * @return the studyAcl prior to the deletion.
     * @throws CatalogException if the user asking to remove the ACLs does not have proper permissions or the member does not have any ACL
     * defined.
     */
    QueryResult<StudyAclEntry> removeStudyAcl(String userId, long studyId, String member) throws CatalogException;

    QueryResult<StudyAclEntry> updateStudyAcl(String userId, long studyId, String member, @Nullable String addPermissions,
                                              @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;

    //------------------------- End of study ACL ----------------------

    //------------------------- Sample ACL -----------------------------

    QueryResult<SampleAclEntry> createSampleAcls(String userId, long sampleId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<SampleAclEntry> createSampleAcls(String userId, long sampleId, String members, String permissions)
            throws CatalogException {

        List<String> permissionList;
        if (permissions != null && !permissions.isEmpty()) {
            permissionList = Arrays.asList(permissions.split(","));
        } else {
            permissionList = Collections.emptyList();
        }

        List<String> memberList;
        if (members != null && !members.isEmpty()) {
            memberList = Arrays.asList(members.split(","));
        } else {
            memberList = Collections.emptyList();
        }

        return createSampleAcls(userId, sampleId, memberList, permissionList);
    }

    /**
     * Return all the ACLs defined for the sample.
     *
     * @param userId user id asking for the ACLs.
     * @param sampleId sample id.
     * @return a list of sampleAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<SampleAclEntry> getAllSampleAcls(String userId, long sampleId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param sampleId sample id.
     * @param member member whose permissions will be retrieved.
     * @return the SampleAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<SampleAclEntry> getSampleAcl(String userId, long sampleId, String member) throws CatalogException;

    /**
     * Removes the ACLs defined for the member.
     *
     * @param userId user asking to remove the ACLs.
     * @param sampleId sample id.
     * @param member member whose permissions will be taken out.
     * @return the SampleAcl prior to the deletion.
     * @throws CatalogException if the user asking to remove the ACLs does not have proper permissions or the member does not have any ACL
     * defined.
     */
    QueryResult<SampleAclEntry> removeSampleAcl(String userId, long sampleId, String member) throws CatalogException;

    QueryResult<SampleAclEntry> updateSampleAcl(String userId, long sampleId, String member, @Nullable String addPermissions,
                                                @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException;


    //------------------------- End of sample ACL ----------------------


    //------------------------- File ACL -----------------------------

    QueryResult<FileAclEntry> createFileAcls(String userId, long fileId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<FileAclEntry> createFileAcls(String userId, long fileId, String members, String permissions)
            throws CatalogException {

        List<String> permissionList;
        if (permissions != null && !permissions.isEmpty()) {
            permissionList = Arrays.asList(permissions.split(","));
        } else {
            permissionList = Collections.emptyList();
        }

        List<String> memberList;
        if (members != null && !members.isEmpty()) {
            memberList = Arrays.asList(members.split(","));
        } else {
            memberList = Collections.emptyList();
        }

        return createFileAcls(userId, fileId, memberList, permissionList);
    }

    /**
     * Return all the ACLs defined for the file.
     *
     * @param userId user id asking for the ACLs.
     * @param fileId file id.
     * @return a list of FileAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<FileAclEntry> getAllFileAcls(String userId, long fileId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param fileId file id.
     * @param member member whose permissions will be retrieved.
     * @return the FileAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<FileAclEntry> getFileAcl(String userId, long fileId, String member) throws CatalogException;

    /**
     * Removes the ACLs defined for the member.
     *
     * @param userId user asking to remove the ACLs.
     * @param fileId file id.
     * @param member member whose permissions will be taken out.
     * @return the FileAcl prior to the deletion.
     * @throws CatalogException if the user asking to remove the ACLs does not have proper permissions or the member does not have any ACL
     * defined.
     */
    QueryResult<FileAclEntry> removeFileAcl(String userId, long fileId, String member) throws CatalogException;

    QueryResult<FileAclEntry> updateFileAcl(String userId, long fileId, String member, @Nullable String addPermissions,
                                            @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;


    //------------------------- End of file ACL ----------------------

    //------------------------- Individual ACL -----------------------------

    QueryResult<IndividualAclEntry> createIndividualAcls(String userId, long individualId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<IndividualAclEntry> createIndividualAcls(String userId, long individualId, String members, String permissions)
            throws CatalogException {

        List<String> permissionList;
        if (permissions != null && !permissions.isEmpty()) {
            permissionList = Arrays.asList(permissions.split(","));
        } else {
            permissionList = Collections.emptyList();
        }

        List<String> memberList;
        if (members != null && !members.isEmpty()) {
            memberList = Arrays.asList(members.split(","));
        } else {
            memberList = Collections.emptyList();
        }

        return createIndividualAcls(userId, individualId, memberList, permissionList);
    }

    /**
     * Return all the ACLs defined for the individual.
     *
     * @param userId user id asking for the ACLs.
     * @param individualId individual id.
     * @return a list of IndividualAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<IndividualAclEntry> getAllIndividualAcls(String userId, long individualId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param individualId individual id.
     * @param member member whose permissions will be retrieved.
     * @return the IndividualAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<IndividualAclEntry> getIndividualAcl(String userId, long individualId, String member) throws CatalogException;

    /**
     * Removes the ACLs defined for the member.
     *
     * @param userId user asking to remove the ACLs.
     * @param individualId individual id.
     * @param member member whose permissions will be taken out.
     * @return the IndividualAcl prior to the deletion.
     * @throws CatalogException if the user asking to remove the ACLs does not have proper permissions or the member does not have any ACL
     * defined.
     */
    QueryResult<IndividualAclEntry> removeIndividualAcl(String userId, long individualId, String member) throws CatalogException;

    QueryResult<IndividualAclEntry> updateIndividualAcl(String userId, long individualId, String member, @Nullable String addPermissions,
                                                        @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException;


    //------------------------- End of individual ACL ----------------------

    //------------------------- Cohort ACL -----------------------------

    QueryResult<CohortAclEntry> createCohortAcls(String userId, long cohortId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<CohortAclEntry> createCohortAcls(String userId, long cohortId, String members, String permissions)
            throws CatalogException {

        List<String> permissionList;
        if (permissions != null && !permissions.isEmpty()) {
            permissionList = Arrays.asList(permissions.split(","));
        } else {
            permissionList = Collections.emptyList();
        }

        List<String> memberList;
        if (members != null && !members.isEmpty()) {
            memberList = Arrays.asList(members.split(","));
        } else {
            memberList = Collections.emptyList();
        }

        return createCohortAcls(userId, cohortId, memberList, permissionList);
    }

    /**
     * Return all the ACLs defined for the cohort.
     *
     * @param userId user id asking for the ACLs.
     * @param cohortId cohort id.
     * @return a list of CohortAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<CohortAclEntry> getAllCohortAcls(String userId, long cohortId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param cohortId cohort id.
     * @param member member whose permissions will be retrieved.
     * @return the CohortAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<CohortAclEntry> getCohortAcl(String userId, long cohortId, String member) throws CatalogException;

    /**
     * Removes the ACLs defined for the member.
     *
     * @param userId user asking to remove the ACLs.
     * @param cohortId cohort id.
     * @param member member whose permissions will be taken out.
     * @return the CohortAcl prior to the deletion.
     * @throws CatalogException if the user asking to remove the ACLs does not have proper permissions or the member does not have any ACL
     * defined.
     */
    QueryResult<CohortAclEntry> removeCohortAcl(String userId, long cohortId, String member) throws CatalogException;

    QueryResult<CohortAclEntry> updateCohortAcl(String userId, long cohortId, String member, @Nullable String addPermissions,
                                                @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException;

    //------------------------- End of cohort ACL ----------------------

    //------------------------- Dataset ACL -----------------------------

    QueryResult<DatasetAclEntry> createDatasetAcls(String userId, long datasetId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<DatasetAclEntry> createDatasetAcls(String userId, long datasetId, String members, String permissions)
            throws CatalogException {

        List<String> permissionList;
        if (permissions != null && !permissions.isEmpty()) {
            permissionList = Arrays.asList(permissions.split(","));
        } else {
            permissionList = Collections.emptyList();
        }

        List<String> memberList;
        if (members != null && !members.isEmpty()) {
            memberList = Arrays.asList(members.split(","));
        } else {
            memberList = Collections.emptyList();
        }

        return createDatasetAcls(userId, datasetId, memberList, permissionList);
    }

    /**
     * Return all the ACLs defined for the dataset.
     *
     * @param userId user id asking for the ACLs.
     * @param datasetId dataset id.
     * @return a list of DatasetAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<DatasetAclEntry> getAllDatasetAcls(String userId, long datasetId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param datasetId dataset id.
     * @param member member whose permissions will be retrieved.
     * @return the DatasetAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<DatasetAclEntry> getDatasetAcl(String userId, long datasetId, String member) throws CatalogException;

    /**
     * Removes the ACLs defined for the member.
     *
     * @param userId user asking to remove the ACLs.
     * @param datasetId dataset id.
     * @param member member whose permissions will be taken out.
     * @return the DatasetAcl prior to the deletion.
     * @throws CatalogException if the user asking to remove the ACLs does not have proper permissions or the member does not have any ACL
     * defined.
     */
    QueryResult<DatasetAclEntry> removeDatasetAcl(String userId, long datasetId, String member) throws CatalogException;

    QueryResult<DatasetAclEntry> updateDatasetAcl(String userId, long datasetId, String member, @Nullable String addPermissions,
                                                  @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException;


    //------------------------- End of dataset ACL ----------------------

    //------------------------- Job ACL -----------------------------

    QueryResult<JobAclEntry> createJobAcls(String userId, long jobId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<JobAclEntry> createJobAcls(String userId, long jobId, String members, String permissions)
            throws CatalogException {

        List<String> permissionList;
        if (permissions != null && !permissions.isEmpty()) {
            permissionList = Arrays.asList(permissions.split(","));
        } else {
            permissionList = Collections.emptyList();
        }

        List<String> memberList;
        if (members != null && !members.isEmpty()) {
            memberList = Arrays.asList(members.split(","));
        } else {
            memberList = Collections.emptyList();
        }

        return createJobAcls(userId, jobId, memberList, permissionList);
    }

    /**
     * Return all the ACLs defined for the job.
     *
     * @param userId user id asking for the ACLs.
     * @param jobId job id.
     * @return a list of JobAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<JobAclEntry> getAllJobAcls(String userId, long jobId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param jobId job id.
     * @param member member whose permissions will be retrieved.
     * @return the JobAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<JobAclEntry> getJobAcl(String userId, long jobId, String member) throws CatalogException;

    /**
     * Removes the ACLs defined for the member.
     *
     * @param userId user asking to remove the ACLs.
     * @param jobId job id.
     * @param member member whose permissions will be taken out.
     * @return the JobAcl prior to the deletion.
     * @throws CatalogException if the user asking to remove the ACLs does not have proper permissions or the member does not have any ACL
     * defined.
     */
    QueryResult<JobAclEntry> removeJobAcl(String userId, long jobId, String member) throws CatalogException;

    QueryResult<JobAclEntry> updateJobAcl(String userId, long jobId, String member, @Nullable String addPermissions,
                                          @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;


    //------------------------- End of job ACL ----------------------

    //------------------------- Panel ACL -----------------------------

    QueryResult<DiseasePanelAclEntry> createPanelAcls(String userId, long panelId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<DiseasePanelAclEntry> createPanelAcls(String userId, long panelId, String members, String permissions)
            throws CatalogException {

        List<String> permissionList;
        if (permissions != null && !permissions.isEmpty()) {
            permissionList = Arrays.asList(permissions.split(","));
        } else {
            permissionList = Collections.emptyList();
        }

        List<String> memberList;
        if (members != null && !members.isEmpty()) {
            memberList = Arrays.asList(members.split(","));
        } else {
            memberList = Collections.emptyList();
        }

        return createPanelAcls(userId, panelId, memberList, permissionList);
    }

    /**
     * Return all the ACLs defined for the panel.
     *
     * @param userId user id asking for the ACLs.
     * @param panelId panel id.
     * @return a list of DiseasePanelAcl.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<DiseasePanelAclEntry> getAllPanelAcls(String userId, long panelId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param panelId panel id.
     * @param member member whose permissions will be retrieved.
     * @return the DiseasePanelAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<DiseasePanelAclEntry> getPanelAcl(String userId, long panelId, String member) throws CatalogException;

    /**
     * Removes the ACLs defined for the member.
     *
     * @param userId user asking to remove the ACLs.
     * @param panelId panel id.
     * @param member member whose permissions will be taken out.
     * @return the DiseasePanelAcl prior to the deletion.
     * @throws CatalogException if the user asking to remove the ACLs does not have proper permissions or the member does not have any ACL
     * defined.
     */
    QueryResult<DiseasePanelAclEntry> removePanelAcl(String userId, long panelId, String member) throws CatalogException;

    QueryResult<DiseasePanelAclEntry> updatePanelAcl(String userId, long panelId, String member, @Nullable String addPermissions,
                                                     @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException;


    //------------------------- End of panel ACL ----------------------
    /**
     * Checks if the member belongs to one role or not.
     *
     * @param studyId study id.
     * @param member User or group id.
     * @return true if the member belongs to one role. False otherwise.
     * @throws CatalogException CatalogException.
     */
    boolean memberHasPermissionsInStudy(long studyId, String member) throws CatalogException;
}
