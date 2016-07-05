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

    static EnumSet<StudyAcl.StudyPermissions> getAdminAcls() {
        return EnumSet.allOf(StudyAcl.StudyPermissions.class);
    }

    static EnumSet<StudyAcl.StudyPermissions> getAnalystAcls() {
        return EnumSet.of(StudyAcl.StudyPermissions.VIEW_STUDY,
                StudyAcl.StudyPermissions.UPDATE_STUDY, StudyAcl.StudyPermissions.CREATE_VARIABLE_SET,
                StudyAcl.StudyPermissions.VIEW_VARIABLE_SET, StudyAcl.StudyPermissions.UPDATE_VARIABLE_SET,
                StudyAcl.StudyPermissions.CREATE_FILES, StudyAcl.StudyPermissions.VIEW_FILE_HEADERS,
                StudyAcl.StudyPermissions.VIEW_FILE_CONTENTS, StudyAcl.StudyPermissions.VIEW_FILES,
                StudyAcl.StudyPermissions.UPDATE_FILES, StudyAcl.StudyPermissions.DOWNLOAD_FILES,
                StudyAcl.StudyPermissions.CREATE_JOBS, StudyAcl.StudyPermissions.VIEW_JOBS, StudyAcl.StudyPermissions.UPDATE_JOBS,
                StudyAcl.StudyPermissions.CREATE_SAMPLES, StudyAcl.StudyPermissions.VIEW_SAMPLES, StudyAcl.StudyPermissions.UPDATE_SAMPLES,
                StudyAcl.StudyPermissions.CREATE_SAMPLE_ANNOTATIONS, StudyAcl.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS,
                StudyAcl.StudyPermissions.UPDATE_SAMPLE_ANNOTATIONS, StudyAcl.StudyPermissions.CREATE_INDIVIDUALS,
                StudyAcl.StudyPermissions.VIEW_INDIVIDUALS, StudyAcl.StudyPermissions.UPDATE_INDIVIDUALS,
                StudyAcl.StudyPermissions.CREATE_INDIVIDUAL_ANNOTATIONS, StudyAcl.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS,
                StudyAcl.StudyPermissions.UPDATE_INDIVIDUAL_ANNOTATIONS, StudyAcl.StudyPermissions.CREATE_COHORTS,
                StudyAcl.StudyPermissions.VIEW_COHORTS, StudyAcl.StudyPermissions.UPDATE_COHORTS,
                StudyAcl.StudyPermissions.CREATE_COHORT_ANNOTATIONS, StudyAcl.StudyPermissions.VIEW_COHORT_ANNOTATIONS,
                StudyAcl.StudyPermissions.UPDATE_COHORT_ANNOTATIONS, StudyAcl.StudyPermissions.CREATE_DATASETS,
                StudyAcl.StudyPermissions.VIEW_DATASETS, StudyAcl.StudyPermissions.UPDATE_DATASETS,
                StudyAcl.StudyPermissions.CREATE_PANELS, StudyAcl.StudyPermissions.VIEW_PANELS, StudyAcl.StudyPermissions.UPDATE_PANELS);
    }

    static EnumSet<StudyAcl.StudyPermissions> getLockedAcls() {
        return EnumSet.noneOf(StudyAcl.StudyPermissions.class);
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

    void checkDiseasePanelPermission(long panelId, String userId, DiseasePanelAcl.DiseasePanelPermissions permission)
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
    QueryResult<StudyAcl> createStudyAcls(String userId, long studyId, List<String> members, List<String> permissions,
                                          @Nullable String template) throws CatalogException;
    default QueryResult<StudyAcl> createStudyAcls(String userId, long studyId, String members, String permissions,
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
    QueryResult<StudyAcl> getAllStudyAcls(String userId, long studyId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param studyId study id.
     * @param member member whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<StudyAcl> getStudyAcl(String userId, long studyId, String member) throws CatalogException;

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
    QueryResult<StudyAcl> removeStudyAcl(String userId, long studyId, String member) throws CatalogException;

    QueryResult<StudyAcl> updateStudyAcl(String userId, long studyId, String member, @Nullable String addPermissions,
                                         @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;

    //------------------------- End of study ACL ----------------------

    //------------------------- Sample ACL -----------------------------

    QueryResult<SampleAcl> createSampleAcls(String userId, long sampleId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<SampleAcl> createSampleAcls(String userId, long sampleId, String members, String permissions)
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
    QueryResult<SampleAcl> getAllSampleAcls(String userId, long sampleId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param sampleId sample id.
     * @param member member whose permissions will be retrieved.
     * @return the SampleAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<SampleAcl> getSampleAcl(String userId, long sampleId, String member) throws CatalogException;

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
    QueryResult<SampleAcl> removeSampleAcl(String userId, long sampleId, String member) throws CatalogException;

    QueryResult<SampleAcl> updateSampleAcl(String userId, long sampleId, String member, @Nullable String addPermissions,
                                         @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;


    //------------------------- End of sample ACL ----------------------


    //------------------------- File ACL -----------------------------

    QueryResult<FileAcl> createFileAcls(String userId, long fileId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<FileAcl> createFileAcls(String userId, long fileId, String members, String permissions)
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
    QueryResult<FileAcl> getAllFileAcls(String userId, long fileId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param fileId file id.
     * @param member member whose permissions will be retrieved.
     * @return the FileAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<FileAcl> getFileAcl(String userId, long fileId, String member) throws CatalogException;

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
    QueryResult<FileAcl> removeFileAcl(String userId, long fileId, String member) throws CatalogException;

    QueryResult<FileAcl> updateFileAcl(String userId, long fileId, String member, @Nullable String addPermissions,
                                           @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;


    //------------------------- End of file ACL ----------------------

    //------------------------- Individual ACL -----------------------------

    QueryResult<IndividualAcl> createIndividualAcls(String userId, long individualId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<IndividualAcl> createIndividualAcls(String userId, long individualId, String members, String permissions)
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
    QueryResult<IndividualAcl> getAllIndividualAcls(String userId, long individualId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param individualId individual id.
     * @param member member whose permissions will be retrieved.
     * @return the IndividualAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<IndividualAcl> getIndividualAcl(String userId, long individualId, String member) throws CatalogException;

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
    QueryResult<IndividualAcl> removeIndividualAcl(String userId, long individualId, String member) throws CatalogException;

    QueryResult<IndividualAcl> updateIndividualAcl(String userId, long individualId, String member, @Nullable String addPermissions,
                                       @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;


    //------------------------- End of individual ACL ----------------------

    //------------------------- Cohort ACL -----------------------------

    QueryResult<CohortAcl> createCohortAcls(String userId, long cohortId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<CohortAcl> createCohortAcls(String userId, long cohortId, String members, String permissions)
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
    QueryResult<CohortAcl> getAllCohortAcls(String userId, long cohortId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param cohortId cohort id.
     * @param member member whose permissions will be retrieved.
     * @return the CohortAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<CohortAcl> getCohortAcl(String userId, long cohortId, String member) throws CatalogException;

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
    QueryResult<CohortAcl> removeCohortAcl(String userId, long cohortId, String member) throws CatalogException;

    QueryResult<CohortAcl> updateCohortAcl(String userId, long cohortId, String member, @Nullable String addPermissions,
                                           @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;

    //------------------------- End of cohort ACL ----------------------

    //------------------------- Dataset ACL -----------------------------

    QueryResult<DatasetAcl> createDatasetAcls(String userId, long datasetId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<DatasetAcl> createDatasetAcls(String userId, long datasetId, String members, String permissions)
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
    QueryResult<DatasetAcl> getAllDatasetAcls(String userId, long datasetId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param datasetId dataset id.
     * @param member member whose permissions will be retrieved.
     * @return the DatasetAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<DatasetAcl> getDatasetAcl(String userId, long datasetId, String member) throws CatalogException;

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
    QueryResult<DatasetAcl> removeDatasetAcl(String userId, long datasetId, String member) throws CatalogException;

    QueryResult<DatasetAcl> updateDatasetAcl(String userId, long datasetId, String member, @Nullable String addPermissions,
                                             @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;


    //------------------------- End of dataset ACL ----------------------

    //------------------------- Job ACL -----------------------------

    QueryResult<JobAcl> createJobAcls(String userId, long jobId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<JobAcl> createJobAcls(String userId, long jobId, String members, String permissions)
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
    QueryResult<JobAcl> getAllJobAcls(String userId, long jobId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param jobId job id.
     * @param member member whose permissions will be retrieved.
     * @return the JobAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<JobAcl> getJobAcl(String userId, long jobId, String member) throws CatalogException;

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
    QueryResult<JobAcl> removeJobAcl(String userId, long jobId, String member) throws CatalogException;

    QueryResult<JobAcl> updateJobAcl(String userId, long jobId, String member, @Nullable String addPermissions,
                                     @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;


    //------------------------- End of job ACL ----------------------

    //------------------------- Panel ACL -----------------------------

    QueryResult<DiseasePanelAcl> createPanelAcls(String userId, long panelId, List<String> members, List<String> permissions)
            throws CatalogException;

    default QueryResult<DiseasePanelAcl> createPanelAcls(String userId, long panelId, String members, String permissions)
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
    QueryResult<DiseasePanelAcl> getAllPanelAcls(String userId, long panelId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param panelId panel id.
     * @param member member whose permissions will be retrieved.
     * @return the DiseasePanelAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<DiseasePanelAcl> getPanelAcl(String userId, long panelId, String member) throws CatalogException;

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
    QueryResult<DiseasePanelAcl> removePanelAcl(String userId, long panelId, String member) throws CatalogException;

    QueryResult<DiseasePanelAcl> updatePanelAcl(String userId, long panelId, String member, @Nullable String addPermissions,
                                         @Nullable String removePermissions, @Nullable String setPermissions) throws CatalogException;


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
