/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.auth.authorization;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.*;

import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.List;

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
    String ROLE_VIEW_ONLY = "view_only";
    String ROLE_LOCKED = "locked";

    String OTHER_USERS_ID = "*";

    static EnumSet<StudyAclEntry.StudyPermissions> getAdminAcls() {
        return EnumSet.allOf(StudyAclEntry.StudyPermissions.class);
    }

    static EnumSet<StudyAclEntry.StudyPermissions> getAnalystAcls() {
        return EnumSet.of(StudyAclEntry.StudyPermissions.VIEW_STUDY, StudyAclEntry.StudyPermissions.UPDATE_STUDY,
                StudyAclEntry.StudyPermissions.WRITE_VARIABLE_SET, StudyAclEntry.StudyPermissions.VIEW_VARIABLE_SET,
                StudyAclEntry.StudyPermissions.WRITE_FILES, StudyAclEntry.StudyPermissions.VIEW_FILE_HEADERS,
                StudyAclEntry.StudyPermissions.VIEW_FILE_CONTENTS, StudyAclEntry.StudyPermissions.VIEW_FILES,
                StudyAclEntry.StudyPermissions.DOWNLOAD_FILES, StudyAclEntry.StudyPermissions.UPLOAD_FILES,
                StudyAclEntry.StudyPermissions.WRITE_JOBS, StudyAclEntry.StudyPermissions.VIEW_JOBS,
                StudyAclEntry.StudyPermissions.WRITE_SAMPLES, StudyAclEntry.StudyPermissions.VIEW_SAMPLES,
                StudyAclEntry.StudyPermissions.WRITE_SAMPLE_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS,
                StudyAclEntry.StudyPermissions.WRITE_INDIVIDUAL_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.WRITE_COHORTS, StudyAclEntry.StudyPermissions.VIEW_COHORTS,
                StudyAclEntry.StudyPermissions.WRITE_COHORT_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.WRITE_DATASETS, StudyAclEntry.StudyPermissions.VIEW_DATASETS,
                StudyAclEntry.StudyPermissions.WRITE_PANELS, StudyAclEntry.StudyPermissions.VIEW_PANELS);
    }

    static EnumSet<StudyAclEntry.StudyPermissions> getViewOnlyAcls() {
        return EnumSet.of(StudyAclEntry.StudyPermissions.VIEW_STUDY, StudyAclEntry.StudyPermissions.VIEW_VARIABLE_SET,
                StudyAclEntry.StudyPermissions.VIEW_FILE_HEADERS, StudyAclEntry.StudyPermissions.VIEW_FILE_CONTENTS,
                StudyAclEntry.StudyPermissions.VIEW_FILES, StudyAclEntry.StudyPermissions.DOWNLOAD_FILES,
                StudyAclEntry.StudyPermissions.VIEW_JOBS, StudyAclEntry.StudyPermissions.VIEW_SAMPLES,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_COHORTS,
                StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_DATASETS,
                StudyAclEntry.StudyPermissions.VIEW_PANELS);
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
     * @param userId   UserId.
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
     * @param userId      UserId
     * @param studyId     StudyId
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
     * @param jobs    Jobs.
     * @throws CatalogException CatalogException.
     */
    void filterJobs(String userId, long studyId, List<Job> jobs) throws CatalogException;

    /**
     * Removes from the list the datasets that the user can not read.
     *
     * @param userId   UserId.
     * @param studyId  StudyId.
     * @param datasets datasets.
     * @throws CatalogException CatalogException.
     */
    void filterDatasets(String userId, long studyId, List<Dataset> datasets) throws CatalogException;

    //------------------------- Study ACL -----------------------------

    /**
     * Return all the ACLs defined in the study.
     *
     * @param userId  user id asking for the ACLs.
     * @param studyId study id.
     * @return a list of studyAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the study does not have proper permissions.
     */
    QueryResult<StudyAclEntry> getAllStudyAcls(String userId, long studyId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId  user asking for the ACL.
     * @param studyId study id.
     * @param member  member whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<StudyAclEntry> getStudyAcl(String userId, long studyId, String member) throws CatalogException;

    //------------------------- End of study ACL ----------------------

    //------------------------- Sample ACL -----------------------------

    /**
     * Return all the ACLs defined for the sample.
     *
     * @param userId   user id asking for the ACLs.
     * @param sampleId sample id.
     * @return a list of sampleAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<SampleAclEntry> getAllSampleAcls(String userId, long sampleId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId   user asking for the ACL.
     * @param sampleId sample id.
     * @param member   member whose permissions will be retrieved.
     * @return the SampleAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<SampleAclEntry> getSampleAcl(String userId, long sampleId, String member) throws CatalogException;

    //------------------------- End of sample ACL ----------------------


    //------------------------- File ACL -----------------------------

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

    //------------------------- End of file ACL ----------------------

    //------------------------- Individual ACL -----------------------------

    /**
     * Return all the ACLs defined for the individual.
     *
     * @param userId       user id asking for the ACLs.
     * @param individualId individual id.
     * @return a list of IndividualAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<IndividualAclEntry> getAllIndividualAcls(String userId, long individualId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId       user asking for the ACL.
     * @param individualId individual id.
     * @param member       member whose permissions will be retrieved.
     * @return the IndividualAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<IndividualAclEntry> getIndividualAcl(String userId, long individualId, String member) throws CatalogException;

    //------------------------- End of individual ACL ----------------------

    //------------------------- Cohort ACL -----------------------------

    /**
     * Return all the ACLs defined for the cohort.
     *
     * @param userId   user id asking for the ACLs.
     * @param cohortId cohort id.
     * @return a list of CohortAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<CohortAclEntry> getAllCohortAcls(String userId, long cohortId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId   user asking for the ACL.
     * @param cohortId cohort id.
     * @param member   member whose permissions will be retrieved.
     * @return the CohortAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<CohortAclEntry> getCohortAcl(String userId, long cohortId, String member) throws CatalogException;

    //------------------------- End of cohort ACL ----------------------

    //------------------------- Dataset ACL -----------------------------

    /**
     * Return all the ACLs defined for the dataset.
     *
     * @param userId    user id asking for the ACLs.
     * @param datasetId dataset id.
     * @return a list of DatasetAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<DatasetAclEntry> getAllDatasetAcls(String userId, long datasetId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId    user asking for the ACL.
     * @param datasetId dataset id.
     * @param member    member whose permissions will be retrieved.
     * @return the DatasetAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<DatasetAclEntry> getDatasetAcl(String userId, long datasetId, String member) throws CatalogException;


    //------------------------- End of dataset ACL ----------------------

    //------------------------- Job ACL -----------------------------

    /**
     * Return all the ACLs defined for the job.
     *
     * @param userId user id asking for the ACLs.
     * @param jobId  job id.
     * @return a list of JobAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    QueryResult<JobAclEntry> getAllJobAcls(String userId, long jobId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId user asking for the ACL.
     * @param jobId  job id.
     * @param member member whose permissions will be retrieved.
     * @return the JobAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    QueryResult<JobAclEntry> getJobAcl(String userId, long jobId, String member) throws CatalogException;

    //------------------------- End of job ACL ----------------------

    List<QueryResult<StudyAclEntry>> setStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException;

    List<QueryResult<StudyAclEntry>> addStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException;

    List<QueryResult<StudyAclEntry>> removeStudyAcls(List<Long> studyIds, List<String> members, @Nullable List<String> permissions)
            throws CatalogException;

    <E extends AbstractAclEntry> QueryResult<E> getAcl(long id, List<String> members, String entity) throws CatalogException;

    <E extends AbstractAclEntry> List<QueryResult<E>> getAcls(List<Long> ids, List<String> members, String entity) throws CatalogException;

    <E extends AbstractAclEntry> List<QueryResult<E>> setAcls(List<Long> ids, List<String> members,
                                                              List<String> permissions, String entity) throws CatalogException;

    <E extends AbstractAclEntry> List<QueryResult<E>> addAcls(List<Long> ids, List<String> members,
                                                              List<String> permissions, String entity) throws CatalogException;

    <E extends AbstractAclEntry> List<QueryResult<E>> removeAcls(List<Long> ids, List<String> members, @Nullable List<String> permissions,
                                                                 String entity) throws CatalogException;

    <E extends Enum<E>> void checkValidPermission(List<String> permissions, Class<E> enumClass) throws CatalogException;

    /**
     * Checks if the member belongs to one role or not.
     *
     * @param studyId study id.
     * @param member  User or group id.
     * @return true if the member belongs to one role. False otherwise.
     * @throws CatalogException CatalogException.
     */
    boolean memberHasPermissionsInStudy(long studyId, String member) throws CatalogException;

//    /**
//     * Checks whether any of the members already have any permission set for the particular document.
//     *
//     * @param studyId   study id where the main id belongs to.
//     * @param id        id of the document that is going to be checked (file id, sample id, cohort id...)
//     * @param members   List of members (users or groups) that will be checked.
//     * @param dbAdaptor Mongo db adaptor to make the mongo query.
//     * @return a boolean indicating whether any of the members already have permissions.
//     * @throws CatalogException CatalogException.
//     */
//    boolean anyMemberHasPermissions(long studyId, long id, List<String> members, org.opencb.opencga.catalog.db.api.AclDBAdaptor dbAdaptor)
//            throws CatalogException;

}
