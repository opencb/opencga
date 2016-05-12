package org.opencb.opencga.catalog.authorization;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.*;

import java.util.List;

/**
 * Created by pfurio on 12/05/16.
 */
public class CatalogAuthorizationManager implements AuthorizationManager {

    @Override
    public void checkProjectPermission(long projectId, String userId, StudyAcl.StudyPermissions permission) throws CatalogException {

    }

    @Override
    public void checkStudyPermission(long studyId, String userId, StudyAcl.StudyPermissions permission) throws CatalogException {

    }

    @Override
    public void checkStudyPermission(long studyId, String userId, StudyAcl.StudyPermissions permission, String message)
            throws CatalogException {

    }

    @Override
    public void checkFilePermission(long fileId, String userId, FileAcl.FilePermissions permission) throws CatalogException {

    }

    @Override
    public void checkSamplePermission(long sampleId, String userId, SampleAcl.SamplePermissions permission) throws CatalogException {

    }

    @Override
    public void checkIndividualPermission(long individualId, String userId, IndividualAcl.IndividualPermissions permission)
            throws CatalogException {

    }

    @Override
    public void checkJobPermission(long jobId, String userId, JobAcl.JobPermissions permission) throws CatalogException {

    }

    @Override
    public void checkCohortPermission(long cohortId, String userId, CohortAcl.CohortPermissions permission) throws CatalogException {

    }

    @Override
    public void checkDatasetPermission(long datasetId, String userId, StudyAcl.StudyPermissions permission) throws CatalogException {

    }

    @Override
    public QueryResult<FileAcl> setFilePermissions(String userId, String fileIds, String userIds, List<FileAcl.FilePermissions> permissions)
            throws CatalogException {
        return null;
    }

    @Override
    public void unsetFilePermissions(String userId, String fileIds, String userIds) throws CatalogException {

    }

    @Override
    public QueryResult<SampleAcl> setSamplePermissions(String userId, String sampleIds, String userIds,
                                                       List<SampleAcl.SamplePermissions> permissions) throws CatalogException {
        return null;
    }

    @Override
    public void unsetSamplePermissions(String userId, String sampleIds, String userIds) throws CatalogException {

    }

    @Override
    public QueryResult<CohortAcl> setCohortPermissions(String userId, String cohortIds, String userIds,
                                                       List<CohortAcl.CohortPermissions> permissions) throws CatalogException {
        return null;
    }

    @Override
    public void unsetCohortPermissions(String userId, String cohortIds, String userIds) throws CatalogException {

    }

    @Override
    public QueryResult<IndividualAcl> setIndividualPermissions(String userId, String individualIds, String userIds,
                                                               List<IndividualAcl.IndividualPermissions> permissions)
            throws CatalogException {
        return null;
    }

    @Override
    public void unsetIndividualPermissions(String userId, String individualIds, String userIds) throws CatalogException {

    }

    @Override
    public QueryResult<JobAcl> setJobPermissions(String userId, String jobIds, String userIds, List<JobAcl.JobPermissions> permissions)
            throws CatalogException {
        return null;
    }

    @Override
    public void unsetJobPermissions(String userId, String jobIds, String userIds) throws CatalogException {

    }

    @Override
    public void filterProjects(String userId, List<Project> projects) throws CatalogException {

    }

    @Override
    public void filterStudies(String userId, List<Study> studies) throws CatalogException {

    }

    @Override
    public void filterFiles(String userId, long studyId, List<File> files) throws CatalogException {

    }

    @Override
    public void filterSamples(String userId, long studyId, List<Sample> samples) throws CatalogException {

    }

    @Override
    public void filterIndividuals(String userId, long studyId, List<Individual> individuals) throws CatalogException {

    }

    @Override
    public void filterCohorts(String userId, long studyId, List<Cohort> cohorts) throws CatalogException {

    }

    @Override
    public void filterJobs(String userId, long studyId, List<Job> jobs) throws CatalogException {

    }

    @Override
    public String getGroupBelonging(long studyId, String userId) throws CatalogException {
        return null;
    }

    @Override
    public StudyAcl getStudyAclBelonging(long studyId, String userId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Group> addMember(String userId, long studyId, String groupId, String newUserId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Group> removeMember(String userId, long studyId, String groupId, String oldUserId) throws CatalogException {
        return null;
    }
}
