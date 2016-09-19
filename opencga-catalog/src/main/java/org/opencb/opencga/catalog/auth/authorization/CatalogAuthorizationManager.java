package org.opencb.opencga.catalog.auth.authorization;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.api.AclDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.utils.CatalogMemberValidator.checkMembers;

/**
 * Created by pfurio on 12/05/16.
 */
public class CatalogAuthorizationManager implements AuthorizationManager {

    private static final QueryOptions FILE_INCLUDE_QUERY_OPTIONS = new QueryOptions(QueryOptions.INCLUDE,
            Arrays.asList(FILTER_ROUTE_FILES + FileDBAdaptor.QueryParams.ID.key(),
                    FILTER_ROUTE_FILES + FileDBAdaptor.QueryParams.PATH.key(),
                    FILTER_ROUTE_FILES + FileDBAdaptor.QueryParams.ACL.key()
            ));

    private final DBAdaptorFactory dbAdaptorFactory;
    private final UserDBAdaptor userDBAdaptor;
    private final ProjectDBAdaptor projectDBAdaptor;
    private final StudyDBAdaptor studyDBAdaptor;
    private final FileDBAdaptor fileDBAdaptor;
    private final JobDBAdaptor jobDBAdaptor;
    private final SampleDBAdaptor sampleDBAdaptor;
    private final IndividualDBAdaptor individualDBAdaptor;
    private final CohortDBAdaptor cohortDBAdaptor;
    private final DatasetDBAdaptor datasetDBAdaptor;
    private final PanelDBAdaptor panelDBAdaptor;
    private final MetaDBAdaptor metaDBAdaptor;
    private final AuditManager auditManager;

    public CatalogAuthorizationManager(DBAdaptorFactory catalogDBAdaptorFactory, AuditManager auditManager) {
        this.auditManager = auditManager;
        this.dbAdaptorFactory = catalogDBAdaptorFactory;
        userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        projectDBAdaptor = catalogDBAdaptorFactory.getCatalogProjectDbAdaptor();
        studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
        cohortDBAdaptor = catalogDBAdaptorFactory.getCatalogCohortDBAdaptor();
        datasetDBAdaptor = catalogDBAdaptorFactory.getCatalogDatasetDBAdaptor();
        panelDBAdaptor = catalogDBAdaptorFactory.getCatalogPanelDBAdaptor();
        metaDBAdaptor = catalogDBAdaptorFactory.getCatalogMetaDBAdaptor();
    }

    @Override
    public void checkProjectPermission(long projectId, String userId, StudyAclEntry.StudyPermissions permission) throws CatalogException {
        if (projectDBAdaptor.getOwnerId(projectId).equals(userId)) {
            return;
        }

        if (permission.equals(StudyAclEntry.StudyPermissions.VIEW_STUDY)) {
            final Query query = new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId);
            final QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                    FILTER_ROUTE_STUDIES + StudyDBAdaptor.QueryParams.ID.key());
            for (Study study : studyDBAdaptor.get(query, queryOptions).getResult()) {
                try {
                    checkStudyPermission(study.getId(), userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);
                    return; //Return if can read some study
                } catch (CatalogException e) {
                    e.printStackTrace();
                }
            }
        }

        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Project", projectId, null);
    }

    @Override
    public void checkStudyPermission(long studyId, String userId, StudyAclEntry.StudyPermissions permission) throws CatalogException {
        checkStudyPermission(studyId, userId, permission, permission.toString());
    }

    @Override
    public void checkStudyPermission(long studyId, String userId, StudyAclEntry.StudyPermissions permission, String message)
            throws CatalogException {
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        StudyAclEntry studyAcl = null;
        if (userId.equals("admin")) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList("admin"));
            if (studyAclQueryResult.getNumResults() == 1) {
                studyAcl = studyAclQueryResult.first();
            }
        } else {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            String groupId = group.getNumResults() == 1 ? group.first().getName() : null;
            studyAcl = getStudyAclBelonging(studyId, userId, groupId);
        }
        if (studyAcl == null) {
            throw CatalogAuthorizationException.deny(userId, message, "Study", studyId, null);
        }

        if (studyAcl.getPermissions().contains(permission)) {
            return;
        }

        throw CatalogAuthorizationException.deny(userId, message, "Study", studyId, null);
    }

    @Override
    public void checkFilePermission(long fileId, String userId, FileAclEntry.FilePermissions permission) throws CatalogException {
        checkFilePermission(fileId, userId, permission, null);
    }

    private void checkFilePermission(long fileId, String userId, FileAclEntry.FilePermissions permission,
                                     StudyAuthenticationContext studyAuthenticationContext) throws CatalogException {
        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        FileAclEntry fileAcl = null;

        if (userId.equals("admin")) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList("admin"));
            if (studyAclQueryResult.getNumResults() == 1) {
                fileAcl = transformStudyAclToFileAcl(studyAclQueryResult.first());
            }
        } else {
            if (studyAuthenticationContext == null) {
                studyAuthenticationContext = new StudyAuthenticationContext(studyId);
            }
            fileAcl = resolveFilePermissions(fileId, userId, studyId, studyAuthenticationContext);
        }


        if (!fileAcl.getPermissions().contains(permission)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "File", fileId, null);
        }
    }

    /**
     * Obtains the file permissions of the userId.
     *
     * @param fileId file id.
     * @param userId user id.
     * @param studyId study id.
     * @param studyAuthenticationContext Mini cache containing the map of path -> user -> permissions.
     * @return fileAcl.
     */
    private FileAclEntry resolveFilePermissions(long fileId, String userId, long studyId,
                                                StudyAuthenticationContext studyAuthenticationContext) throws CatalogException {
        return resolveFilePermissions(fileDBAdaptor.get(fileId, FILE_INCLUDE_QUERY_OPTIONS).first(), userId, studyId,
                studyAuthenticationContext);
    }

    /**
     * Obtains the file permissions of the userId.
     *
     * @param file
     * @param userId
     * @param studyId
     * @param studyAuthenticationContext
     * @return
     * @throws CatalogException
     */
    private FileAclEntry resolveFilePermissions(File file, String userId, long studyId,
                                                StudyAuthenticationContext studyAuthenticationContext) throws CatalogException {
        QueryResult<Group> group = getGroupBelonging(studyId, userId);
        String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

        // We obtain all the paths from our current position to the root folder
        List<String> paths = FileManager.getParentPaths(file.getPath());
        // We obtain a map with the permissions
        Map<String, Map<String, FileAclEntry>> pathAclMap = getFileAcls(studyAuthenticationContext, userId, studyId, groupId, paths);

        FileAclEntry fileAcl = null;
        for (int i = paths.size() - 1; i >= 0; i--) {
            String path = paths.get(i);
            if (pathAclMap.containsKey(path)) {
                Map<String, FileAclEntry> aclMap = pathAclMap.get(path);
                if (aclMap.get(userId) != null) {
                    fileAcl = aclMap.get(userId);
                } else if (aclMap.get(groupId) != null) {
                    fileAcl = aclMap.get(groupId);
                } else if (aclMap.get(OTHER_USERS_ID) != null) {
                    fileAcl = aclMap.get(OTHER_USERS_ID);
                }
                if (fileAcl != null) {
                    break;
                }
            }
        }

        if (fileAcl == null) {
            StudyAclEntry studyAcl = getStudyAclBelonging(studyId, userId, groupId);
            fileAcl = transformStudyAclToFileAcl(studyAcl);
        }

        return fileAcl;
    }

    @Override
    public void checkSamplePermission(long sampleId, String userId, SampleAclEntry.SamplePermissions permission) throws CatalogException {
        long studyId = sampleDBAdaptor.getStudyId(sampleId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        SampleAclEntry sampleAcl = null;
        if (userId.equals("admin")) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList("admin"));
            if (studyAclQueryResult.getNumResults() == 1) {
                sampleAcl = transformStudyAclToSampleAcl(studyAclQueryResult.first());
            }
        } else {
            sampleAcl = resolveSamplePermissions(studyId, sampleId, userId);
        }

        if (!sampleAcl.getPermissions().contains(permission)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "Sample", sampleId, null);
        }
    }

    /**
     * Resolves the permissions between a sample and a user.
     * Returns the most specific matching ACL following the next sequence:
     * user > group > others > study
     *
     * @param studyId Study id.
     * @param sample Sample.
     * @param userId User id.
     * @return
     * @throws CatalogException
     */
    private SampleAclEntry resolveSamplePermissions(long studyId, Sample sample, String userId) throws CatalogException {
        if (sample.getAcl() == null) {
            return resolveSamplePermissions(studyId, sample.getId(), userId);
        } else {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

            Map<String, SampleAclEntry> userAclMap = new HashMap<>();
            for (SampleAclEntry sampleAcl : sample.getAcl()) {
                userAclMap.put(sampleAcl.getMember(), sampleAcl);
            }
            return resolveSamplePermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private SampleAclEntry resolveSamplePermissions(long studyId, long sampleId, String userId) throws CatalogException {
        QueryResult<Group> group = getGroupBelonging(studyId, userId);
        String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID);
        List<SampleAclEntry> sampleAclList = sampleDBAdaptor.getAcl(sampleId, userIds).getResult();

        Map<String, SampleAclEntry> userAclMap = new HashMap<>();
        for (SampleAclEntry sampleAcl : sampleAclList) {
            userAclMap.put(sampleAcl.getMember(), sampleAcl);
        }

        return resolveSamplePermissions(studyId, userId, groupId, userAclMap);
    }

    private SampleAclEntry resolveSamplePermissions(long studyId, String userId, String groupId, Map<String, SampleAclEntry> userAclMap)
            throws CatalogException {
        if (userAclMap.containsKey(userId)) {
            return userAclMap.get(userId);
        } else if (groupId != null && userAclMap.containsKey(groupId)) {
            return userAclMap.get(groupId);
        } else if (userAclMap.containsKey(OTHER_USERS_ID)) {
            return userAclMap.get(OTHER_USERS_ID);
        } else {
            return transformStudyAclToSampleAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void checkIndividualPermission(long individualId, String userId, IndividualAclEntry.IndividualPermissions permission)
            throws CatalogException {
        long studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        IndividualAclEntry individualAcl = null;
        if (userId.equals("admin")) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList("admin"));
            if (studyAclQueryResult.getNumResults() == 1) {
                individualAcl = transformStudyAclToIndividualAcl(studyAclQueryResult.first());
            }
        } else {
            individualAcl = resolveIndividualPermissions(studyId, individualId, userId);
        }

        if (!individualAcl.getPermissions().contains(permission)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "Individual", individualId, null);
        }
    }

    private IndividualAclEntry resolveIndividualPermissions(long studyId, Individual individual, String userId) throws CatalogException {
        if (individual.getAcl() == null) {
            return resolveIndividualPermissions(studyId, individual.getId(), userId);
        } else {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

            Map<String, IndividualAclEntry> userAclMap = new HashMap<>();
            for (IndividualAclEntry individualAcl : individual.getAcl()) {
                userAclMap.put(individualAcl.getMember(), individualAcl);
            }
            return resolveIndividualPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private IndividualAclEntry resolveIndividualPermissions(long studyId, long individualId, String userId) throws CatalogException {
        QueryResult<Group> group = getGroupBelonging(studyId, userId);
        String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID);
        List<IndividualAclEntry> individualAcls = individualDBAdaptor.getAcl(individualId, userIds).getResult();

        Map<String, IndividualAclEntry> userAclMap = new HashMap<>();
        for (IndividualAclEntry individualAcl : individualAcls) {
            userAclMap.put(individualAcl.getMember(), individualAcl);
        }

        return resolveIndividualPermissions(studyId, userId, groupId, userAclMap);
    }

    private IndividualAclEntry resolveIndividualPermissions(long studyId, String userId, String groupId, Map<String,
            IndividualAclEntry> userAclMap) throws CatalogException {
        if (userAclMap.containsKey(userId)) {
            return userAclMap.get(userId);
        } else if (groupId != null && userAclMap.containsKey(groupId)) {
            return userAclMap.get(groupId);
        } else if (userAclMap.containsKey(OTHER_USERS_ID)) {
            return userAclMap.get(OTHER_USERS_ID);
        } else {
            return transformStudyAclToIndividualAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void checkJobPermission(long jobId, String userId, JobAclEntry.JobPermissions permission) throws CatalogException {
        long studyId = jobDBAdaptor.getStudyId(jobId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        JobAclEntry jobAcl = null;
        if (userId.equals("admin")) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList("admin"));
            if (studyAclQueryResult.getNumResults() == 1) {
                jobAcl = transformStudyAclToJobAcl(studyAclQueryResult.first());
            }
        } else {
            jobAcl = resolveJobPermissions(studyId, jobId, userId);
        }


        if (!jobAcl.getPermissions().contains(permission)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "Job", jobId, null);
        }
    }

    private JobAclEntry resolveJobPermissions(long studyId, Job job, String userId) throws CatalogException {
        if (job.getAcl() == null) {
            return resolveJobPermissions(studyId, job.getId(), userId);
        } else {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

            Map<String, JobAclEntry> userAclMap = new HashMap<>();
            for (JobAclEntry jobAcl : job.getAcl()) {
                userAclMap.put(jobAcl.getMember(), jobAcl);
            }
            return resolveJobPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private JobAclEntry resolveJobPermissions(long studyId, long jobId, String userId) throws CatalogException {
        QueryResult<Group> group = getGroupBelonging(studyId, userId);
        String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID);
        List<JobAclEntry> jobAcls = jobDBAdaptor.getAcl(jobId, userIds).getResult();

        Map<String, JobAclEntry> userAclMap = new HashMap<>();
        for (JobAclEntry jobAcl : jobAcls) {
            userAclMap.put(jobAcl.getMember(), jobAcl);
        }

        return resolveJobPermissions(studyId, userId, groupId, userAclMap);
    }

    private JobAclEntry resolveJobPermissions(long studyId, String userId, String groupId, Map<String, JobAclEntry> userAclMap)
            throws CatalogException {
        if (userAclMap.containsKey(userId)) {
            return userAclMap.get(userId);
        } else if (groupId != null && userAclMap.containsKey(groupId)) {
            return userAclMap.get(groupId);
        } else if (userAclMap.containsKey(OTHER_USERS_ID)) {
            return userAclMap.get(OTHER_USERS_ID);
        } else {
            return transformStudyAclToJobAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void checkCohortPermission(long cohortId, String userId, CohortAclEntry.CohortPermissions permission) throws CatalogException {
        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        CohortAclEntry cohortAcl = null;
        if (userId.equals("admin")) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList("admin"));
            if (studyAclQueryResult.getNumResults() == 1) {
                cohortAcl = transformStudyAclToCohortAcl(studyAclQueryResult.first());
            }
        } else {
            cohortAcl = resolveCohortPermissions(studyId, cohortId, userId);
        }

        if (!cohortAcl.getPermissions().contains(permission)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "Cohort", cohortId, null);
        }
    }

    private CohortAclEntry resolveCohortPermissions(long studyId, Cohort cohort, String userId) throws CatalogException {
        if (cohort.getAcl() == null) {
            return resolveCohortPermissions(studyId, cohort.getId(), userId);
        } else {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

            Map<String, CohortAclEntry> userAclMap = new HashMap<>();
            for (CohortAclEntry cohortAcl : cohort.getAcl()) {
                userAclMap.put(cohortAcl.getMember(), cohortAcl);
            }
            return resolveCohortPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private CohortAclEntry resolveCohortPermissions(long studyId, long cohortId, String userId) throws CatalogException {
        QueryResult<Group> group = getGroupBelonging(studyId, userId);
        String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID);
        List<CohortAclEntry> cohortAcls = cohortDBAdaptor.getAcl(cohortId, userIds).getResult();

        Map<String, CohortAclEntry> userAclMap = new HashMap<>();
        for (CohortAclEntry cohortAcl : cohortAcls) {
            userAclMap.put(cohortAcl.getMember(), cohortAcl);
        }

        return resolveCohortPermissions(studyId, userId, groupId, userAclMap);
    }

    private CohortAclEntry resolveCohortPermissions(long studyId, String userId, String groupId, Map<String, CohortAclEntry> userAclMap)
            throws CatalogException {
        if (userAclMap.containsKey(userId)) {
            return userAclMap.get(userId);
        } else if (groupId != null && userAclMap.containsKey(groupId)) {
            return userAclMap.get(groupId);
        } else if (userAclMap.containsKey(OTHER_USERS_ID)) {
            return userAclMap.get(OTHER_USERS_ID);
        } else {
            return transformStudyAclToCohortAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void checkDatasetPermission(long datasetId, String userId, DatasetAclEntry.DatasetPermissions permission)
            throws CatalogException {
        long studyId = datasetDBAdaptor.getStudyIdByDatasetId(datasetId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        DatasetAclEntry datasetAcl = null;
        if (userId.equals("admin")) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList("admin"));
            if (studyAclQueryResult.getNumResults() == 1) {
                datasetAcl = transformStudyAclToDatasetAcl(studyAclQueryResult.first());
            }
        } else {
            datasetAcl = resolveDatasetPermissions(studyId, datasetId, userId);
        }

        if (!datasetAcl.getPermissions().contains(permission)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "Dataset", datasetId, null);
        }
    }

    private DatasetAclEntry resolveDatasetPermissions(long studyId, Dataset dataset, String userId) throws CatalogException {
        if (dataset.getAcl() == null) {
            return resolveDatasetPermissions(studyId, dataset.getId(), userId);
        } else {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

            Map<String, DatasetAclEntry> userAclMap = new HashMap<>();
            for (DatasetAclEntry datasetAcl : dataset.getAcl()) {
                userAclMap.put(datasetAcl.getMember(), datasetAcl);
            }
            return resolveDatasetPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private DatasetAclEntry resolveDatasetPermissions(long studyId, long datasetId, String userId) throws CatalogException {
        QueryResult<Group> group = getGroupBelonging(studyId, userId);
        String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID);
        List<DatasetAclEntry> datasetAcls = datasetDBAdaptor.getAcl(datasetId, userIds).getResult();

        Map<String, DatasetAclEntry> userAclMap = new HashMap<>();
        for (DatasetAclEntry datasetAcl : datasetAcls) {
            userAclMap.put(datasetAcl.getMember(), datasetAcl);
        }

        return resolveDatasetPermissions(studyId, userId, groupId, userAclMap);
    }

    private DatasetAclEntry resolveDatasetPermissions(long studyId, String userId, String groupId, Map<String, DatasetAclEntry> userAclMap)
            throws CatalogException {
        if (userAclMap.containsKey(userId)) {
            return userAclMap.get(userId);
        } else if (groupId != null && userAclMap.containsKey(groupId)) {
            return userAclMap.get(groupId);
        } else if (userAclMap.containsKey(OTHER_USERS_ID)) {
            return userAclMap.get(OTHER_USERS_ID);
        } else {
            return transformStudyAclToDatasetAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void checkDiseasePanelPermission(long panelId, String userId, DiseasePanelAclEntry.DiseasePanelPermissions permission)
            throws CatalogException {
        long studyId = panelDBAdaptor.getStudyId(panelId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        DiseasePanelAclEntry panelAcl = null;
        if (userId.equals("admin")) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList("admin"));
            if (studyAclQueryResult.getNumResults() == 1) {
                panelAcl = transformStudyAclToDiseasePanelAcl(studyAclQueryResult.first());
            }
        } else {
            panelAcl = resolveDiseasePanelPermissions(studyId, panelId, userId);
        }

        if (!panelAcl.getPermissions().contains(permission)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "DiseasePanel", panelId, null);
        }
    }

    private DiseasePanelAclEntry resolveDiseasePanelPermissions(long studyId, DiseasePanel panel, String userId) throws CatalogException {
        if (panel.getAcl() == null) {
            return resolveDiseasePanelPermissions(studyId, panel.getId(), userId);
        } else {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

            Map<String, DiseasePanelAclEntry> userAclMap = new HashMap<>();
            for (DiseasePanelAclEntry panelAcl : panel.getAcl()) {
                userAclMap.put(panelAcl.getMember(), panelAcl);
            }
            return resolveDiseasePanelPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private DiseasePanelAclEntry resolveDiseasePanelPermissions(long studyId, long panelId, String userId) throws CatalogException {
        QueryResult<Group> group = getGroupBelonging(studyId, userId);
        String groupId = group.getNumResults() == 1 ? group.first().getName() : null;

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID);
        List<DiseasePanelAclEntry> panelAcls = panelDBAdaptor.getAcl(panelId, userIds).getResult();

        Map<String, DiseasePanelAclEntry> userAclMap = new HashMap<>();
        for (DiseasePanelAclEntry panelAcl : panelAcls) {
            userAclMap.put(panelAcl.getMember(), panelAcl);
        }

        return resolveDiseasePanelPermissions(studyId, userId, groupId, userAclMap);
    }

    private DiseasePanelAclEntry resolveDiseasePanelPermissions(long studyId, String userId, String groupId,
                                                                Map<String, DiseasePanelAclEntry> userAclMap) throws CatalogException {
        if (userAclMap.containsKey(userId)) {
            return userAclMap.get(userId);
        } else if (groupId != null && userAclMap.containsKey(groupId)) {
            return userAclMap.get(groupId);
        } else if (userAclMap.containsKey(OTHER_USERS_ID)) {
            return userAclMap.get(OTHER_USERS_ID);
        } else {
            return transformStudyAclToDiseasePanelAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void filterProjects(String userId, List<Project> projects) throws CatalogException {
        if (projects == null || projects.isEmpty()) {
            return;
        }
        if (userId.equals("admin")) {
            return;
        }
        Iterator<Project> projectIt = projects.iterator();
        while (projectIt.hasNext()) {
            Project p = projectIt.next();
            try {
                checkProjectPermission(p.getId(), userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);
            } catch (CatalogAuthorizationException e) {
                projectIt.remove();
                continue;
            }
            filterStudies(userId, p.getStudies());
        }
    }

    @Override
    public void filterStudies(String userId, List<Study> studies) throws CatalogException {
        if (studies == null || studies.isEmpty()) {
            return;
        }
        if (userId.equals("admin")) {
            return;
        }
        Iterator<Study> studyIt = studies.iterator();
        while (studyIt.hasNext()) {
            Study study = studyIt.next();
            try {
                checkStudyPermission(study.getId(), userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);
            } catch (CatalogAuthorizationException e) {
                studyIt.remove();
                continue;
            }
            StudyAuthenticationContext studyAuthenticationContext = new StudyAuthenticationContext(study.getId());
            filterFiles(userId, study.getId(), study.getFiles(), studyAuthenticationContext);
            filterSamples(userId, study.getId(), study.getSamples());
            filterJobs(userId, study.getId(), study.getJobs());
            filterCohorts(userId, study.getId(), study.getCohorts());
            filterIndividuals(userId, study.getId(), study.getIndividuals());
            filterDatasets(userId, study.getId(), study.getDatasets());
        }
    }

    @Override
    public void filterFiles(String userId, long studyId, List<File> files) throws CatalogException {
        filterFiles(userId, studyId, files, new StudyAuthenticationContext(studyId));
    }

    private void filterFiles(String userId, long studyId, List<File> files, StudyAuthenticationContext studyAuthenticationContext)
            throws CatalogException {
        if (files == null || files.isEmpty()) {
            return;
        }
        if (userId.equals("admin")) {
            return;
        }
        if (isStudyOwner(studyId, userId)) {
            return;
        }

//        getFileAcls(studyAuthenticationContext, userId, files);

        Iterator<File> fileIt = files.iterator();
        while (fileIt.hasNext()) {
            File file = fileIt.next();
            FileAclEntry fileAcl = resolveFilePermissions(file, userId, studyId, studyAuthenticationContext);
            if (!fileAcl.getPermissions().contains(FileAclEntry.FilePermissions.VIEW)) {
                fileIt.remove();
            }
        }
    }

    @Override
    public void filterSamples(String userId, long studyId, List<Sample> samples) throws CatalogException {
        if (samples == null || samples.isEmpty()) {
            return;
        }
        if (userId.equals("admin")) {
            return;
        }
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        Iterator<Sample> sampleIterator = samples.iterator();
        while (sampleIterator.hasNext()) {
            Sample sample = sampleIterator.next();
            SampleAclEntry sampleACL = resolveSamplePermissions(studyId, sample, userId);
            if (!sampleACL.getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW)) {
                sampleIterator.remove();
                continue;
            }

            if (!sampleACL.getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW_ANNOTATIONS)) {
                sample.setAnnotationSets(new ArrayList<>());
            }
        }
    }

    @Override
    public void filterIndividuals(String userId, long studyId, List<Individual> individuals) throws CatalogException {
        if (individuals == null || individuals.isEmpty()) {
            return;
        }
        if (userId.equals("admin")) {
            return;
        }
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        Iterator<Individual> individualIterator = individuals.iterator();
        while (individualIterator.hasNext()) {
            Individual individual = individualIterator.next();
            IndividualAclEntry individualAcl = resolveIndividualPermissions(studyId, individual, userId);
            if (!individualAcl.getPermissions().contains(IndividualAclEntry.IndividualPermissions.VIEW)) {
                individualIterator.remove();
                continue;
            }

            if (!individualAcl.getPermissions().contains(IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS)) {
                individual.setAnnotationSets(new ArrayList<>());
            }
        }
    }

    @Override
    public void filterCohorts(String userId, long studyId, List<Cohort> cohorts) throws CatalogException {
        if (cohorts == null || cohorts.isEmpty()) {
            return;
        }
        if (userId.equals("admin")) {
            return;
        }
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        Iterator<Cohort> cohortIterator = cohorts.iterator();
        while (cohortIterator.hasNext()) {
            Cohort cohort = cohortIterator.next();
            CohortAclEntry cohortAcl = resolveCohortPermissions(studyId, cohort, userId);
            if (!cohortAcl.getPermissions().contains(CohortAclEntry.CohortPermissions.VIEW)) {
                cohortIterator.remove();
                continue;
            }

            if (!cohortAcl.getPermissions().contains(CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS)) {
                cohort.setAnnotationSets(new ArrayList<>());
            }
        }
    }

    @Override
    public void filterJobs(String userId, long studyId, List<Job> jobs) throws CatalogException {
        if (jobs == null || jobs.isEmpty()) {
            return;
        }
        if (userId.equals("admin")) {
            return;
        }
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        Iterator<Job> jobIterator = jobs.iterator();
        while (jobIterator.hasNext()) {
            Job job = jobIterator.next();
            JobAclEntry jobAcl = resolveJobPermissions(studyId, job, userId);
            if (!jobAcl.getPermissions().contains(JobAclEntry.JobPermissions.VIEW)) {
                jobIterator.remove();
            }
        }
    }

    @Override
    public void filterDatasets(String userId, long studyId, List<Dataset> datasets) throws CatalogException {
        if (datasets == null || datasets.isEmpty()) {
            return;
        }
        if (userId.equals("admin")) {
            return;
        }
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        Iterator<Dataset> datasetIterator = datasets.iterator();
        while (datasetIterator.hasNext()) {
            Dataset dataset = datasetIterator.next();
            DatasetAclEntry datasetAcl = resolveDatasetPermissions(studyId, dataset, userId);
            if (!datasetAcl.getPermissions().contains(DatasetAclEntry.DatasetPermissions.VIEW)) {
                datasetIterator.remove();
            }
        }
    }

    @Override
    public QueryResult<StudyAclEntry> createStudyAcls(String userId, long studyId, List<String> members, List<String> permissions,
                                                      String template) throws CatalogException {
        studyDBAdaptor.checkId(studyId);
        checkMembers(dbAdaptorFactory, studyId, members);

        checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);

        // We obtain the permissions present in the demanded template (if present)
        EnumSet<StudyAclEntry.StudyPermissions> studyPermissions = AuthorizationManager.getLockedAcls();
        if (template != null && !template.isEmpty()) {
            if (template.equals("admin")) {
                studyPermissions = AuthorizationManager.getAdminAcls();
            } else if (template.equals("analyst")) {
                studyPermissions = AuthorizationManager.getAnalystAcls();
            }
        }
        // Add the permissions present in permissions
        studyPermissions.addAll(permissions.stream().map(StudyAclEntry.StudyPermissions::valueOf).collect(Collectors.toList()));

        // If the user already has permissions set, we cannot create a new set of permissions
        List<StudyAclEntry> studyAclList = new ArrayList<>(members.size());
        for (String member : members) {
            if (memberHasPermissionsInStudy(studyId, member)) {
                throw new CatalogException("The member " + member + " already has some permissions set in study. Please, remove those"
                        + " permissions or add, remove or set new permissions.");
            }
        }

        int timeSpent = 0;
        for (String member : members) {
            StudyAclEntry studyAcl = new StudyAclEntry(member, studyPermissions);
            QueryResult<StudyAclEntry> studyAclQueryResult = studyDBAdaptor.createAcl(studyId, studyAcl);
            timeSpent += studyAclQueryResult.getDbTime();
            studyAclList.add(studyAclQueryResult.first());
        }

        return new QueryResult<>("create study Acls", timeSpent, studyAclList.size(), studyAclList.size(), "", "", studyAclList);
    }

    @Override
    public QueryResult<StudyAclEntry> getAllStudyAcls(String userId, long studyId) throws CatalogException {
        studyDBAdaptor.checkId(studyId);
        checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);

        Query query = new Query(StudyDBAdaptor.QueryParams.ID.key(), studyId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.ACL.key());
        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, queryOptions);

        List<StudyAclEntry> studyAclList;
        if (studyQueryResult == null || studyQueryResult.getNumResults() == 0) {
            studyAclList = Collections.emptyList();
        } else {
            studyAclList = studyQueryResult.first().getAcl();
        }

        return new QueryResult<>("Get all study Acls", studyQueryResult.getDbTime(), studyAclList.size(), studyAclList.size(),
                studyQueryResult.getWarningMsg(), studyQueryResult.getErrorMsg(), studyAclList);
    }

    @Override
    public QueryResult<StudyAclEntry> getStudyAcl(String userId, long studyId, String member) throws CatalogException {
        studyDBAdaptor.checkId(studyId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            if (member.startsWith("@")) { //group
                // If the userId does not belong to the group...
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
                if (groupBelonging.getNumResults() != 1 || !groupBelonging.first().getName().equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            } else {
                // If the userId asking to see the permissions is not asking to see their own permissions
                if (!userId.equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            }
        }

        List<String> members = new ArrayList<>(2);
        members.add(member);
        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
                members.add(groupBelonging.first().getName());
            }
        }

        return studyDBAdaptor.getAcl(studyId, members);
    }

    @Override
    public QueryResult<StudyAclEntry> removeStudyAcl(String userId, long studyId, String member) throws CatalogException {
        studyDBAdaptor.checkId(studyId);

        checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);

        // Cannot remove permissions for the owner of the study
        String studyOwnerId = studyDBAdaptor.getOwnerId(studyId);
        if (member.equals(studyOwnerId)) {
            throw new CatalogException("Error: It is not allowed removing the permissions to the owner of the study.");
        }

        // Obtain the ACLs the member had
        QueryResult<StudyAclEntry> studyAcl = studyDBAdaptor.getAcl(studyId, Arrays.asList(member));
        if (studyAcl == null || studyAcl.getNumResults() == 0) {
            throw new CatalogException("Could not remove the ACLs for " + member + ". It seems " + member + " did not have any ACLs "
                    + "defined");
        }

        studyDBAdaptor.removeAcl(studyId, member);

        studyAcl.setId("Remove study ACLs");
        return studyAcl;
    }

    @Override
    public QueryResult<StudyAclEntry> updateStudyAcl(String userId, long studyId, String member, @Nullable String addPermissions,
                                                     @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException {
        studyDBAdaptor.checkId(studyId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));
        checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);

        // Check that the member has permissions
        Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.ID.key(), studyId)
                .append(StudyDBAdaptor.QueryParams.ACL_MEMBER.key(), member);
        QueryResult<Long> count = studyDBAdaptor.count(query);
        if (count == null || count.first() == 0) {
            throw new CatalogException("Could not update ACLs for " + member + ". It seems the member does not have any permissions set "
                    + "yet.");
        }

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(setPermissions.split(","));
            // Check if the permissions are correct
            checkPermissions(permissions, StudyAclEntry.StudyPermissions::valueOf);

            studyDBAdaptor.setAclsToMember(studyId, member, permissions);
        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(addPermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, StudyAclEntry.StudyPermissions::valueOf);

                studyDBAdaptor.addAclsToMember(studyId, member, permissions);
            }

            if (removePermissions != null) {
                permissions = Arrays.asList(removePermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, StudyAclEntry.StudyPermissions::valueOf);

                studyDBAdaptor.removeAclsFromMember(studyId, member, permissions);
            }
        }

        return studyDBAdaptor.getAcl(studyId, Arrays.asList(member));
    }

    @Override
    public QueryResult<SampleAclEntry> createSampleAcls(String userId, long sampleId, List<String> members, List<String> permissions)
            throws CatalogException {
        sampleDBAdaptor.checkId(sampleId);
        // Check if the userId has proper permissions for all the samples.
        checkSamplePermission(sampleId, userId, SampleAclEntry.SamplePermissions.SHARE);

        // Check if all the members have a permission already set at the study level.
        long studyId = sampleDBAdaptor.getStudyId(sampleId);
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberHasPermissionsInStudy(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        // Check all the members exist in all the possible different studies
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check if any of the members already have permissions set in the sample
        if (anyMemberHasPermissions(studyId, sampleId, members, sampleDBAdaptor)) {
            throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                    + "particular sample. Please, use update instead.");
        }

        // Set the permissions
        int timeSpent = 0;
        List<SampleAclEntry> sampleAclList = new ArrayList<>(members.size());
        for (String member : members) {
            SampleAclEntry sampleAcl = new SampleAclEntry(member, permissions);
            QueryResult<SampleAclEntry> sampleAclQueryResult = sampleDBAdaptor.createAcl(sampleId, sampleAcl);
            timeSpent += sampleAclQueryResult.getDbTime();
            sampleAclList.add(sampleAclQueryResult.first());
        }

        return new QueryResult<>("create sample acl", timeSpent, sampleAclList.size(), sampleAclList.size(), "", "", sampleAclList);
    }

    @Override
    public QueryResult<SampleAclEntry> getAllSampleAcls(String userId, long sampleId) throws CatalogException {
        sampleDBAdaptor.checkId(sampleId);
        // Check if the userId has proper permissions for all the samples.
        checkSamplePermission(sampleId, userId, SampleAclEntry.SamplePermissions.SHARE);

        // Obtain the Acls
        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), sampleId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ACL.key());
        QueryResult<Sample> queryResult = sampleDBAdaptor.get(query, queryOptions);

        List<SampleAclEntry> aclList;
        if (queryResult != null && queryResult.getNumResults() == 1) {
            aclList = queryResult.first().getAcl();
        } else {
            aclList = Collections.emptyList();
        }
        return new QueryResult<>("get sample acl", queryResult.getDbTime(), aclList.size(), aclList.size(), queryResult.getWarningMsg(),
                queryResult.getErrorMsg(), aclList);
    }

    @Override
    public QueryResult<SampleAclEntry> getSampleAcl(String userId, long sampleId, String member) throws CatalogException {
        sampleDBAdaptor.checkId(sampleId);

        long studyId = sampleDBAdaptor.getStudyId(sampleId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkSamplePermission(sampleId, userId, SampleAclEntry.SamplePermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            if (member.startsWith("@")) { //group
                // If the userId does not belong to the group...
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
                if (groupBelonging.getNumResults() != 1 || !groupBelonging.first().getName().equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            } else {
                // If the userId asking to see the permissions is not asking to see their own permissions
                if (!userId.equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            }
        }

        List<String> members = new ArrayList<>(2);
        members.add(member);
        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
                members.add(groupBelonging.first().getName());
            }
        }

        return sampleDBAdaptor.getAcl(studyId, members);
    }

    @Override
    public QueryResult<SampleAclEntry> removeSampleAcl(String userId, long sampleId, String member) throws CatalogException {
        sampleDBAdaptor.checkId(sampleId);
        checkSamplePermission(sampleId, userId, SampleAclEntry.SamplePermissions.SHARE);

        long studyId = sampleDBAdaptor.getStudyId(sampleId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Obtain the ACLs the member had
        QueryResult<SampleAclEntry> sampleAcl = sampleDBAdaptor.getAcl(sampleId, Arrays.asList(member));
        if (sampleAcl == null || sampleAcl.getNumResults() == 0) {
            throw new CatalogException("Could not remove the ACLs for " + member + ". It seems " + member + " did not have any ACLs "
                    + "defined");
        }

        sampleDBAdaptor.removeAcl(sampleId, member);

        sampleAcl.setId("Remove sample ACLs");
        return sampleAcl;
    }

    @Override
    public QueryResult<SampleAclEntry> updateSampleAcl(String userId, long sampleId, String member, @Nullable String addPermissions,
                                                       @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException {
        sampleDBAdaptor.checkId(sampleId);
        checkSamplePermission(sampleId, userId, SampleAclEntry.SamplePermissions.SHARE);

        long studyId = sampleDBAdaptor.getStudyId(sampleId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Check that the member has permissions
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), sampleId)
                .append(SampleDBAdaptor.QueryParams.ACL_MEMBER.key(), member);
        QueryResult<Long> count = sampleDBAdaptor.count(query);
        if (count == null || count.first() == 0) {
            throw new CatalogException("Could not update ACLs for " + member + ". It seems the member does not have any permissions set "
                    + "yet.");
        }

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(setPermissions.split(","));
            // Check if the permissions are correct
            checkPermissions(permissions, SampleAclEntry.SamplePermissions::valueOf);

            sampleDBAdaptor.setAclsToMember(sampleId, member, permissions);
        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(addPermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, SampleAclEntry.SamplePermissions::valueOf);

                sampleDBAdaptor.addAclsToMember(sampleId, member, permissions);
            }

            if (removePermissions != null) {
                permissions = Arrays.asList(removePermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, SampleAclEntry.SamplePermissions::valueOf);

                sampleDBAdaptor.removeAclsFromMember(sampleId, member, permissions);
            }
        }

        return sampleDBAdaptor.getAcl(sampleId, Arrays.asList(member));
    }

    @Override
    public QueryResult<FileAclEntry> createFileAcls(String userId, long fileId, List<String> members, List<String> permissions)
            throws CatalogException {
        fileDBAdaptor.checkId(fileId);

        // Check if the userId has proper permissions for all the files.
        checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.SHARE);

        // Check if all the members have a permission already set at the study level.
        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberHasPermissionsInStudy(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        // Check all the members exist in all the possible different studies
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check if any of the members already have permissions set in the file
        if (anyMemberHasPermissions(studyId, fileId, members, fileDBAdaptor)) {
            throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                    + "particular file. Please, use update instead.");
        }

        // Set the permissions
        int timeSpent = 0;
        List<FileAclEntry> fileAclList = new ArrayList<>(members.size());
        for (String member : members) {
            FileAclEntry fileAcl = new FileAclEntry(member, permissions);
            QueryResult<FileAclEntry> fileAclQueryResult = fileDBAdaptor.createAcl(fileId, fileAcl);
            timeSpent += fileAclQueryResult.getDbTime();
            fileAclList.add(fileAclQueryResult.first());
        }

        return new QueryResult<>("create file acl", timeSpent, fileAclList.size(), fileAclList.size(), "", "", fileAclList);
    }

    @Override
    public QueryResult<FileAclEntry> getAllFileAcls(String userId, long fileId) throws CatalogException {
        fileDBAdaptor.checkId(fileId);
        // Check if the userId has proper permissions for all the samples.
        checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.SHARE);

        // Obtain the Acls
        Query query = new Query(FileDBAdaptor.QueryParams.ID.key(), fileId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ACL.key());
        QueryResult<File> queryResult = fileDBAdaptor.get(query, queryOptions);

        List<FileAclEntry> aclList;
        if (queryResult != null && queryResult.getNumResults() == 1) {
            aclList = queryResult.first().getAcl();
        } else {
            aclList = Collections.emptyList();
        }
        return new QueryResult<>("get file acl", queryResult.getDbTime(), aclList.size(), aclList.size(), queryResult.getWarningMsg(),
                queryResult.getErrorMsg(), aclList);
    }

    @Override
    public QueryResult<FileAclEntry> getFileAcl(String userId, long fileId, String member) throws CatalogException {
        fileDBAdaptor.checkId(fileId);

        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            if (member.startsWith("@")) { //group
                // If the userId does not belong to the group...
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
                if (groupBelonging.getNumResults() != 1 || !groupBelonging.first().getName().equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            } else {
                // If the userId asking to see the permissions is not asking to see their own permissions
                if (!userId.equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            }
        }

        List<String> members = new ArrayList<>(2);
        members.add(member);
        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
                members.add(groupBelonging.first().getName());
            }
        }

        return fileDBAdaptor.getAcl(fileId, members);
    }

    @Override
    public QueryResult<FileAclEntry> removeFileAcl(String userId, long fileId, String member) throws CatalogException {
        fileDBAdaptor.checkId(fileId);
        checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.SHARE);

        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Obtain the ACLs the member had
        QueryResult<FileAclEntry> fileAcl = fileDBAdaptor.getAcl(fileId, Arrays.asList(member));
        if (fileAcl == null || fileAcl.getNumResults() == 0) {
            throw new CatalogException("Could not remove the ACLs for " + member + ". It seems " + member + " did not have any ACLs "
                    + "defined");
        }

        fileDBAdaptor.removeAcl(fileId, member);

        fileAcl.setId("Remove file ACLs");
        return fileAcl;
    }

    @Override
    public QueryResult<FileAclEntry> updateFileAcl(String userId, long fileId, String member, @Nullable String addPermissions,
                                                   @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException {
        fileDBAdaptor.checkId(fileId);
        checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.SHARE);

        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Check that the member has permissions
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.ID.key(), fileId)
                .append(FileDBAdaptor.QueryParams.ACL_MEMBER.key(), member);
        QueryResult<Long> count = fileDBAdaptor.count(query);
        if (count == null || count.first() == 0) {
            throw new CatalogException("Could not update ACLs for " + member + ". It seems the member does not have any permissions set "
                    + "yet.");
        }

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(setPermissions.split(","));
            // Check if the permissions are correct
            checkPermissions(permissions, FileAclEntry.FilePermissions::valueOf);

            fileDBAdaptor.setAclsToMember(fileId, member, permissions);
        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(addPermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, FileAclEntry.FilePermissions::valueOf);

                fileDBAdaptor.addAclsToMember(fileId, member, permissions);
            }

            if (removePermissions != null) {
                permissions = Arrays.asList(removePermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, FileAclEntry.FilePermissions::valueOf);

                fileDBAdaptor.removeAclsFromMember(fileId, member, permissions);
            }
        }

        return fileDBAdaptor.getAcl(fileId, Arrays.asList(member));
    }

    @Override
    public QueryResult<IndividualAclEntry> createIndividualAcls(String userId, long individualId, List<String> members,
                                                                List<String> permissions) throws CatalogException {
        individualDBAdaptor.checkId(individualId);
        // Check if the userId has proper permissions for all the samples.
        checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);

        // Check if all the members have a permission already set at the study level.
        long studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberHasPermissionsInStudy(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        // Check all the members exist in all the possible different studies
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check if any of the members already have permissions set in the individual
        if (anyMemberHasPermissions(studyId, individualId, members, individualDBAdaptor)) {
            throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                    + "particular individual. Please, use update instead.");
        }

        // Set the permissions
        int timeSpent = 0;
        List<IndividualAclEntry> individualAclList = new ArrayList<>(members.size());
        for (String member : members) {
            IndividualAclEntry individualAcl = new IndividualAclEntry(member, permissions);
            QueryResult<IndividualAclEntry> individualAclQueryResult = individualDBAdaptor.createAcl(individualId, individualAcl);
            timeSpent += individualAclQueryResult.getDbTime();
            individualAclList.add(individualAclQueryResult.first());
        }

        return new QueryResult<>("create individual acl", timeSpent, individualAclList.size(), individualAclList.size(), "", "",
                individualAclList);
    }

    @Override
    public QueryResult<IndividualAclEntry> getAllIndividualAcls(String userId, long individualId) throws CatalogException {
        individualDBAdaptor.checkId(individualId);
        // Check if the userId has proper permissions for all the individuals.
        checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);

        // Obtain the Acls
        Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), individualId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ACL.key());
        QueryResult<Individual> queryResult = individualDBAdaptor.get(query, queryOptions);

        List<IndividualAclEntry> aclList;
        if (queryResult != null && queryResult.getNumResults() == 1) {
            aclList = queryResult.first().getAcl();
        } else {
            aclList = Collections.emptyList();
        }
        return new QueryResult<>("get individual acl", queryResult.getDbTime(), aclList.size(), aclList.size(),
                queryResult.getWarningMsg(), queryResult.getErrorMsg(), aclList);
    }

    @Override
    public QueryResult<IndividualAclEntry> getIndividualAcl(String userId, long individualId, String member) throws CatalogException {
        individualDBAdaptor.checkId(individualId);

        long studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            if (member.startsWith("@")) { //group
                // If the userId does not belong to the group...
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
                if (groupBelonging.getNumResults() != 1 || !groupBelonging.first().getName().equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            } else {
                // If the userId asking to see the permissions is not asking to see their own permissions
                if (!userId.equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            }
        }

        List<String> members = new ArrayList<>(2);
        members.add(member);
        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
                members.add(groupBelonging.first().getName());
            }
        }

        return individualDBAdaptor.getAcl(individualId, members);
    }

    @Override
    public QueryResult<IndividualAclEntry> removeIndividualAcl(String userId, long individualId, String member) throws CatalogException {
        individualDBAdaptor.checkId(individualId);
        checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);

        long studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Obtain the ACLs the member had
        QueryResult<IndividualAclEntry> individualDBAdaptorAcl = individualDBAdaptor.getAcl(individualId, Arrays.asList(member));
        if (individualDBAdaptorAcl == null || individualDBAdaptorAcl.getNumResults() == 0) {
            throw new CatalogException("Could not remove the ACLs for " + member + ". It seems " + member + " did not have any ACLs "
                    + "defined");
        }

        individualDBAdaptor.removeAcl(individualId, member);

        individualDBAdaptorAcl.setId("Remove individual ACLs");
        return individualDBAdaptorAcl;
    }

    @Override
    public QueryResult<IndividualAclEntry> updateIndividualAcl(String userId, long individualId, String member,
                                                               @Nullable String addPermissions, @Nullable String removePermissions,
                                                               @Nullable String setPermissions) throws CatalogException {
        individualDBAdaptor.checkId(individualId);
        checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);

        long studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Check that the member has permissions
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.ID.key(), individualId)
                .append(IndividualDBAdaptor.QueryParams.ACL_MEMBER.key(), member);
        QueryResult<Long> count = individualDBAdaptor.count(query);
        if (count == null || count.first() == 0) {
            throw new CatalogException("Could not update ACLs for " + member + ". It seems the member does not have any permissions set "
                    + "yet.");
        }

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(setPermissions.split(","));
            // Check if the permissions are correct
            checkPermissions(permissions, IndividualAclEntry.IndividualPermissions::valueOf);

            individualDBAdaptor.setAclsToMember(individualId, member, permissions);
        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(addPermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, IndividualAclEntry.IndividualPermissions::valueOf);

                individualDBAdaptor.addAclsToMember(individualId, member, permissions);
            }

            if (removePermissions != null) {
                permissions = Arrays.asList(removePermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, IndividualAclEntry.IndividualPermissions::valueOf);

                individualDBAdaptor.removeAclsFromMember(individualId, member, permissions);
            }
        }

        return individualDBAdaptor.getAcl(individualId, Arrays.asList(member));
    }

    @Override
    public QueryResult<CohortAclEntry> createCohortAcls(String userId, long cohortId, List<String> members, List<String> permissions)
            throws CatalogException {
        cohortDBAdaptor.checkId(cohortId);
        // Check if the userId has proper permissions for all the cohorts.
        checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);

        // Check if all the members have a permission already set at the study level.
        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberHasPermissionsInStudy(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        // Check all the members exist in all the possible different studies
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check if any of the members already have permissions set in the file
        if (anyMemberHasPermissions(studyId, cohortId, members, cohortDBAdaptor)) {
            throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                    + "particular cohort. Please, use update instead.");
        }

        // Set the permissions
        int timeSpent = 0;
        List<CohortAclEntry> cohortAclList = new ArrayList<>(members.size());
        for (String member : members) {
            CohortAclEntry cohortAcl = new CohortAclEntry(member, permissions);
            QueryResult<CohortAclEntry> cohortAclQueryResult = cohortDBAdaptor.createAcl(cohortId, cohortAcl);
            timeSpent += cohortAclQueryResult.getDbTime();
            cohortAclList.add(cohortAclQueryResult.first());
        }

        return new QueryResult<>("create cohort acl", timeSpent, cohortAclList.size(), cohortAclList.size(), "", "", cohortAclList);
    }

    @Override
    public QueryResult<CohortAclEntry> getAllCohortAcls(String userId, long cohortId) throws CatalogException {
        cohortDBAdaptor.checkId(cohortId);
        // Check if the userId has proper permissions for all the cohorts.
        checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);

        // Obtain the Acls
        Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), cohortId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ACL.key());
        QueryResult<Cohort> queryResult = cohortDBAdaptor.get(query, queryOptions);

        List<CohortAclEntry> aclList;
        if (queryResult != null && queryResult.getNumResults() == 1) {
            aclList = queryResult.first().getAcl();
        } else {
            aclList = Collections.emptyList();
        }
        return new QueryResult<>("get cohort acl", queryResult.getDbTime(), aclList.size(), aclList.size(),
                queryResult.getWarningMsg(), queryResult.getErrorMsg(), aclList);
    }

    @Override
    public QueryResult<CohortAclEntry> getCohortAcl(String userId, long cohortId, String member) throws CatalogException {
        cohortDBAdaptor.checkId(cohortId);

        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            if (member.startsWith("@")) { //group
                // If the userId does not belong to the group...
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
                if (groupBelonging.getNumResults() != 1 || !groupBelonging.first().getName().equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            } else {
                // If the userId asking to see the permissions is not asking to see their own permissions
                if (!userId.equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            }
        }

        List<String> members = new ArrayList<>(2);
        members.add(member);
        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
                members.add(groupBelonging.first().getName());
            }
        }

        return cohortDBAdaptor.getAcl(cohortId, members);
    }

    @Override
    public QueryResult<CohortAclEntry> removeCohortAcl(String userId, long cohortId, String member) throws CatalogException {
        cohortDBAdaptor.checkId(cohortId);
        checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);

        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Obtain the ACLs the member had
        QueryResult<CohortAclEntry> cohortDBAdaptorAcl = cohortDBAdaptor.getAcl(cohortId, Arrays.asList(member));
        if (cohortDBAdaptorAcl == null || cohortDBAdaptorAcl.getNumResults() == 0) {
            throw new CatalogException("Could not remove the ACLs for " + member + ". It seems " + member + " did not have any ACLs "
                    + "defined");
        }

        cohortDBAdaptor.removeAcl(cohortId, member);

        cohortDBAdaptorAcl.setId("Remove cohort ACLs");
        return cohortDBAdaptorAcl;
    }

    @Override
    public QueryResult<CohortAclEntry> updateCohortAcl(String userId, long cohortId, String member, @Nullable String addPermissions,
                                                       @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException {
        cohortDBAdaptor.checkId(cohortId);
        checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);

        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Check that the member has permissions
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.ID.key(), cohortId)
                .append(CohortDBAdaptor.QueryParams.ACL_MEMBER.key(), member);
        QueryResult<Long> count = cohortDBAdaptor.count(query);
        if (count == null || count.first() == 0) {
            throw new CatalogException("Could not update ACLs for " + member + ". It seems the member does not have any permissions set "
                    + "yet.");
        }

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(setPermissions.split(","));
            // Check if the permissions are correct
            checkPermissions(permissions, CohortAclEntry.CohortPermissions::valueOf);

            cohortDBAdaptor.setAclsToMember(cohortId, member, permissions);
        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(addPermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, CohortAclEntry.CohortPermissions::valueOf);

                cohortDBAdaptor.addAclsToMember(cohortId, member, permissions);
            }

            if (removePermissions != null) {
                permissions = Arrays.asList(removePermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, CohortAclEntry.CohortPermissions::valueOf);

                cohortDBAdaptor.removeAclsFromMember(cohortId, member, permissions);
            }
        }

        return cohortDBAdaptor.getAcl(cohortId, Arrays.asList(member));
    }

    @Override
    public QueryResult<DatasetAclEntry> createDatasetAcls(String userId, long datasetId, List<String> members, List<String> permissions)
            throws CatalogException {
        datasetDBAdaptor.checkId(datasetId);
        // Check if the userId has proper permissions for all the datasets.
        checkDatasetPermission(datasetId, userId, DatasetAclEntry.DatasetPermissions.SHARE);

        // Check if all the members have a permission already set at the study level.
        long studyId = datasetDBAdaptor.getStudyIdByDatasetId(datasetId);
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberHasPermissionsInStudy(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        // Check all the members exist in all the possible different studies
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check if any of the members already have permissions set in the file
        if (anyMemberHasPermissions(studyId, datasetId, members, datasetDBAdaptor)) {
            throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                    + "particular dataset. Please, use update instead.");
        }

        // Set the permissions
        int timeSpent = 0;
        List<DatasetAclEntry> datasetAclList = new ArrayList<>(members.size());
        for (String member : members) {
            DatasetAclEntry datasetAcl = new DatasetAclEntry(member, permissions);
            QueryResult<DatasetAclEntry> datasetAclQueryResult = datasetDBAdaptor.createAcl(datasetId, datasetAcl);
            timeSpent += datasetAclQueryResult.getDbTime();
            datasetAclList.add(datasetAclQueryResult.first());
        }

        return new QueryResult<>("create dataset acl", timeSpent, datasetAclList.size(), datasetAclList.size(), "", "",
                datasetAclList);
    }

    @Override
    public QueryResult<DatasetAclEntry> getAllDatasetAcls(String userId, long datasetId) throws CatalogException {
        datasetDBAdaptor.checkId(datasetId);
        // Check if the userId has proper permissions for all the datasets.
        checkDatasetPermission(datasetId, userId, DatasetAclEntry.DatasetPermissions.SHARE);

        // Obtain the Acls
        Query query = new Query(DatasetDBAdaptor.QueryParams.ID.key(), datasetId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, DatasetDBAdaptor.QueryParams.ACL.key());
        QueryResult<Dataset> queryResult = datasetDBAdaptor.get(query, queryOptions);

        List<DatasetAclEntry> aclList;
        if (queryResult != null && queryResult.getNumResults() == 1) {
            aclList = queryResult.first().getAcl();
        } else {
            aclList = Collections.emptyList();
        }
        return new QueryResult<>("get dataset acl", queryResult.getDbTime(), aclList.size(), aclList.size(),
                queryResult.getWarningMsg(), queryResult.getErrorMsg(), aclList);
    }

    @Override
    public QueryResult<DatasetAclEntry> getDatasetAcl(String userId, long datasetId, String member) throws CatalogException {
        datasetDBAdaptor.checkId(datasetId);

        long studyId = datasetDBAdaptor.getStudyIdByDatasetId(datasetId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkDatasetPermission(datasetId, userId, DatasetAclEntry.DatasetPermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            if (member.startsWith("@")) { //group
                // If the userId does not belong to the group...
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
                if (groupBelonging.getNumResults() != 1 || !groupBelonging.first().getName().equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            } else {
                // If the userId asking to see the permissions is not asking to see their own permissions
                if (!userId.equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            }
        }

        List<String> members = new ArrayList<>(2);
        members.add(member);
        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
                members.add(groupBelonging.first().getName());
            }
        }

        return datasetDBAdaptor.getAcl(datasetId, members);
    }

    @Override
    public QueryResult<DatasetAclEntry> removeDatasetAcl(String userId, long datasetId, String member) throws CatalogException {
        datasetDBAdaptor.checkId(datasetId);
        checkDatasetPermission(datasetId, userId, DatasetAclEntry.DatasetPermissions.SHARE);

        long studyId = datasetDBAdaptor.getStudyIdByDatasetId(datasetId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Obtain the ACLs the member had
        QueryResult<DatasetAclEntry> datasetDBAdaptorAcl = datasetDBAdaptor.getAcl(datasetId, Arrays.asList(member));
        if (datasetDBAdaptorAcl == null || datasetDBAdaptorAcl.getNumResults() == 0) {
            throw new CatalogException("Could not remove the ACLs for " + member + ". It seems " + member + " did not have any ACLs "
                    + "defined");
        }

        datasetDBAdaptor.removeAcl(datasetId, member);

        datasetDBAdaptorAcl.setId("Remove dataset ACLs");
        return datasetDBAdaptorAcl;
    }

    @Override
    public QueryResult<DatasetAclEntry> updateDatasetAcl(String userId, long datasetId, String member, @Nullable String addPermissions,
                                                         @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException {
        datasetDBAdaptor.checkId(datasetId);
        checkDatasetPermission(datasetId, userId, DatasetAclEntry.DatasetPermissions.SHARE);

        long studyId = datasetDBAdaptor.getStudyIdByDatasetId(datasetId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Check that the member has permissions
        Query query = new Query()
                .append(DatasetDBAdaptor.QueryParams.ID.key(), datasetId)
                .append(DatasetDBAdaptor.QueryParams.ACL_MEMBER.key(), member);
        QueryResult<Long> count = datasetDBAdaptor.count(query);
        if (count == null || count.first() == 0) {
            throw new CatalogException("Could not update ACLs for " + member + ". It seems the member does not have any permissions set "
                    + "yet.");
        }

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(setPermissions.split(","));
            // Check if the permissions are correct
            checkPermissions(permissions, DatasetAclEntry.DatasetPermissions::valueOf);

            datasetDBAdaptor.setAclsToMember(datasetId, member, permissions);
        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(addPermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, DatasetAclEntry.DatasetPermissions::valueOf);

                datasetDBAdaptor.addAclsToMember(datasetId, member, permissions);
            }

            if (removePermissions != null) {
                permissions = Arrays.asList(removePermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, DatasetAclEntry.DatasetPermissions::valueOf);

                datasetDBAdaptor.removeAclsFromMember(datasetId, member, permissions);
            }
        }

        return datasetDBAdaptor.getAcl(datasetId, Arrays.asList(member));
    }

    @Override
    public QueryResult<JobAclEntry> createJobAcls(String userId, long jobId, List<String> members, List<String> permissions)
            throws CatalogException {
        jobDBAdaptor.checkId(jobId);
        // Check if the userId has proper permissions for all the jobs.
        checkJobPermission(jobId, userId, JobAclEntry.JobPermissions.SHARE);

        // Check if all the members have a permission already set at the study level.
        long studyId = jobDBAdaptor.getStudyId(jobId);
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberHasPermissionsInStudy(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        // Check all the members exist in all the possible different studies
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check if any of the members already have permissions set in the job
        if (anyMemberHasPermissions(studyId, jobId, members, jobDBAdaptor)) {
            throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                    + "particular job. Please, use update instead.");
        }

        // Set the permissions
        int timeSpent = 0;
        List<JobAclEntry> jobAclList = new ArrayList<>(members.size());
        for (String member : members) {
            JobAclEntry jobAcl = new JobAclEntry(member, permissions);
            QueryResult<JobAclEntry> jobAclQueryResult = jobDBAdaptor.createAcl(jobId, jobAcl);
            timeSpent += jobAclQueryResult.getDbTime();
            jobAclList.add(jobAclQueryResult.first());
        }

        return new QueryResult<>("create job acl", timeSpent, jobAclList.size(), jobAclList.size(), "", "", jobAclList);
    }

    @Override
    public QueryResult<JobAclEntry> getAllJobAcls(String userId, long jobId) throws CatalogException {
        jobDBAdaptor.checkId(jobId);
        // Check if the userId has proper permissions for all the jobs.
        checkJobPermission(jobId, userId, JobAclEntry.JobPermissions.SHARE);

        // Obtain the Acls
        Query query = new Query(JobDBAdaptor.QueryParams.ID.key(), jobId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, JobDBAdaptor.QueryParams.ACL.key());
        QueryResult<Job> queryResult = jobDBAdaptor.get(query, queryOptions);

        List<JobAclEntry> aclList;
        if (queryResult != null && queryResult.getNumResults() == 1) {
            aclList = queryResult.first().getAcl();
        } else {
            aclList = Collections.emptyList();
        }
        return new QueryResult<>("get job acl", queryResult.getDbTime(), aclList.size(), aclList.size(),
                queryResult.getWarningMsg(), queryResult.getErrorMsg(), aclList);
    }

    @Override
    public QueryResult<JobAclEntry> getJobAcl(String userId, long jobId, String member) throws CatalogException {
        jobDBAdaptor.checkId(jobId);

        long studyId = jobDBAdaptor.getStudyId(jobId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkJobPermission(jobId, userId, JobAclEntry.JobPermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            if (member.startsWith("@")) { //group
                // If the userId does not belong to the group...
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
                if (groupBelonging.getNumResults() != 1 || !groupBelonging.first().getName().equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            } else {
                // If the userId asking to see the permissions is not asking to see their own permissions
                if (!userId.equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            }
        }

        List<String> members = new ArrayList<>(2);
        members.add(member);
        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
                members.add(groupBelonging.first().getName());
            }
        }

        return jobDBAdaptor.getAcl(jobId, members);
    }

    @Override
    public QueryResult<JobAclEntry> removeJobAcl(String userId, long jobId, String member) throws CatalogException {
        jobDBAdaptor.checkId(jobId);
        checkJobPermission(jobId, userId, JobAclEntry.JobPermissions.SHARE);

        long studyId = jobDBAdaptor.getStudyId(jobId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Obtain the ACLs the member had
        QueryResult<JobAclEntry> jobDBAdaptorAcl = jobDBAdaptor.getAcl(jobId, Arrays.asList(member));
        if (jobDBAdaptorAcl == null || jobDBAdaptorAcl.getNumResults() == 0) {
            throw new CatalogException("Could not remove the ACLs for " + member + ". It seems " + member + " did not have any ACLs "
                    + "defined");
        }

        jobDBAdaptor.removeAcl(jobId, member);

        jobDBAdaptorAcl.setId("Remove job ACLs");
        return jobDBAdaptorAcl;
    }

    @Override
    public QueryResult<JobAclEntry> updateJobAcl(String userId, long jobId, String member, @Nullable String addPermissions,
                                                 @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException {
        jobDBAdaptor.checkId(jobId);
        checkJobPermission(jobId, userId, JobAclEntry.JobPermissions.SHARE);

        long studyId = jobDBAdaptor.getStudyId(jobId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Check that the member has permissions
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.ID.key(), jobId)
                .append(JobDBAdaptor.QueryParams.ACL_MEMBER.key(), member);
        QueryResult<Long> count = jobDBAdaptor.count(query);
        if (count == null || count.first() == 0) {
            throw new CatalogException("Could not update ACLs for " + member + ". It seems the member does not have any permissions set "
                    + "yet.");
        }

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(setPermissions.split(","));
            // Check if the permissions are correct
            checkPermissions(permissions, JobAclEntry.JobPermissions::valueOf);

            jobDBAdaptor.setAclsToMember(jobId, member, permissions);
        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(addPermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, JobAclEntry.JobPermissions::valueOf);

                jobDBAdaptor.addAclsToMember(jobId, member, permissions);
            }

            if (removePermissions != null) {
                permissions = Arrays.asList(removePermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, JobAclEntry.JobPermissions::valueOf);

                jobDBAdaptor.removeAclsFromMember(jobId, member, permissions);
            }
        }

        return jobDBAdaptor.getAcl(jobId, Arrays.asList(member));
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> createPanelAcls(String userId, long panelId, List<String> members, List<String> permissions)
            throws CatalogException {
        panelDBAdaptor.checkId(panelId);
        // Check if the userId has proper permissions for all the panels.
        checkDiseasePanelPermission(panelId, userId, DiseasePanelAclEntry.DiseasePanelPermissions.SHARE);

        // Check if all the members have a permission already set at the study level.
        Set<Long> studySet = new HashSet<>();
        long studyId = panelDBAdaptor.getStudyId(panelId);
        studySet.add(studyId);
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberHasPermissionsInStudy(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        // Check all the members exist in all the possible different studies
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check if any of the members already have permissions set in the panel
        if (anyMemberHasPermissions(studyId, panelId, members, panelDBAdaptor)) {
            throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                    + "particular panel. Please, use update instead.");
        }

        // Set the permissions
        int timeSpent = 0;
        List<DiseasePanelAclEntry> panelAclList = new ArrayList<>(members.size());
        for (String member : members) {
            DiseasePanelAclEntry panelAcl = new DiseasePanelAclEntry(member, permissions);
            QueryResult<DiseasePanelAclEntry> panelAclQueryResult = panelDBAdaptor.createAcl(panelId, panelAcl);
            timeSpent += panelAclQueryResult.getDbTime();
            panelAclList.add(panelAclQueryResult.first());
        }

        return new QueryResult<>("create panel acl", timeSpent, panelAclList.size(), panelAclList.size(), "", "",
                panelAclList);
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> getAllPanelAcls(String userId, long panelId) throws CatalogException {
        panelDBAdaptor.checkId(panelId);
        // Check if the userId has proper permissions for all the panels.
        checkDiseasePanelPermission(panelId, userId, DiseasePanelAclEntry.DiseasePanelPermissions.SHARE);

        // Obtain the Acls
        Query query = new Query(PanelDBAdaptor.QueryParams.ID.key(), panelId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, PanelDBAdaptor.QueryParams.ACL.key());
        QueryResult<DiseasePanel> queryResult = panelDBAdaptor.get(query, queryOptions);

        List<DiseasePanelAclEntry> aclList;
        if (queryResult != null && queryResult.getNumResults() == 1) {
            aclList = queryResult.first().getAcl();
        } else {
            aclList = Collections.emptyList();
        }
        return new QueryResult<>("get panel acl", queryResult.getDbTime(), aclList.size(), aclList.size(),
                queryResult.getWarningMsg(), queryResult.getErrorMsg(), aclList);
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> getPanelAcl(String userId, long panelId, String member) throws CatalogException {
        panelDBAdaptor.checkId(panelId);

        long studyId = panelDBAdaptor.getStudyId(panelId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkDiseasePanelPermission(panelId, userId, DiseasePanelAclEntry.DiseasePanelPermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            if (member.startsWith("@")) { //group
                // If the userId does not belong to the group...
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
                if (groupBelonging.getNumResults() != 1 || !groupBelonging.first().getName().equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            } else {
                // If the userId asking to see the permissions is not asking to see their own permissions
                if (!userId.equals(member)) {
                    throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                            + member);
                }
            }
        }

        List<String> members = new ArrayList<>(2);
        members.add(member);
        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
                members.add(groupBelonging.first().getName());
            }
        }

        return panelDBAdaptor.getAcl(panelId, members);
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> removePanelAcl(String userId, long panelId, String member) throws CatalogException {
        panelDBAdaptor.checkId(panelId);
        checkDiseasePanelPermission(panelId, userId, DiseasePanelAclEntry.DiseasePanelPermissions.SHARE);

        long studyId = panelDBAdaptor.getStudyId(panelId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Obtain the ACLs the member had
        QueryResult<DiseasePanelAclEntry> panelDBAdaptorAcl = panelDBAdaptor.getAcl(panelId, Arrays.asList(member));
        if (panelDBAdaptorAcl == null || panelDBAdaptorAcl.getNumResults() == 0) {
            throw new CatalogException("Could not remove the ACLs for " + member + ". It seems " + member + " did not have any ACLs "
                    + "defined");
        }

        panelDBAdaptor.removeAcl(panelId, member);

        panelDBAdaptorAcl.setId("Remove panel ACLs");
        return panelDBAdaptorAcl;
    }

    @Override
    public QueryResult<DiseasePanelAclEntry> updatePanelAcl(String userId, long panelId, String member, @Nullable String addPermissions,
                                                            @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException {
        panelDBAdaptor.checkId(panelId);
        checkDiseasePanelPermission(panelId, userId, DiseasePanelAclEntry.DiseasePanelPermissions.SHARE);

        long studyId = panelDBAdaptor.getStudyId(panelId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Check that the member has permissions
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.ID.key(), panelId)
                .append(PanelDBAdaptor.QueryParams.ACL_MEMBER.key(), member);
        QueryResult<Long> count = panelDBAdaptor.count(query);
        if (count == null || count.first() == 0) {
            throw new CatalogException("Could not update ACLs for " + member + ". It seems the member does not have any permissions set "
                    + "yet.");
        }

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(setPermissions.split(","));
            // Check if the permissions are correct
            checkPermissions(permissions, DiseasePanelAclEntry.DiseasePanelPermissions::valueOf);

            panelDBAdaptor.setAclsToMember(panelId, member, permissions);
        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(addPermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, DiseasePanelAclEntry.DiseasePanelPermissions::valueOf);

                panelDBAdaptor.addAclsToMember(panelId, member, permissions);
            }

            if (removePermissions != null) {
                permissions = Arrays.asList(removePermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, DiseasePanelAclEntry.DiseasePanelPermissions::valueOf);

                panelDBAdaptor.removeAclsFromMember(panelId, member, permissions);
            }
        }

        return panelDBAdaptor.getAcl(panelId, Arrays.asList(member));
    }

    /**
     * Checks whether any of the members already have any permission set for the particular document.
     *
     * @param studyId study id where the main id belongs to.
     * @param id id of the document that is going to be checked (file id, sample id, cohort id...)
     * @param members List of members (users or groups) that will be checked.
     * @param dbAdaptor Mongo db adaptor to make the mongo query.
     * @return a boolean indicating whether any of the members already have permissions.
     */
    private boolean anyMemberHasPermissions(long studyId, long id, List<String> members, AclDBAdaptor dbAdaptor)
            throws CatalogException {

        List<String> allMembers = new ArrayList<>(members.size());
        allMembers.addAll(members);

        for (String member : members) {
            if (member.startsWith("@")) { // It's a group
                // Obtain the users of the group
                QueryResult<Group> group = studyDBAdaptor.getGroup(studyId, member, Collections.emptyList());
                if (group != null && group.getNumResults() == 1) {
                    allMembers.addAll(group.first().getUserIds());
                }
            } else if (!member.equalsIgnoreCase("anonymous") && !member.equals("*")) { // It's a user id
                // Get the group where the user might belong to
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
                if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
                    allMembers.add(groupBelonging.first().getName());
                }
            }
        }

        return dbAdaptor.getAcl(id, allMembers).getNumResults() > 0;
    }

    @Override
    public boolean memberHasPermissionsInStudy(long studyId, String member) throws CatalogException {
        List<String> memberList = new ArrayList<>();
        memberList.add(member);
        if (!member.startsWith("@")) { // User
            if (member.equals("admin") || isStudyOwner(studyId, member)) {
                return true;
            }
            if (!member.equals("anonymous") && !member.equals("*")) {
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
                if (groupBelonging.getNumResults() > 0) {
                    memberList.add(groupBelonging.first().getName()); // Add the groupId to the memberList
                }
            }
        }
        StudyAclEntry studyAcl = getStudyAclBelonging(studyId, memberList);
        return studyAcl != null;
    }

    private boolean isStudyOwner(long studyId, String userId) throws CatalogDBException {
        return studyDBAdaptor.getOwnerId(studyId).equals(userId);
    }

    /**
     * This is like a mini cache where we will store the permissions for each path and user. This is useful for single queries trying to
     * fetch multiple files for example. This way, we only retrieve the permissions once while maintaining the authorization step for each
     * one.
     */
    static class StudyAuthenticationContext {
        private final long studyId;
        private final Map<String, Map<String, FileAclEntry>> pathUserAclMap;

        StudyAuthenticationContext(long studyId) {
            this.studyId = studyId;
            pathUserAclMap = new HashMap<>();
        }
    }

    /**
     * Return the Acls for the user, group (if any) and {@link #OTHER_USERS_ID}.
     *
     * @param studyAuthenticationContext Context with already fetched elements. Will avoid fetch extra data
     * @param userId                     User id
     * @param studyId                    Study identifier
     * @param groupId                    User belonging group. May be null.
     * @param paths                      List of paths to check
     * @return Map (Path -> Map (UserId -> FileAcl) )
     * @throws CatalogDBException
     */
    private Map<String, Map<String, FileAclEntry>> getFileAcls(StudyAuthenticationContext studyAuthenticationContext, String userId,
                                                               long studyId, String groupId, List<String> paths) throws CatalogDBException {

        // Make a copy of the pathsClone
        List<String> pathsClone = new ArrayList<>(paths.size());
        for (String path : paths) {
            pathsClone.add(path);
        }

        // We first remove the pathsClone for which we already have the ACL
        for (Iterator<String> iterator = pathsClone.iterator(); iterator.hasNext();) {
            String path = iterator.next();
            if (studyAuthenticationContext.pathUserAclMap.containsKey(path)) {
                Map<String, FileAclEntry> userAclMap = studyAuthenticationContext.pathUserAclMap.get(path);
                if (userAclMap.containsKey(userId) && (groupId == null || userAclMap.containsKey(groupId))
                        && userAclMap.containsKey(OTHER_USERS_ID)) {
                    iterator.remove();
                }
            }
        }

        // We obtain the Acls for the paths for which we still don't have them.
        if (!pathsClone.isEmpty()) {
            // We make a query to obtain the ACLs of all the pathsClone for userId, groupId and *
            List<String> userIds = (groupId == null)
                    ? Arrays.asList(userId, OTHER_USERS_ID)
                    : Arrays.asList(userId, groupId, OTHER_USERS_ID);
            Map<String, Map<String, FileAclEntry>> map = fileDBAdaptor.getAcls(studyId, pathsClone, userIds).first();
            for (String path : pathsClone) {
                Map<String, FileAclEntry> stringAclEntryMap;
                if (map.containsKey(path)) {
                    stringAclEntryMap = map.get(path);
                } else {
                    stringAclEntryMap = new HashMap<>();
                }
                stringAclEntryMap.putIfAbsent(userId, null);
                if (groupId != null) {
                    stringAclEntryMap.putIfAbsent(groupId, null);
                }
                stringAclEntryMap.putIfAbsent(OTHER_USERS_ID, null);

                if (studyAuthenticationContext.pathUserAclMap.containsKey(path)) {
                    studyAuthenticationContext.pathUserAclMap.get(path).putAll(stringAclEntryMap);
                } else {
                    studyAuthenticationContext.pathUserAclMap.put(path, stringAclEntryMap);
                }
            }
        }

        return studyAuthenticationContext.pathUserAclMap;
    }


    /*
    ====================================
    Auxiliar methods
    ====================================
     */
    /**
     * Retrieves the groupId where the members belongs to.
     *
     * @param studyId study id.
     * @param members List of user ids.
     * @return the group id of the user. Null if the user does not take part of any group.
     * @throws CatalogException when there is any database error.
     */
    QueryResult<Group> getGroupBelonging(long studyId, List<String> members) throws CatalogException {
        return studyDBAdaptor.getGroup(studyId, null, members);
    }
    QueryResult<Group> getGroupBelonging(long studyId, String members) throws CatalogException {
        return getGroupBelonging(studyId, Arrays.asList(members.split(",")));
    }

    /**
     * Retrieves the StudyAcl where the user/group belongs to.
     *
     * @param studyId study id.
     * @param userId user id.
     * @param groupId group id. This can be null.
     * @return the studyAcl where the user/group belongs to.
     * @throws CatalogException when there is any database error.
     */
    StudyAclEntry getStudyAclBelonging(long studyId, String userId, @Nullable String groupId) throws CatalogException {
        List<String> members = groupId != null ? Arrays.asList(userId, groupId) : Arrays.asList(userId);
        return getStudyAclBelonging(studyId, members);
    }

    /**
     * Retrieves the studyAcl for the members.
     *
     * @param studyId study id.
     * @param members Might be one user, one group or one user and the group where the user belongs to.
     * @return the studyAcl of the user/group.
     * @throws CatalogException when there is a database error.
     */
    StudyAclEntry getStudyAclBelonging(long studyId, List<String> members) throws CatalogException {
        QueryResult<StudyAclEntry> studyQueryResult = studyDBAdaptor.getAcl(studyId, members);
        if (studyQueryResult.getNumResults() > 0) {
            return studyQueryResult.first();
        }
        return null;
    }

    public static void checkPermissions(List<String> permissions, Function<String, Enum> getValue) throws CatalogException {
        for (String permission : permissions) {
            try {
                getValue.apply(permission);
            } catch (IllegalArgumentException e) {
                throw new CatalogException("The permission " + permission + " is not a correct permission.");
            }
        }
    }

    /*
    ====================================
    ACL transformation methods
    ====================================
     */
    private FileAclEntry transformStudyAclToFileAcl(StudyAclEntry studyAcl) {
        FileAclEntry fileAcl = new FileAclEntry(null, Collections.emptyList());
        if (studyAcl == null) {
            return fileAcl;
        }

        fileAcl.setMember(studyAcl.getMember());
        EnumSet<StudyAclEntry.StudyPermissions> studyPermissions = studyAcl.getPermissions();
        EnumSet<FileAclEntry.FilePermissions> filePermissions = EnumSet.noneOf(FileAclEntry.FilePermissions.class);

        for (StudyAclEntry.StudyPermissions studyPermission : studyPermissions) {
            FileAclEntry.FilePermissions aux = studyPermission.getFilePermission();
            if (aux != null) {
                filePermissions.add(aux);
            }
        }
        fileAcl.setPermissions(filePermissions);
        return fileAcl;
    }

    private SampleAclEntry transformStudyAclToSampleAcl(StudyAclEntry studyAcl) {
        SampleAclEntry sampleAcl = new SampleAclEntry(null, Collections.emptyList());
        if (studyAcl == null) {
            return sampleAcl;
        }

        sampleAcl.setMember(studyAcl.getMember());
        EnumSet<StudyAclEntry.StudyPermissions> studyPermissions = studyAcl.getPermissions();
        EnumSet<SampleAclEntry.SamplePermissions> samplePermission = EnumSet.noneOf(SampleAclEntry.SamplePermissions.class);

        for (StudyAclEntry.StudyPermissions studyPermission : studyPermissions) {
            SampleAclEntry.SamplePermissions aux = studyPermission.getSamplePermission();
            if (aux != null) {
                samplePermission.add(aux);
            }
        }
        sampleAcl.setPermissions(samplePermission);
        return sampleAcl;
    }

    private IndividualAclEntry transformStudyAclToIndividualAcl(StudyAclEntry studyAcl) {
        IndividualAclEntry individualAcl = new IndividualAclEntry(null, Collections.emptyList());
        if (studyAcl == null) {
            return individualAcl;
        }

        individualAcl.setMember(studyAcl.getMember());
        EnumSet<StudyAclEntry.StudyPermissions> studyPermissions = studyAcl.getPermissions();
        EnumSet<IndividualAclEntry.IndividualPermissions> individualPermissions =
                EnumSet.noneOf(IndividualAclEntry.IndividualPermissions.class);

        for (StudyAclEntry.StudyPermissions studyPermission : studyPermissions) {
            IndividualAclEntry.IndividualPermissions aux = studyPermission.getIndividualPermission();
            if (aux != null) {
                individualPermissions.add(aux);
            }
        }
        individualAcl.setPermissions(individualPermissions);
        return individualAcl;
    }

    private JobAclEntry transformStudyAclToJobAcl(StudyAclEntry studyAcl) {
        JobAclEntry jobAcl = new JobAclEntry(null, Collections.emptyList());
        if (studyAcl == null) {
            return jobAcl;
        }

        jobAcl.setMember(studyAcl.getMember());
        EnumSet<StudyAclEntry.StudyPermissions> studyPermissions = studyAcl.getPermissions();
        EnumSet<JobAclEntry.JobPermissions> jobPermissions = EnumSet.noneOf(JobAclEntry.JobPermissions.class);

        for (StudyAclEntry.StudyPermissions studyPermission : studyPermissions) {
            JobAclEntry.JobPermissions aux = studyPermission.getJobPermission();
            if (aux != null) {
                jobPermissions.add(aux);
            }
        }
        jobAcl.setPermissions(jobPermissions);
        return jobAcl;
    }

    private CohortAclEntry transformStudyAclToCohortAcl(StudyAclEntry studyAcl) {
        CohortAclEntry cohortAcl = new CohortAclEntry(null, Collections.emptyList());
        if (studyAcl == null) {
            return cohortAcl;
        }

        cohortAcl.setMember(studyAcl.getMember());
        EnumSet<StudyAclEntry.StudyPermissions> studyPermissions = studyAcl.getPermissions();
        EnumSet<CohortAclEntry.CohortPermissions> cohortPermissions = EnumSet.noneOf(CohortAclEntry.CohortPermissions.class);

        for (StudyAclEntry.StudyPermissions studyPermission : studyPermissions) {
            CohortAclEntry.CohortPermissions aux = studyPermission.getCohortPermission();
            if (aux != null) {
                cohortPermissions.add(aux);
            }
        }
        cohortAcl.setPermissions(cohortPermissions);
        return cohortAcl;
    }

    private DatasetAclEntry transformStudyAclToDatasetAcl(StudyAclEntry studyAcl) {
        DatasetAclEntry datasetAcl = new DatasetAclEntry(null, Collections.emptyList());
        if (studyAcl == null) {
            return datasetAcl;
        }

        datasetAcl.setMember(studyAcl.getMember());
        EnumSet<StudyAclEntry.StudyPermissions> studyPermissions = studyAcl.getPermissions();
        EnumSet<DatasetAclEntry.DatasetPermissions> datasetPermissions = EnumSet.noneOf(DatasetAclEntry.DatasetPermissions.class);

        for (StudyAclEntry.StudyPermissions studyPermission : studyPermissions) {
            DatasetAclEntry.DatasetPermissions aux = studyPermission.getDatasetPermission();
            if (aux != null) {
                datasetPermissions.add(aux);
            }
        }
        datasetAcl.setPermissions(datasetPermissions);
        return datasetAcl;
    }

    private DiseasePanelAclEntry transformStudyAclToDiseasePanelAcl(StudyAclEntry studyAcl) {
        DiseasePanelAclEntry panelAcl = new DiseasePanelAclEntry(null, Collections.emptyList());
        if (studyAcl == null) {
            return panelAcl;
        }

        panelAcl.setMember(studyAcl.getMember());
        EnumSet<StudyAclEntry.StudyPermissions> studyPermissions = studyAcl.getPermissions();
        EnumSet<DiseasePanelAclEntry.DiseasePanelPermissions> datasetPermissions =
                EnumSet.noneOf(DiseasePanelAclEntry.DiseasePanelPermissions.class);

        for (StudyAclEntry.StudyPermissions studyPermission : studyPermissions) {
            DiseasePanelAclEntry.DiseasePanelPermissions aux = studyPermission.getDiseasePanelPermission();
            if (aux != null) {
                datasetPermissions.add(aux);
            }
        }
        panelAcl.setPermissions(datasetPermissions);
        return panelAcl;
    }
}
