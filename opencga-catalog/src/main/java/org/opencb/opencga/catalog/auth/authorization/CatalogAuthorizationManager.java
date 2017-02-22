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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.AclDBAdaptor;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
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

    private final String ADMIN = "admin";
    private final String ANONYMOUS = "anonymous";

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
        if (userId.equals(ADMIN)) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList(ADMIN));
            if (studyAclQueryResult.getNumResults() == 1) {
                studyAcl = studyAclQueryResult.first();
            }
        } else {
            String groupId = null;
            if (!userId.equalsIgnoreCase(ANONYMOUS)) {
                QueryResult<Group> group = getGroupBelonging(studyId, userId);
                groupId = group.getNumResults() == 1 ? group.first().getName() : null;
            }
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
        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        FileAclEntry fileAcl = null;
        if (userId.equals(ADMIN)) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList(ADMIN));
            if (studyAclQueryResult.getNumResults() == 1) {
                fileAcl = transformStudyAclToFileAcl(studyAclQueryResult.first());
            }
        } else {
            fileAcl = resolveFilePermissions(studyId, fileId, userId);
        }

        if (!fileAcl.getPermissions().contains(permission)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "File", fileId, null);
        }
    }

    private FileAclEntry resolveFilePermissions(long studyId, File file, String userId) throws CatalogException {
        if (file.getAcl() == null) {
            return resolveFilePermissions(studyId, file.getId(), userId);
        } else {
            String groupId = null;
            if (!userId.equalsIgnoreCase(ANONYMOUS)) {
                QueryResult<Group> group = getGroupBelonging(studyId, userId);
                groupId = group.getNumResults() == 1 ? group.first().getName() : null;
            }

            Map<String, FileAclEntry> userAclMap = new HashMap<>();
            for (FileAclEntry fileAcl : file.getAcl()) {
                userAclMap.put(fileAcl.getMember(), fileAcl);
            }
            return resolveFilePermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private FileAclEntry resolveFilePermissions(long studyId, long fileId, String userId) throws CatalogException {
        String groupId = null;
        if (!userId.equalsIgnoreCase(ANONYMOUS)) {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            groupId = group.getNumResults() == 1 ? group.first().getName() : null;
        }

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID, ANONYMOUS)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID, ANONYMOUS);
        List<FileAclEntry> fileAclEntryList = fileDBAdaptor.getAcl(fileId, userIds).getResult();

        Map<String, FileAclEntry> userAclMap = new HashMap<>();
        for (FileAclEntry fileAclEntry : fileAclEntryList) {
            userAclMap.put(fileAclEntry.getMember(), fileAclEntry);
        }

        return resolveFilePermissions(studyId, userId, groupId, userAclMap);
    }

    private FileAclEntry resolveFilePermissions(long studyId, String userId, String groupId, Map<String, FileAclEntry> userAclMap)
            throws CatalogException {
        if (userId.equals(ANONYMOUS)) {
            if (userAclMap.containsKey(userId)) {
                return userAclMap.get(userId);
            } else {
                return transformStudyAclToFileAcl(getStudyAclBelonging(studyId, userId, groupId));
            }
        }

        // Registered user
        EnumSet<FileAclEntry.FilePermissions> permissions = EnumSet.noneOf(FileAclEntry.FilePermissions.class);
        boolean flagPermissionFound = false;

        if (userAclMap.containsKey(userId)) {
            permissions.addAll(userAclMap.get(userId).getPermissions());
            flagPermissionFound = true;
        }
        if (StringUtils.isNotEmpty(groupId) && userAclMap.containsKey(groupId)) {
            permissions.addAll(userAclMap.get(groupId).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(ANONYMOUS)) {
            permissions.addAll(userAclMap.get(ANONYMOUS).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(OTHER_USERS_ID)) {
            permissions.addAll(userAclMap.get(OTHER_USERS_ID).getPermissions());
            flagPermissionFound = true;
        }

        if (flagPermissionFound) {
            return new FileAclEntry(userId, permissions);
        } else {
            return transformStudyAclToFileAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void checkSamplePermission(long sampleId, String userId, SampleAclEntry.SamplePermissions permission) throws CatalogException {
        long studyId = sampleDBAdaptor.getStudyId(sampleId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        SampleAclEntry sampleAcl = null;
        if (userId.equals(ADMIN)) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList(ADMIN));
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
            String groupId = null;
            if (!userId.equalsIgnoreCase(ANONYMOUS)) {
                QueryResult<Group> group = getGroupBelonging(studyId, userId);
                groupId = group.getNumResults() == 1 ? group.first().getName() : null;
            }

            Map<String, SampleAclEntry> userAclMap = new HashMap<>();
            for (SampleAclEntry sampleAcl : sample.getAcl()) {
                userAclMap.put(sampleAcl.getMember(), sampleAcl);
            }
            return resolveSamplePermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private SampleAclEntry resolveSamplePermissions(long studyId, long sampleId, String userId) throws CatalogException {
        String groupId = null;
        if (!userId.equalsIgnoreCase(ANONYMOUS)) {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            groupId = group.getNumResults() == 1 ? group.first().getName() : null;
        }

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID, ANONYMOUS)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID, ANONYMOUS);
        List<SampleAclEntry> sampleAclList = sampleDBAdaptor.getAcl(sampleId, userIds).getResult();

        Map<String, SampleAclEntry> userAclMap = new HashMap<>();
        for (SampleAclEntry sampleAcl : sampleAclList) {
            userAclMap.put(sampleAcl.getMember(), sampleAcl);
        }

        return resolveSamplePermissions(studyId, userId, groupId, userAclMap);
    }

    private SampleAclEntry resolveSamplePermissions(long studyId, String userId, String groupId, Map<String, SampleAclEntry> userAclMap)
            throws CatalogException {
        if (userId.equals(ANONYMOUS)) {
            if (userAclMap.containsKey(userId)) {
                return userAclMap.get(userId);
            } else {
                return transformStudyAclToSampleAcl(getStudyAclBelonging(studyId, userId, groupId));
            }
        }

        // Registered user
        EnumSet<SampleAclEntry.SamplePermissions> permissions = EnumSet.noneOf(SampleAclEntry.SamplePermissions.class);
        boolean flagPermissionFound = false;

        if (userAclMap.containsKey(userId)) {
            permissions.addAll(userAclMap.get(userId).getPermissions());
            flagPermissionFound = true;
        }
        if (StringUtils.isNotEmpty(groupId) && userAclMap.containsKey(groupId)) {
            permissions.addAll(userAclMap.get(groupId).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(ANONYMOUS)) {
            permissions.addAll(userAclMap.get(ANONYMOUS).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(OTHER_USERS_ID)) {
            permissions.addAll(userAclMap.get(OTHER_USERS_ID).getPermissions());
            flagPermissionFound = true;
        }

        if (flagPermissionFound) {
            return new SampleAclEntry(userId, permissions);
        } else {
            return transformStudyAclToSampleAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void checkIndividualPermission(long individualId, String userId, IndividualAclEntry.IndividualPermissions permission)
            throws CatalogException {
        long studyId = individualDBAdaptor.getStudyId(individualId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        IndividualAclEntry individualAcl = null;
        if (userId.equals(ADMIN)) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList(ADMIN));
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
            String groupId = null;
            if (!userId.equalsIgnoreCase(ANONYMOUS)) {
                QueryResult<Group> group = getGroupBelonging(studyId, userId);
                groupId = group.getNumResults() == 1 ? group.first().getName() : null;
            }

            Map<String, IndividualAclEntry> userAclMap = new HashMap<>();
            for (IndividualAclEntry individualAcl : individual.getAcl()) {
                userAclMap.put(individualAcl.getMember(), individualAcl);
            }
            return resolveIndividualPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private IndividualAclEntry resolveIndividualPermissions(long studyId, long individualId, String userId) throws CatalogException {
        String groupId = null;
        if (!userId.equalsIgnoreCase(ANONYMOUS)) {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            groupId = group.getNumResults() == 1 ? group.first().getName() : null;
        }

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID, ANONYMOUS)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID, ANONYMOUS);
        List<IndividualAclEntry> individualAcls = individualDBAdaptor.getAcl(individualId, userIds).getResult();

        Map<String, IndividualAclEntry> userAclMap = new HashMap<>();
        for (IndividualAclEntry individualAcl : individualAcls) {
            userAclMap.put(individualAcl.getMember(), individualAcl);
        }

        return resolveIndividualPermissions(studyId, userId, groupId, userAclMap);
    }

    private IndividualAclEntry resolveIndividualPermissions(long studyId, String userId, String groupId, Map<String,
            IndividualAclEntry> userAclMap) throws CatalogException {
        if (userId.equals(ANONYMOUS)) {
            if (userAclMap.containsKey(userId)) {
                return userAclMap.get(userId);
            } else {
                return transformStudyAclToIndividualAcl(getStudyAclBelonging(studyId, userId, groupId));
            }
        }

        // Registered user
        EnumSet<IndividualAclEntry.IndividualPermissions> permissions = EnumSet.noneOf(IndividualAclEntry.IndividualPermissions.class);
        boolean flagPermissionFound = false;

        if (userAclMap.containsKey(userId)) {
            permissions.addAll(userAclMap.get(userId).getPermissions());
            flagPermissionFound = true;
        }
        if (StringUtils.isNotEmpty(groupId) && userAclMap.containsKey(groupId)) {
            permissions.addAll(userAclMap.get(groupId).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(ANONYMOUS)) {
            permissions.addAll(userAclMap.get(ANONYMOUS).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(OTHER_USERS_ID)) {
            permissions.addAll(userAclMap.get(OTHER_USERS_ID).getPermissions());
            flagPermissionFound = true;
        }

        if (flagPermissionFound) {
            return new IndividualAclEntry(userId, permissions);
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
        if (userId.equals(ADMIN)) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList(ADMIN));
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
            String groupId = null;
            if (!userId.equalsIgnoreCase(ANONYMOUS)) {
                QueryResult<Group> group = getGroupBelonging(studyId, userId);
                groupId = group.getNumResults() == 1 ? group.first().getName() : null;
            }

            Map<String, JobAclEntry> userAclMap = new HashMap<>();
            for (JobAclEntry jobAcl : job.getAcl()) {
                userAclMap.put(jobAcl.getMember(), jobAcl);
            }
            return resolveJobPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private JobAclEntry resolveJobPermissions(long studyId, long jobId, String userId) throws CatalogException {
        String groupId = null;
        if (!userId.equalsIgnoreCase(ANONYMOUS)) {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            groupId = group.getNumResults() == 1 ? group.first().getName() : null;
        }

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID, ANONYMOUS)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID, ANONYMOUS);
        List<JobAclEntry> jobAcls = jobDBAdaptor.getAcl(jobId, userIds).getResult();

        Map<String, JobAclEntry> userAclMap = new HashMap<>();
        for (JobAclEntry jobAcl : jobAcls) {
            userAclMap.put(jobAcl.getMember(), jobAcl);
        }

        return resolveJobPermissions(studyId, userId, groupId, userAclMap);
    }

    private JobAclEntry resolveJobPermissions(long studyId, String userId, String groupId, Map<String, JobAclEntry> userAclMap)
            throws CatalogException {
        if (userId.equals(ANONYMOUS)) {
            if (userAclMap.containsKey(userId)) {
                return userAclMap.get(userId);
            } else {
                return transformStudyAclToJobAcl(getStudyAclBelonging(studyId, userId, groupId));
            }
        }

        // Registered user
        EnumSet<JobAclEntry.JobPermissions> permissions = EnumSet.noneOf(JobAclEntry.JobPermissions.class);
        boolean flagPermissionFound = false;

        if (userAclMap.containsKey(userId)) {
            permissions.addAll(userAclMap.get(userId).getPermissions());
            flagPermissionFound = true;
        }
        if (StringUtils.isNotEmpty(groupId) && userAclMap.containsKey(groupId)) {
            permissions.addAll(userAclMap.get(groupId).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(ANONYMOUS)) {
            permissions.addAll(userAclMap.get(ANONYMOUS).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(OTHER_USERS_ID)) {
            permissions.addAll(userAclMap.get(OTHER_USERS_ID).getPermissions());
            flagPermissionFound = true;
        }

        if (flagPermissionFound) {
            return new JobAclEntry(userId, permissions);
        } else {
            return transformStudyAclToJobAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void checkCohortPermission(long cohortId, String userId, CohortAclEntry.CohortPermissions permission) throws CatalogException {
        long studyId = cohortDBAdaptor.getStudyId(cohortId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        CohortAclEntry cohortAcl = null;
        if (userId.equals(ADMIN)) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList(ADMIN));
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
            String groupId = null;
            if (!userId.equalsIgnoreCase(ANONYMOUS)) {
                QueryResult<Group> group = getGroupBelonging(studyId, userId);
                groupId = group.getNumResults() == 1 ? group.first().getName() : null;
            }

            Map<String, CohortAclEntry> userAclMap = new HashMap<>();
            for (CohortAclEntry cohortAcl : cohort.getAcl()) {
                userAclMap.put(cohortAcl.getMember(), cohortAcl);
            }
            return resolveCohortPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private CohortAclEntry resolveCohortPermissions(long studyId, long cohortId, String userId) throws CatalogException {
        String groupId = null;
        if (!userId.equalsIgnoreCase(ANONYMOUS)) {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            groupId = group.getNumResults() == 1 ? group.first().getName() : null;
        }

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID, ANONYMOUS)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID, ANONYMOUS);
        List<CohortAclEntry> cohortAcls = cohortDBAdaptor.getAcl(cohortId, userIds).getResult();

        Map<String, CohortAclEntry> userAclMap = new HashMap<>();
        for (CohortAclEntry cohortAcl : cohortAcls) {
            userAclMap.put(cohortAcl.getMember(), cohortAcl);
        }

        return resolveCohortPermissions(studyId, userId, groupId, userAclMap);
    }

    private CohortAclEntry resolveCohortPermissions(long studyId, String userId, String groupId, Map<String, CohortAclEntry> userAclMap)
            throws CatalogException {
        if (userId.equals(ANONYMOUS)) {
            if (userAclMap.containsKey(userId)) {
                return userAclMap.get(userId);
            } else {
                return transformStudyAclToCohortAcl(getStudyAclBelonging(studyId, userId, groupId));
            }
        }

        // Registered user
        EnumSet<CohortAclEntry.CohortPermissions> permissions = EnumSet.noneOf(CohortAclEntry.CohortPermissions.class);
        boolean flagPermissionFound = false;

        if (userAclMap.containsKey(userId)) {
            permissions.addAll(userAclMap.get(userId).getPermissions());
            flagPermissionFound = true;
        }
        if (StringUtils.isNotEmpty(groupId) && userAclMap.containsKey(groupId)) {
            permissions.addAll(userAclMap.get(groupId).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(ANONYMOUS)) {
            permissions.addAll(userAclMap.get(ANONYMOUS).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(OTHER_USERS_ID)) {
            permissions.addAll(userAclMap.get(OTHER_USERS_ID).getPermissions());
            flagPermissionFound = true;
        }

        if (flagPermissionFound) {
            return new CohortAclEntry(userId, permissions);
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
        if (userId.equals(ADMIN)) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList(ADMIN));
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
            String groupId = null;
            if (!userId.equalsIgnoreCase(ANONYMOUS)) {
                QueryResult<Group> group = getGroupBelonging(studyId, userId);
                groupId = group.getNumResults() == 1 ? group.first().getName() : null;
            }

            Map<String, DatasetAclEntry> userAclMap = new HashMap<>();
            for (DatasetAclEntry datasetAcl : dataset.getAcl()) {
                userAclMap.put(datasetAcl.getMember(), datasetAcl);
            }
            return resolveDatasetPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private DatasetAclEntry resolveDatasetPermissions(long studyId, long datasetId, String userId) throws CatalogException {
        String groupId = null;
        if (!userId.equalsIgnoreCase(ANONYMOUS)) {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            groupId = group.getNumResults() == 1 ? group.first().getName() : null;
        }

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID, ANONYMOUS)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID, ANONYMOUS);
        List<DatasetAclEntry> datasetAcls = datasetDBAdaptor.getAcl(datasetId, userIds).getResult();

        Map<String, DatasetAclEntry> userAclMap = new HashMap<>();
        for (DatasetAclEntry datasetAcl : datasetAcls) {
            userAclMap.put(datasetAcl.getMember(), datasetAcl);
        }

        return resolveDatasetPermissions(studyId, userId, groupId, userAclMap);
    }

    private DatasetAclEntry resolveDatasetPermissions(long studyId, String userId, String groupId, Map<String, DatasetAclEntry> userAclMap)
            throws CatalogException {
        if (userId.equals(ANONYMOUS)) {
            if (userAclMap.containsKey(userId)) {
                return userAclMap.get(userId);
            } else {
                return transformStudyAclToDatasetAcl(getStudyAclBelonging(studyId, userId, groupId));
            }
        }

        // Registered user
        EnumSet<DatasetAclEntry.DatasetPermissions> permissions = EnumSet.noneOf(DatasetAclEntry.DatasetPermissions.class);
        boolean flagPermissionFound = false;

        if (userAclMap.containsKey(userId)) {
            permissions.addAll(userAclMap.get(userId).getPermissions());
            flagPermissionFound = true;
        }
        if (StringUtils.isNotEmpty(groupId) && userAclMap.containsKey(groupId)) {
            permissions.addAll(userAclMap.get(groupId).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(ANONYMOUS)) {
            permissions.addAll(userAclMap.get(ANONYMOUS).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(OTHER_USERS_ID)) {
            permissions.addAll(userAclMap.get(OTHER_USERS_ID).getPermissions());
            flagPermissionFound = true;
        }

        if (flagPermissionFound) {
            return new DatasetAclEntry(userId, permissions);
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
        if (userId.equals(ADMIN)) {
            QueryResult<StudyAclEntry> studyAclQueryResult = metaDBAdaptor.getDaemonAcl(Arrays.asList(ADMIN));
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
            String groupId = null;
            if (!userId.equalsIgnoreCase(ANONYMOUS)) {
                QueryResult<Group> group = getGroupBelonging(studyId, userId);
                groupId = group.getNumResults() == 1 ? group.first().getName() : null;
            }

            Map<String, DiseasePanelAclEntry> userAclMap = new HashMap<>();
            for (DiseasePanelAclEntry panelAcl : panel.getAcl()) {
                userAclMap.put(panelAcl.getMember(), panelAcl);
            }
            return resolveDiseasePanelPermissions(studyId, userId, groupId, userAclMap);
        }
    }

    private DiseasePanelAclEntry resolveDiseasePanelPermissions(long studyId, long panelId, String userId) throws CatalogException {
        String groupId = null;
        if (!userId.equalsIgnoreCase(ANONYMOUS)) {
            QueryResult<Group> group = getGroupBelonging(studyId, userId);
            groupId = group.getNumResults() == 1 ? group.first().getName() : null;
        }

        List<String> userIds = (groupId == null)
                ? Arrays.asList(userId, OTHER_USERS_ID, ANONYMOUS)
                : Arrays.asList(userId, groupId, OTHER_USERS_ID, ANONYMOUS);
        List<DiseasePanelAclEntry> panelAcls = panelDBAdaptor.getAcl(panelId, userIds).getResult();

        Map<String, DiseasePanelAclEntry> userAclMap = new HashMap<>();
        for (DiseasePanelAclEntry panelAcl : panelAcls) {
            userAclMap.put(panelAcl.getMember(), panelAcl);
        }

        return resolveDiseasePanelPermissions(studyId, userId, groupId, userAclMap);
    }

    private DiseasePanelAclEntry resolveDiseasePanelPermissions(long studyId, String userId, String groupId,
                                                                Map<String, DiseasePanelAclEntry> userAclMap) throws CatalogException {
        if (userId.equals(ANONYMOUS)) {
            if (userAclMap.containsKey(userId)) {
                return userAclMap.get(userId);
            } else {
                return transformStudyAclToDiseasePanelAcl(getStudyAclBelonging(studyId, userId, groupId));
            }
        }

        // Registered user
        EnumSet<DiseasePanelAclEntry.DiseasePanelPermissions> permissions =
                EnumSet.noneOf(DiseasePanelAclEntry.DiseasePanelPermissions.class);
        boolean flagPermissionFound = false;

        if (userAclMap.containsKey(userId)) {
            permissions.addAll(userAclMap.get(userId).getPermissions());
            flagPermissionFound = true;
        }
        if (StringUtils.isNotEmpty(groupId) && userAclMap.containsKey(groupId)) {
            permissions.addAll(userAclMap.get(groupId).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(ANONYMOUS)) {
            permissions.addAll(userAclMap.get(ANONYMOUS).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(OTHER_USERS_ID)) {
            permissions.addAll(userAclMap.get(OTHER_USERS_ID).getPermissions());
            flagPermissionFound = true;
        }

        if (flagPermissionFound) {
            return new DiseasePanelAclEntry(userId, permissions);
        } else {
            return transformStudyAclToDiseasePanelAcl(getStudyAclBelonging(studyId, userId, groupId));
        }
    }

    @Override
    public void filterProjects(String userId, List<Project> projects) throws CatalogException {
        if (projects == null || projects.isEmpty()) {
            return;
        }
        if (userId.equals(ADMIN)) {
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
        if (userId.equals(ADMIN)) {
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
            filterFiles(userId, study.getId(), study.getFiles());
            filterSamples(userId, study.getId(), study.getSamples());
            filterJobs(userId, study.getId(), study.getJobs());
            filterCohorts(userId, study.getId(), study.getCohorts());
            filterIndividuals(userId, study.getId(), study.getIndividuals());
            filterDatasets(userId, study.getId(), study.getDatasets());
        }
    }

    @Override
    public void filterFiles(String userId, long studyId, List<File> files) throws CatalogException {
        if (files == null || files.isEmpty()) {
            return;
        }
        if (userId.equals(ADMIN)) {
            return;
        }
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        Iterator<File> fileIt = files.iterator();
        while (fileIt.hasNext()) {
            File file = fileIt.next();
            FileAclEntry fileAcl = resolveFilePermissions(studyId, file, userId);
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
        if (userId.equals(ADMIN)) {
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
        if (userId.equals(ADMIN)) {
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
        if (userId.equals(ADMIN)) {
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
        if (userId.equals(ADMIN)) {
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
        if (userId.equals(ADMIN)) {
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
            if (template.equals(AuthorizationManager.ROLE_ADMIN)) {
                studyPermissions = AuthorizationManager.getAdminAcls();
            } else if (template.equals(AuthorizationManager.ROLE_ANALYST)) {
                studyPermissions = AuthorizationManager.getAnalystAcls();
            } else if (template.equals(AuthorizationManager.ROLE_VIEW_ONLY)) {
                studyPermissions = AuthorizationManager.getViewOnlyAcls();
            }
        }
        // Add the permissions present in permissions
        studyPermissions.addAll(permissions.stream().map(StudyAclEntry.StudyPermissions::valueOf).collect(Collectors.toList()));

        // If the user already has permissions set, we cannot create a new set of permissions
        List<StudyAclEntry> studyAclList = new ArrayList<>(members.size());
        for (String member : members) {
            if (memberExists(studyId, member)) {
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

    private boolean memberExists(long studyId, String member) throws CatalogDBException {
        QueryResult<StudyAclEntry> acl = studyDBAdaptor.getAcl(studyId, Arrays.asList(member));
        return acl.getNumResults() > 0;
    }

    @Override
    public QueryResult<StudyAclEntry> getAllStudyAcls(String userId, long studyId) throws CatalogException {
        studyDBAdaptor.checkId(studyId);
//        checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);

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
        checkUpdateParams(addPermissions, removePermissions, setPermissions);

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
            if (!member.equals("*") && !member.equals("anonymous") && !memberExists(studyId, member)) {
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
//        checkSamplePermission(sampleId, userId, SampleAclEntry.SamplePermissions.SHARE);

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

        return sampleDBAdaptor.getAcl(sampleId, members);
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
        checkUpdateParams(addPermissions, removePermissions, setPermissions);

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
    public List<QueryResult<FileAclEntry>> createFileAcls(AbstractManager.MyResourceIds resourceIds, List<String> members,
                                                          List<String> permissions) throws CatalogException {
        checkPermissions(permissions, FileAclEntry.FilePermissions::valueOf);

        String userId = resourceIds.getUser();
        long studyId = resourceIds.getStudyId();

        // Check all the members are valid members
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check that all the members have permissions defined at the study level
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberExists(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        List<QueryResult<FileAclEntry>> retQueryResultList = new ArrayList<>(resourceIds.getResourceIds().size());

        // We create the list of permissions
        List<FileAclEntry> fileAclEntryList = new ArrayList<>(permissions.size());
        for (String member : members) {
            fileAclEntryList.add(new FileAclEntry(member, permissions));
        }

        for (Long fileId : resourceIds.getResourceIds()) {
            try {
                // Check if the userId has proper permissions for all the files.
                checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.SHARE);

                // Check if any of the members already have permissions set in the file
                if (anyMemberHasPermissions(studyId, fileId, members, fileDBAdaptor)) {
                    throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                            + "particular file. Please, use update instead.");
                }

                QueryResult<File> fileQueryResult = fileDBAdaptor.get(fileId, new QueryOptions(QueryOptions.INCLUDE,
                        Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.TYPE.key())));
                if (fileQueryResult.getNumResults() != 1) {
                    throw new CatalogException("An unexpected error occured. File " + fileId + " not found when trying to create new ACLs");
                }

                // We create those ACLs for the whole path contained (propagate the acls in case of a directory)
                Query query = new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                if (fileQueryResult.first().getType().equals(File.Type.FILE)) {
                    query.append(FileDBAdaptor.QueryParams.ID.key(), fileId);
                } else {
                    query.append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + fileQueryResult.first().getPath());
                }
                fileDBAdaptor.createAcl(query, fileAclEntryList);

                QueryResult<FileAclEntry> aclEntryQueryResult = fileDBAdaptor.getAcl(fileId, members);
                aclEntryQueryResult.setId("Create file ACLs");
                retQueryResultList.add(aclEntryQueryResult);

            } catch (CatalogException e) {
                QueryResult<FileAclEntry> queryResult = new QueryResult<>();
                queryResult.setErrorMsg(e.getMessage());
                queryResult.setId("Create file ACLs");

                retQueryResultList.add(queryResult);
            }
        }

        return retQueryResultList;
    }

    @Override
    public QueryResult<FileAclEntry> getAllFileAcls(String userId, long fileId) throws CatalogException {
        fileDBAdaptor.checkId(fileId);
        // Check if the userId has proper permissions for all the samples.
//        checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.SHARE);

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
    public List<QueryResult<FileAclEntry>> removeFileAcls(AbstractManager.MyResourceIds resources, List<String> members)
            throws CatalogException {
        // Check the member is valid
        checkMembers(dbAdaptorFactory, resources.getStudyId(), members);

        List<QueryResult<FileAclEntry>> retQueryResultList = new ArrayList<>(resources.getResourceIds().size());
        for (Long fileId : resources.getResourceIds()) {
            try {
                // Check if the userId has proper permissions for all the files.
                checkFilePermission(fileId, resources.getUser(), FileAclEntry.FilePermissions.SHARE);

                QueryResult<FileAclEntry> aclEntryQueryResult = fileDBAdaptor.getAcl(fileId, members);

                QueryResult<File> fileQueryResult = fileDBAdaptor.get(fileId, new QueryOptions(QueryOptions.INCLUDE,
                        Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.TYPE.key())));
                if (fileQueryResult.getNumResults() != 1) {
                    throw new CatalogException("An unexpected error occured. File " + fileId + " not found when trying to delete the ACLs");
                }

                // We delete those ACLs for the whole path contained (propagate the acls in case of a directory)
                Query query = new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), resources.getStudyId());
                if (fileQueryResult.first().getType().equals(File.Type.FILE)) {
                    query.append(FileDBAdaptor.QueryParams.ID.key(), fileId);
                } else {
                    query.append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + fileQueryResult.first().getPath());
                }
                fileDBAdaptor.removeAclsFromMember(query, members, null);

                aclEntryQueryResult.setId("Remove file ACLs");
                retQueryResultList.add(aclEntryQueryResult);
            } catch (CatalogException e) {
                QueryResult<FileAclEntry> queryResult = new QueryResult<>();
                queryResult.setErrorMsg(e.getMessage());
                queryResult.setId("Remove file ACLs");

                retQueryResultList.add(queryResult);
            }
        }

        return retQueryResultList;
    }

    @Override
    public List<QueryResult<FileAclEntry>> updateFileAcl(AbstractManager.MyResourceIds resources, String member,
                                                         @Nullable String addPermissions, @Nullable String removePermissions,
                                                         @Nullable String setPermissions) throws CatalogException {
        checkUpdateParams(addPermissions, removePermissions, setPermissions);

        // Check the member is valid
        checkMembers(dbAdaptorFactory, resources.getStudyId(), Arrays.asList(member));

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(StringUtils.split(setPermissions, ","));
            // Check if the permissions are correct
            checkPermissions(permissions, FileAclEntry.FilePermissions::valueOf);
        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(StringUtils.split(addPermissions, ","));
                // Check if the permissions are correct
                checkPermissions(permissions, FileAclEntry.FilePermissions::valueOf);
            } else if (removePermissions != null) {
                permissions = Arrays.asList(StringUtils.split(removePermissions, ","));
                // Check if the permissions are correct
                checkPermissions(permissions, FileAclEntry.FilePermissions::valueOf);
            } else {
                throw new CatalogException("No permissions given to be added, removed or set");
            }
        }

        List<QueryResult<FileAclEntry>> retQueryResultList = new ArrayList<>(resources.getResourceIds().size());
        for (Long fileId : resources.getResourceIds()) {
            try {
                checkFilePermission(fileId, resources.getUser(), FileAclEntry.FilePermissions.SHARE);

                // Check that the member has permissions in the file
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.ID.key(), fileId)
                        .append(FileDBAdaptor.QueryParams.ACL_MEMBER.key(), member);
                QueryResult<Long> count = fileDBAdaptor.count(query);
                if (count == null || count.first() == 0) {
                    throw new CatalogException("Could not update ACLs for " + member + ". It seems the member does not have any "
                            + "permissions set yet.");
                }

                // We create the query to update just the file acl in case of a file or all the files and folders within the folder in other
                // case
                QueryResult<File> fileQueryResult = fileDBAdaptor.get(fileId, new QueryOptions(QueryOptions.INCLUDE,
                        Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.TYPE.key())));
                if (fileQueryResult.getNumResults() != 1) {
                    throw new CatalogException("An unexpected error occured. File " + fileId + " not found when trying to create new ACLs");
                }

                // We create those ACLs for the whole path contained (propagate the acls in case of a directory)
                query = new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), resources.getStudyId());
                if (fileQueryResult.first().getType().equals(File.Type.FILE)) {
                    query.append(FileDBAdaptor.QueryParams.ID.key(), fileId);
                } else {
                    query.append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + fileQueryResult.first().getPath());
                }

                // Update
                if (setPermissions != null) {
                    FileAclEntry fileAclEntry = new FileAclEntry(member, permissions);
                    fileDBAdaptor.createAcl(query, Arrays.asList(fileAclEntry));
                } else if (addPermissions != null) {
                    fileDBAdaptor.addAclsToMember(query, Arrays.asList(member), permissions);
                } else {
                    fileDBAdaptor.removeAclsFromMember(query, Arrays.asList(member), permissions);
                }

                QueryResult<FileAclEntry> aclEntryQueryResult = fileDBAdaptor.getAcl(fileId, member);
                aclEntryQueryResult.setId("Update file ACLs");
                retQueryResultList.add(aclEntryQueryResult);

            } catch (CatalogException e) {
                QueryResult<FileAclEntry> queryResult = new QueryResult<>();
                queryResult.setErrorMsg(e.getMessage());
                queryResult.setId("Update file ACLs");

                retQueryResultList.add(queryResult);
            }
        }

        return retQueryResultList;
    }

    @Override
    public List<QueryResult<IndividualAclEntry>> createIndividualAcls(AbstractManager.MyResourceIds resourceIds, List<String> members,
                                                                      List<String> permissions) throws CatalogException {
        checkPermissions(permissions, IndividualAclEntry.IndividualPermissions::valueOf);

        String userId = resourceIds.getUser();
        long studyId = resourceIds.getStudyId();

        // Check all the members are valid members
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check that all the members have permissions defined at the study level
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberExists(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        List<QueryResult<IndividualAclEntry>> retQueryResultList = new ArrayList<>(resourceIds.getResourceIds().size());

        // We create the list of permissions
        List<IndividualAclEntry> individualAclEntryList = new ArrayList<>(permissions.size());
        List<SampleAclEntry> sampleAclEntryList = new ArrayList<>(permissions.size());
        for (String member : members) {
            individualAclEntryList.add(new IndividualAclEntry(member, permissions));
            sampleAclEntryList.add(new SampleAclEntry(member, permissions));
        }

        for (Long individualId : resourceIds.getResourceIds()) {
            try {
                // Check if the userId has proper permissions for all the files.
                checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);

                // Check if any of the members already have permissions set in the file
                if (anyMemberHasPermissions(studyId, individualId, members, individualDBAdaptor)) {
                    throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                            + "particular cohort. Please, use update instead.");
                }

                Query query = new Query()
                        .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(CohortDBAdaptor.QueryParams.ID.key(), individualId);
                individualDBAdaptor.createAcl(query, individualAclEntryList);

                // Look for all the samples belonging to the individual
                query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId);
                QueryResult<Sample> sampleQueryResult =
                        sampleDBAdaptor.get(query, new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()));

                // If the individual contains samples, we propagate the permissions to all the samples
                if (sampleQueryResult != null && sampleQueryResult.getResult() != null && sampleQueryResult.getNumResults() > 0) {
                    List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
                    query = new Query()
                            .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                            .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);

                    sampleDBAdaptor.createAcl(query, sampleAclEntryList);
                }

                QueryResult<IndividualAclEntry> aclEntryQueryResult = individualDBAdaptor.getAcl(individualId, members);
                aclEntryQueryResult.setId("Create individual ACLs");
                retQueryResultList.add(aclEntryQueryResult);

            } catch (CatalogException e) {
                QueryResult<IndividualAclEntry> queryResult = new QueryResult<>();
                queryResult.setErrorMsg(e.getMessage());
                queryResult.setId("Create individual ACLs");

                retQueryResultList.add(queryResult);
            }
        }

        return retQueryResultList;
    }

    @Override
    public QueryResult<IndividualAclEntry> getAllIndividualAcls(String userId, long individualId) throws CatalogException {
        individualDBAdaptor.checkId(individualId);
        // Check if the userId has proper permissions for all the individuals.
//        checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);

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

        long studyId = individualDBAdaptor.getStudyId(individualId);
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

        long studyId = individualDBAdaptor.getStudyId(individualId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        // Obtain the ACLs the member had
        QueryResult<IndividualAclEntry> individualDBAdaptorAcl = individualDBAdaptor.getAcl(individualId, Arrays.asList(member));
        if (individualDBAdaptorAcl == null || individualDBAdaptorAcl.getNumResults() == 0) {
            throw new CatalogException("Could not remove the ACLs for " + member + ". It seems " + member + " did not have any ACLs "
                    + "defined");
        }

        individualDBAdaptor.removeAcl(individualId, member);

        // Look for all the samples belonging to the individual
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId);
        QueryResult<Sample> sampleQueryResult =
                sampleDBAdaptor.get(query, new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()));

        // If the individual contains samples, we propagate the permissions to all the samples
        if (sampleQueryResult != null && sampleQueryResult.getResult() != null && sampleQueryResult.getNumResults() > 0) {
            List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
            query = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);

            sampleDBAdaptor.removeAclsFromMember(query, Arrays.asList(member), null);
        }

        individualDBAdaptorAcl.setId("Remove individual ACLs");
        return individualDBAdaptorAcl;
    }

    @Override
    public QueryResult<IndividualAclEntry> updateIndividualAcl(String userId, long individualId, String member,
                                                               @Nullable String addPermissions, @Nullable String removePermissions,
                                                               @Nullable String setPermissions) throws CatalogException {
        checkUpdateParams(addPermissions, removePermissions, setPermissions);

        individualDBAdaptor.checkId(individualId);
        checkIndividualPermission(individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);

        long studyId = individualDBAdaptor.getStudyId(individualId);
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

        // Look for all the samples belonging to the individual
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId);
        QueryResult<Sample> sampleQueryResult =
                sampleDBAdaptor.get(query, new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()));

        List<String> permissions;
        if (setPermissions != null) {
            permissions = Arrays.asList(setPermissions.split(","));
            // Check if the permissions are correct
            checkPermissions(permissions, IndividualAclEntry.IndividualPermissions::valueOf);

            individualDBAdaptor.setAclsToMember(individualId, member, permissions);

            // If the individual contains samples, we propagate the permissions to all the samples
            if (sampleQueryResult != null && sampleQueryResult.getResult() != null && sampleQueryResult.getNumResults() > 0) {
                List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
                query = new Query()
                        .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);

                SampleAclEntry aclEntry = new SampleAclEntry(member, permissions);
                sampleDBAdaptor.createAcl(query, Arrays.asList(aclEntry));
            }

        } else {
            if (addPermissions != null) {
                permissions = Arrays.asList(addPermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, IndividualAclEntry.IndividualPermissions::valueOf);

                individualDBAdaptor.addAclsToMember(individualId, member, permissions);

                // If the individual contains samples, we propagate the permissions to all the samples
                if (sampleQueryResult != null && sampleQueryResult.getResult() != null && sampleQueryResult.getNumResults() > 0) {
                    List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
                    query = new Query()
                            .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                            .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);

                    sampleDBAdaptor.addAclsToMember(query, Arrays.asList(member), permissions);
                }
            }

            if (removePermissions != null) {
                permissions = Arrays.asList(removePermissions.split(","));
                // Check if the permissions are correct
                checkPermissions(permissions, IndividualAclEntry.IndividualPermissions::valueOf);

                individualDBAdaptor.removeAclsFromMember(individualId, member, permissions);

                // If the individual contains samples, we propagate the permissions to all the samples
                if (sampleQueryResult != null && sampleQueryResult.getResult() != null && sampleQueryResult.getNumResults() > 0) {
                    List<Long> sampleIds = sampleQueryResult.getResult().stream().map(Sample::getId).collect(Collectors.toList());
                    query = new Query()
                            .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                            .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);

                    sampleDBAdaptor.removeAclsFromMember(query, Arrays.asList(member), permissions);
                }
            }
        }

        return individualDBAdaptor.getAcl(individualId, Arrays.asList(member));
    }

    @Override
    public List<QueryResult<CohortAclEntry>> createCohortAcls(AbstractManager.MyResourceIds resourceIds, List<String> members,
                                                              List<String> permissions) throws CatalogException {
        checkPermissions(permissions, CohortAclEntry.CohortPermissions::valueOf);

        String userId = resourceIds.getUser();
        long studyId = resourceIds.getStudyId();

        // Check all the members are valid members
        checkMembers(dbAdaptorFactory, studyId, members);

        // Check that all the members have permissions defined at the study level
        for (String member : members) {
            if (!member.equals("*") && !member.equals("anonymous") && !memberExists(studyId, member)) {
                throw new CatalogException("Cannot create ACL for " + member + ". First, a general study permission must be "
                        + "defined for that member.");
            }
        }

        List<QueryResult<CohortAclEntry>> retQueryResultList = new ArrayList<>(resourceIds.getResourceIds().size());

        // We create the list of permissions
        List<CohortAclEntry> cohortAclEntryList = new ArrayList<>(permissions.size());
        for (String member : members) {
            cohortAclEntryList.add(new CohortAclEntry(member, permissions));
        }

        for (Long cohortId : resourceIds.getResourceIds()) {
            try {
                // Check if the userId has proper permissions for all the files.
                checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);

                // Check if any of the members already have permissions set in the file
                if (anyMemberHasPermissions(studyId, cohortId, members, cohortDBAdaptor)) {
                    throw new CatalogException("Cannot create ACL. At least one of the members already have some permissions set for this "
                            + "particular cohort. Please, use update instead.");
                }

                Query query = new Query()
                        .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(CohortDBAdaptor.QueryParams.ID.key(), cohortId);
                cohortDBAdaptor.createAcl(query, cohortAclEntryList);

                QueryResult<CohortAclEntry> aclEntryQueryResult = cohortDBAdaptor.getAcl(cohortId, members);
                aclEntryQueryResult.setId("Create cohort ACLs");
                retQueryResultList.add(aclEntryQueryResult);

            } catch (CatalogException e) {
                QueryResult<CohortAclEntry> queryResult = new QueryResult<>();
                queryResult.setErrorMsg(e.getMessage());
                queryResult.setId("Create cohort ACLs");

                retQueryResultList.add(queryResult);
            }
        }

        return retQueryResultList;
    }

    @Override
    public QueryResult<CohortAclEntry> getAllCohortAcls(String userId, long cohortId) throws CatalogException {
        cohortDBAdaptor.checkId(cohortId);
        // Check if the userId has proper permissions for all the cohorts.
//        checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);

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

        long studyId = cohortDBAdaptor.getStudyId(cohortId);
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

        long studyId = cohortDBAdaptor.getStudyId(cohortId);
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
        checkUpdateParams(addPermissions, removePermissions, setPermissions);

        cohortDBAdaptor.checkId(cohortId);
        checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);

        long studyId = cohortDBAdaptor.getStudyId(cohortId);
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

    private void checkUpdateParams(@Nullable String addPermissions, @Nullable String removePermissions, @Nullable String setPermissions)
            throws CatalogException {
        int cont = 0;
        cont += addPermissions != null ? 1 : 0;
        cont += removePermissions != null ? 1 : 0;
        cont += setPermissions != null ? 1 : 0;

        if (cont == 0) {
            throw new CatalogException("No permissions to be added, removed or set");
        } else if (cont > 1) {
            throw new CatalogException("More than one action to be performed found. Please, select just one.");
        }
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
            if (!member.equals("*") && !member.equals("anonymous") && !memberExists(studyId, member)) {
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
//        checkDatasetPermission(datasetId, userId, DatasetAclEntry.DatasetPermissions.SHARE);

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
        checkUpdateParams(addPermissions, removePermissions, setPermissions);

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
            if (!member.equals("*") && !member.equals("anonymous") && !memberExists(studyId, member)) {
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
//        checkJobPermission(jobId, userId, JobAclEntry.JobPermissions.SHARE);

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
        checkUpdateParams(addPermissions, removePermissions, setPermissions);

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
            if (!member.equals("*") && !member.equals("anonymous") && !memberExists(studyId, member)) {
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
//        checkDiseasePanelPermission(panelId, userId, DiseasePanelAclEntry.DiseasePanelPermissions.SHARE);

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
        checkUpdateParams(addPermissions, removePermissions, setPermissions);

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

    @Override
    public boolean anyMemberHasPermissions(long studyId, long id, List<String> members, AclDBAdaptor dbAdaptor)
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
        String userId = member;
        String groupId = null;
        if (!member.startsWith("@")) { // User
            if (member.equals(ADMIN) || isStudyOwner(studyId, member)) {
                return true;
            }
            if (!member.equals("anonymous") && !member.equals("*")) {
                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
                if (groupBelonging.getNumResults() > 0) {
                    groupId = groupBelonging.first().getName();
                }
            }
        }
        StudyAclEntry studyAcl = getStudyAclBelonging(studyId, userId, groupId);
        return studyAcl != null;
    }

    private boolean isStudyOwner(long studyId, String userId) throws CatalogDBException {
        return studyDBAdaptor.getOwnerId(studyId).equals(userId);
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
        List<String> members = (groupId != null)
                ? Arrays.asList(userId, groupId, OTHER_USERS_ID, ANONYMOUS)
                : Arrays.asList(userId, OTHER_USERS_ID, ANONYMOUS);

        QueryResult<StudyAclEntry> studyQueryResult = studyDBAdaptor.getAcl(studyId, members);
        Map<String, StudyAclEntry> userAclMap = studyQueryResult.getResult().stream().collect(Collectors.toMap(StudyAclEntry::getMember,
                Function.identity()));

        if (userId.equals(ANONYMOUS)) {
            if (userAclMap.containsKey(userId)) {
                return userAclMap.get(userId);
            }
            return null;
        }

        // Registered user
        EnumSet<StudyAclEntry.StudyPermissions> permissions = EnumSet.noneOf(StudyAclEntry.StudyPermissions.class);
        boolean flagPermissionFound = false;

        if (userAclMap.containsKey(userId)) {
            permissions.addAll(userAclMap.get(userId).getPermissions());
            flagPermissionFound = true;
        }
        if (StringUtils.isNotEmpty(groupId) && userAclMap.containsKey(groupId)) {
            permissions.addAll(userAclMap.get(groupId).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(ANONYMOUS)) {
            permissions.addAll(userAclMap.get(ANONYMOUS).getPermissions());
            flagPermissionFound = true;
        }
        if (userAclMap.containsKey(OTHER_USERS_ID)) {
            permissions.addAll(userAclMap.get(OTHER_USERS_ID).getPermissions());
            flagPermissionFound = true;
        }

        if (flagPermissionFound) {
            return new StudyAclEntry(userId, permissions);
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
