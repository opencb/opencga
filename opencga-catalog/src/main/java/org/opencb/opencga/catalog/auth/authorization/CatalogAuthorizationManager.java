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
import org.opencb.opencga.catalog.audit.CatalogAuditManager;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final Logger logger;

    private final DBAdaptorFactory dbAdaptorFactory;
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

    private final AuthorizationDBAdaptor aclDBAdaptor;

    private final String ADMIN = "admin";
    private final String ANONYMOUS = "anonymous";

    public CatalogAuthorizationManager(DBAdaptorFactory dbFactory, CatalogAuditManager auditManager, Configuration configuration)
            throws CatalogDBException {
        this.logger = LoggerFactory.getLogger(CatalogAuthorizationManager.class);
        this.auditManager = auditManager;
        this.aclDBAdaptor = new AuthorizationMongoDBAdaptor(configuration);

        this.dbAdaptorFactory = dbFactory;
        projectDBAdaptor = dbFactory.getCatalogProjectDbAdaptor();
        studyDBAdaptor = dbFactory.getCatalogStudyDBAdaptor();
        fileDBAdaptor = dbFactory.getCatalogFileDBAdaptor();
        jobDBAdaptor = dbFactory.getCatalogJobDBAdaptor();
        sampleDBAdaptor = dbFactory.getCatalogSampleDBAdaptor();
        individualDBAdaptor = dbFactory.getCatalogIndividualDBAdaptor();
        cohortDBAdaptor = dbFactory.getCatalogCohortDBAdaptor();
        datasetDBAdaptor = dbFactory.getCatalogDatasetDBAdaptor();
        panelDBAdaptor = dbFactory.getCatalogPanelDBAdaptor();
        metaDBAdaptor = dbFactory.getCatalogMetaDBAdaptor();
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
        QueryResult<FileAclEntry> fileQueryResult = aclDBAdaptor.get(fileId, userIds, MongoDBAdaptorFactory.FILE_COLLECTION);

        Map<String, FileAclEntry> userAclMap = new HashMap<>();
        for (FileAclEntry fileAclEntry : fileQueryResult.getResult()) {
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
        QueryResult<SampleAclEntry> sampleQueryResult = aclDBAdaptor.get(sampleId, userIds, MongoDBAdaptorFactory.SAMPLE_COLLECTION);

        Map<String, SampleAclEntry> userAclMap = new HashMap<>();
        for (SampleAclEntry sampleAcl : sampleQueryResult.getResult()) {
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
        QueryResult<IndividualAclEntry> individualQueryResult = aclDBAdaptor.get(individualId, userIds,
                MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION);

        Map<String, IndividualAclEntry> userAclMap = new HashMap<>();
        for (IndividualAclEntry individualAcl : individualQueryResult.getResult()) {
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
        QueryResult<JobAclEntry> jobQueryResult = aclDBAdaptor.get(jobId, userIds, MongoDBAdaptorFactory.JOB_COLLECTION);

        Map<String, JobAclEntry> userAclMap = new HashMap<>();
        for (JobAclEntry jobAcl : jobQueryResult.getResult()) {
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
        QueryResult<CohortAclEntry> cohortQueryResult = aclDBAdaptor.get(cohortId, userIds, MongoDBAdaptorFactory.COHORT_COLLECTION);

        Map<String, CohortAclEntry> userAclMap = new HashMap<>();
        for (CohortAclEntry cohortAcl : cohortQueryResult.getResult()) {
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
        QueryResult<DatasetAclEntry> datasetQueryResult= aclDBAdaptor.get(datasetId, userIds,
                MongoDBAdaptorFactory.DATASET_COLLECTION);

        Map<String, DatasetAclEntry> userAclMap = new HashMap<>();
        for (DatasetAclEntry datasetAcl : datasetQueryResult.getResult()) {
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
        QueryResult<DiseasePanelAclEntry> panelQueryResult = aclDBAdaptor.get(panelId, userIds,
                MongoDBAdaptorFactory.PANEL_COLLECTION);

        Map<String, DiseasePanelAclEntry> userAclMap = new HashMap<>();
        for (DiseasePanelAclEntry panelAcl : panelQueryResult.getResult()) {
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

    private boolean memberExists(long studyId, String member) throws CatalogException {
        QueryResult<StudyAclEntry> acl = aclDBAdaptor.get(studyId, Arrays.asList(member), MongoDBAdaptorFactory.STUDY_COLLECTION);
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

        return aclDBAdaptor.get(studyId, members, MongoDBAdaptorFactory.STUDY_COLLECTION);
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

        return aclDBAdaptor.get(sampleId, members, MongoDBAdaptorFactory.SAMPLE_COLLECTION);
    }

    @Override
    public <E extends Enum<E>> void checkValidPermission(List<String> permissions, Class<E> enumClass) throws CatalogException {
        if (permissions == null) {
            return;
        }

        Set<String> allowedAclEntrySet = new HashSet<>();
        for (E e : enumClass.getEnumConstants()) {
            allowedAclEntrySet.add(e.name());
        }

        for (String permission : permissions) {
            if (!allowedAclEntrySet.contains(permission)) {
                throw new CatalogException(permission + " is not a valid permission");
            }
        }
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

        return aclDBAdaptor.get(fileId, members, MongoDBAdaptorFactory.FILE_COLLECTION);
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

        return aclDBAdaptor.get(individualId, members, MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION);
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

        return aclDBAdaptor.get(cohortId, members, MongoDBAdaptorFactory.COHORT_COLLECTION);
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

        return aclDBAdaptor.get(datasetId, members, MongoDBAdaptorFactory.DATASET_COLLECTION);
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

        return aclDBAdaptor.get(jobId, members, MongoDBAdaptorFactory.JOB_COLLECTION);
    }

    @Override
    public List<QueryResult<StudyAclEntry>> setStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException {
        aclDBAdaptor.setToMembers(studyIds, members, permissions, MongoDBAdaptorFactory.STUDY_COLLECTION);
        return aclDBAdaptor.get(studyIds, members, MongoDBAdaptorFactory.STUDY_COLLECTION);
    }

    @Override
    public List<QueryResult<StudyAclEntry>> addStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException {
        aclDBAdaptor.addToMembers(studyIds, members, permissions, MongoDBAdaptorFactory.STUDY_COLLECTION);
        return aclDBAdaptor.get(studyIds, members, MongoDBAdaptorFactory.STUDY_COLLECTION);
    }

    @Override
    public List<QueryResult<StudyAclEntry>> removeStudyAcls(List<Long> studyIds, List<String> members, @Nullable List<String> permissions)
            throws CatalogException {
        aclDBAdaptor.removeFromMembers(studyIds, members, permissions, MongoDBAdaptorFactory.STUDY_COLLECTION);
        return aclDBAdaptor.get(studyIds, members, MongoDBAdaptorFactory.STUDY_COLLECTION);
    }

    @Override
    public <E extends AbstractAclEntry> QueryResult<E> getAcl(long id, List<String> members, String entity) throws CatalogException {
        return aclDBAdaptor.get(id, members, entity);
    }

    @Override
    public <E extends AbstractAclEntry> List<QueryResult<E>> getAcls(List<Long> ids, List<String> members, String entity)
            throws CatalogException {
        return aclDBAdaptor.get(ids, members, entity);
    }

    @Override
    public <E extends AbstractAclEntry> List<QueryResult<E>> setAcls(List<Long> ids, List<String> members, List<String> permissions,
                                                                     String entity) throws CatalogException {
        if (ids == null || ids.size() == 0) {
            logger.warn("Missing identifiers to set acls");
            return Collections.emptyList();
        }

        long startTime = System.currentTimeMillis();
        aclDBAdaptor.setToMembers(ids, members, permissions, entity);
        int dbTime = (int) (System.currentTimeMillis() - startTime);

        List<QueryResult<E>> aclResultList = getAcls(ids, members, entity);

        for (QueryResult<E> aclEntryQueryResult : aclResultList) {
            aclEntryQueryResult.setDbTime(aclEntryQueryResult.getDbTime() + dbTime);
        }

        return aclResultList;
    }

    @Override
    public <E extends AbstractAclEntry> List<QueryResult<E>> addAcls(List<Long> ids, List<String> members, List<String> permissions,
                                                                     String entity) throws CatalogException {
        if (ids == null || ids.size() == 0) {
            logger.warn("Missing identifiers to add acls");
            return Collections.emptyList();
        }

        long startTime = System.currentTimeMillis();
        aclDBAdaptor.addToMembers(ids, members, permissions, entity);
        int dbTime = (int) (System.currentTimeMillis() - startTime);

        List<QueryResult<E>> aclResultList = getAcls(ids, members, entity);

        for (QueryResult<E> aclEntryQueryResult : aclResultList) {
            aclEntryQueryResult.setDbTime(aclEntryQueryResult.getDbTime() + dbTime);
        }

        return aclResultList;
    }

    @Override
    public <E extends AbstractAclEntry> List<QueryResult<E>> removeAcls(List<Long> ids, List<String> members,
                                                                        @Nullable List<String> permissions, String entity)
            throws CatalogException {
        if (ids == null || ids.size() == 0) {
            logger.warn("Missing identifiers to remove acls");
            return Collections.emptyList();
        }

        long startTime = System.currentTimeMillis();
        aclDBAdaptor.removeFromMembers(ids, members, permissions, entity);

        int dbTime = (int) (System.currentTimeMillis() - startTime);
        List<QueryResult<E>> aclResultList = getAcls(ids, members, entity);

        // Update dbTime
        for (QueryResult<E> aclEntryQueryResult : aclResultList) {
            aclEntryQueryResult.setDbTime(aclEntryQueryResult.getDbTime() + dbTime);
        }
        return aclResultList;
    }

//    @Override
//    public boolean anyMemberHasPermissions(long studyId, long id, List<String> members, AclDBAdaptor dbAdaptor)
//            throws CatalogException {
//
//        List<String> allMembers = new ArrayList<>(members.size());
//        allMembers.addAll(members);
//
//        for (String member : members) {
//            if (member.startsWith("@")) { // It's a group
//                // Obtain the users of the group
//                QueryResult<Group> group = studyDBAdaptor.getGroup(studyId, member, Collections.emptyList());
//                if (group != null && group.getNumResults() == 1) {
//                    allMembers.addAll(group.first().getUserIds());
//                }
//            } else if (!member.equalsIgnoreCase("anonymous") && !member.equals("*")) { // It's a user id
//                // Get the group where the user might belong to
//                QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//                if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                    allMembers.add(groupBelonging.first().getName());
//                }
//            }
//        }
//
//        return dbAdaptor.getAcl(id, allMembers).getNumResults() > 0;
//    }

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

        QueryResult<StudyAclEntry> studyQueryResult = aclDBAdaptor.get(studyId, members, MongoDBAdaptorFactory.STUDY_COLLECTION);
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
