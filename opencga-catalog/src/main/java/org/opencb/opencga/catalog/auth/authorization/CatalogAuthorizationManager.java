/*
 * Copyright 2015-2017 OpenCB
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

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.CatalogAuditManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.acls.permissions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.utils.CatalogMemberValidator.checkMembers;

/**
 * Created by pfurio on 12/05/16.
 */
public class CatalogAuthorizationManager implements AuthorizationManager {

    private static final String MEMBERS_GROUP = "@members";
    private static final String ADMIN = "admin";

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
    private final FamilyDBAdaptor familyDBAdaptor;
    private final ClinicalAnalysisDBAdaptor clinicalAnalysisDBAdaptor;
    private final AuditManager auditManager;

    // List of Acls defined for the special users (admin, daemon...) read from the main configuration file.
    private static final List<StudyAclEntry> SPECIAL_ACL_LIST = Arrays.asList(
            new StudyAclEntry(ADMIN, Arrays.asList("VIEW_FILE_HEADERS", "VIEW_FILE_CONTENTS", "VIEW_FILES", "WRITE_FILES",
                    "VIEW_JOBS", "WRITE_JOBS", "VIEW_STUDY", "UPDATE_STUDY", "SHARE_STUDY")));

    private final boolean openRegister;

    private final AuthorizationDBAdaptor aclDBAdaptor;

    public CatalogAuthorizationManager(DBAdaptorFactory dbFactory, CatalogAuditManager auditManager, Configuration configuration)
            throws CatalogDBException, CatalogAuthorizationException {
        this.logger = LoggerFactory.getLogger(CatalogAuthorizationManager.class);
        this.auditManager = auditManager;
        this.aclDBAdaptor = new AuthorizationMongoDBAdaptor(configuration);

        this.openRegister = configuration.isOpenRegister();

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
        familyDBAdaptor = dbFactory.getCatalogFamilyDBAdaptor();
        clinicalAnalysisDBAdaptor = dbFactory.getClinicalAnalysisDBAdaptor();
    }

    private StudyAclEntry getSpecialPermissions(String member) {
        for (StudyAclEntry studyAclEntry : SPECIAL_ACL_LIST) {
            if (studyAclEntry.getMember().equals(member)) {
                return studyAclEntry;
            }
        }
        return null;
    }

    @Override
    public boolean isPublicRegistration() {
        return openRegister;
    }

    @Override
    public void checkProjectPermission(long projectId, String userId, StudyAclEntry.StudyPermissions permission) throws CatalogException {
        if (projectDBAdaptor.getOwnerId(projectId).equals(userId)) {
            return;
        }

        if (permission.equals(StudyAclEntry.StudyPermissions.VIEW_STUDY)) {
            if (userId.equals(ADMIN)) {
                if (getSpecialPermissions(ADMIN).getPermissions().contains(permission)) {
                    return;
                }
            } else {
                final Query query = new Query(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId);
                final QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FILTER_ROUTE_STUDIES
                        + StudyDBAdaptor.QueryParams.ID.key());

                for (Study study : studyDBAdaptor.get(query, queryOptions).getResult()) {
                    try {
                        checkStudyPermission(study.getId(), userId, StudyAclEntry.StudyPermissions.VIEW_STUDY);
                        return; //Return if can read some study
                    } catch (CatalogException e) {
                        e.printStackTrace();
                    }
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
        if (userId.equals(ADMIN)) {
            if (getSpecialPermissions(ADMIN).getPermissions().contains(permission)) {
                return;
            }
        } else {
            if (studyDBAdaptor.hasStudyPermission(studyId, userId, permission)) {
                return;
            }
        }
        throw CatalogAuthorizationException.deny(userId, message, "Study", studyId, null);
    }

    @Override
    public void checkFilePermission(long studyId, long fileId, String userId, FileAclEntry.FilePermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.ID.key(), fileId)
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        StudyAclEntry.StudyPermissions studyPermission;
        switch (permission) {
            case VIEW_HEADER:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_FILE_HEADERS;
                break;
            case VIEW_CONTENT:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_FILE_CONTENTS;
                break;
            case VIEW:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_FILES;
                break;
            case WRITE:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_FILES;
                break;
            case DELETE:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_FILES;
                break;
            case DOWNLOAD:
                studyPermission = StudyAclEntry.StudyPermissions.DOWNLOAD_FILES;
                break;
            case UPLOAD:
                studyPermission = StudyAclEntry.StudyPermissions.UPLOAD_FILES;
                break;
            case SHARE:
                studyPermission = StudyAclEntry.StudyPermissions.SHARE_FILES;
                break;
            default:
                throw new CatalogAuthorizationException("Permission " + permission.toString() + " not found");
        }

        if (checkUserPermission(userId, query, studyPermission, fileDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "File", fileId, null);
    }

    private boolean checkUserPermission(String userId, Query query, StudyAclEntry.StudyPermissions studyPermission, DBAdaptor dbAdaptor)
            throws CatalogDBException, CatalogAuthorizationException {
        if (userId.equals(ADMIN)) {
            if (getSpecialPermissions(ADMIN).getPermissions().contains(studyPermission)) {
                return true;
            }
        } else {
            if ((Long) dbAdaptor.count(query, userId, studyPermission).first() == 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkSamplePermission(long studyId, long sampleId, String userId, SampleAclEntry.SamplePermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), sampleId)
                .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        StudyAclEntry.StudyPermissions studyPermission;
        switch (permission) {
            case VIEW:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_SAMPLES;
                break;
            case UPDATE:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_SAMPLES;
                break;
            case DELETE:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_SAMPLES;
                break;
            case SHARE:
                studyPermission = StudyAclEntry.StudyPermissions.SHARE_SAMPLES;
                break;
            case WRITE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_SAMPLE_ANNOTATIONS;
                break;
            case VIEW_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS;
                break;
            case DELETE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_SAMPLE_ANNOTATIONS;
                break;
            default:
                throw new CatalogAuthorizationException("Permission " + permission.toString() + " not found");
        }

        if (checkUserPermission(userId, query, studyPermission, sampleDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Sample", sampleId, null);
    }

    @Override
    public void checkIndividualPermission(long studyId, long individualId, String userId,
                                          IndividualAclEntry.IndividualPermissions permission) throws CatalogException {
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.ID.key(), individualId)
                .append(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        StudyAclEntry.StudyPermissions studyPermission;
        switch (permission) {
            case VIEW:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS;
                break;
            case UPDATE:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS;
                break;
            case DELETE:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_INDIVIDUALS;
                break;
            case SHARE:
                studyPermission = StudyAclEntry.StudyPermissions.SHARE_INDIVIDUALS;
                break;
            case WRITE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_INDIVIDUAL_ANNOTATIONS;
                break;
            case VIEW_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS;
                break;
            case DELETE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_INDIVIDUAL_ANNOTATIONS;
                break;
            default:
                throw new CatalogAuthorizationException("Permission " + permission.toString() + " not found");
        }

        if (checkUserPermission(userId, query, studyPermission, individualDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Individual", individualId, null);
    }

    @Override
    public void checkJobPermission(long studyId, long jobId, String userId, JobAclEntry.JobPermissions permission) throws CatalogException {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.ID.key(), jobId)
                .append(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        StudyAclEntry.StudyPermissions studyPermission;
        switch (permission) {
            case VIEW:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_JOBS;
                break;
            case UPDATE:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_JOBS;
                break;
            case DELETE:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_JOBS;
                break;
            case SHARE:
                studyPermission = StudyAclEntry.StudyPermissions.SHARE_JOBS;
                break;
            default:
                throw new CatalogAuthorizationException("Permission " + permission.toString() + " not found");
        }

        if (checkUserPermission(userId, query, studyPermission, jobDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Job", jobId, null);
    }

    @Override
    public void checkCohortPermission(long studyId, long cohortId, String userId, CohortAclEntry.CohortPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.ID.key(), cohortId)
                .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        StudyAclEntry.StudyPermissions studyPermission;
        switch (permission) {
            case VIEW:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_COHORTS;
                break;
            case UPDATE:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_COHORTS;
                break;
            case DELETE:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_COHORTS;
                break;
            case SHARE:
                studyPermission = StudyAclEntry.StudyPermissions.SHARE_COHORTS;
                break;
            case WRITE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_COHORT_ANNOTATIONS;
                break;
            case VIEW_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS;
                break;
            case DELETE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_COHORT_ANNOTATIONS;
                break;
            default:
                throw new CatalogAuthorizationException("Permission " + permission.toString() + " not found");
        }

        if (checkUserPermission(userId, query, studyPermission, cohortDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Cohort", cohortId, null);

    }

    @Override
    public void checkDiseasePanelPermission(long studyId, long panelId, String userId,
                                            DiseasePanelAclEntry.DiseasePanelPermissions permission) throws CatalogException {
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.ID.key(), panelId)
                .append(PanelDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        StudyAclEntry.StudyPermissions studyPermission;
        switch (permission) {
            case VIEW:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_PANELS;
                break;
            case UPDATE:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_PANELS;
                break;
            case DELETE:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_PANELS;
                break;
            case SHARE:
                studyPermission = StudyAclEntry.StudyPermissions.SHARE_PANELS;
                break;
            default:
                throw new CatalogAuthorizationException("Permission " + permission.toString() + " not found");
        }

        if (checkUserPermission(userId, query, studyPermission, panelDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Panel", panelId, null);
    }

    @Override
    public void checkFamilyPermission(long studyId, long familyId, String userId, FamilyAclEntry.FamilyPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.ID.key(), familyId)
                .append(FamilyDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        StudyAclEntry.StudyPermissions studyPermission;
        switch (permission) {
            case VIEW:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_FAMILIES;
                break;
            case UPDATE:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_FAMILIES;
                break;
            case DELETE:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_FAMILIES;
                break;
            case SHARE:
                studyPermission = StudyAclEntry.StudyPermissions.SHARE_FAMILIES;
                break;
            case WRITE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_FAMILY_ANNOTATIONS;
                break;
            case VIEW_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS;
                break;
            case DELETE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_FAMILY_ANNOTATIONS;
                break;
            default:
                throw new CatalogAuthorizationException("Permission " + permission.toString() + " not found");
        }

        if (checkUserPermission(userId, query, studyPermission, familyDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Family", familyId, null);

    }

    @Override
    public void checkClinicalAnalysisPermission(long studyId, long analysisId, String userId,
                                                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions permission) throws CatalogException {
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), analysisId)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        StudyAclEntry.StudyPermissions studyPermission;
        switch (permission) {
            case VIEW:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS;
                break;
            case UPDATE:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS;
                break;
            case DELETE:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_CLINICAL_ANALYSIS;
                break;
            case SHARE:
                studyPermission = StudyAclEntry.StudyPermissions.SHARE_CLINICAL_ANALYSIS;
                break;
            default:
                throw new CatalogAuthorizationException("Permission " + permission.toString() + " not found");
        }

        if (checkUserPermission(userId, query, studyPermission, clinicalAnalysisDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "ClinicalAnalysis", analysisId, null);
    }

    @Override
    public QueryResult<StudyAclEntry> getAllStudyAcls(String userId, long studyId) throws CatalogException {
        studyDBAdaptor.checkId(studyId);
        checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        return aclDBAdaptor.get(studyId, null, MongoDBAdaptorFactory.STUDY_COLLECTION);
    }

    @Override
    public QueryResult<StudyAclEntry> getStudyAcl(String userId, long studyId, String member) throws CatalogException {
        studyDBAdaptor.checkId(studyId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.SHARE_STUDY);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(studyId, Arrays.asList(member), MongoDBAdaptorFactory.STUDY_COLLECTION);
    }

    @Override
    public QueryResult<SampleAclEntry> getAllSampleAcls(String userId, long sampleId) throws CatalogException {
        sampleDBAdaptor.checkId(sampleId);
        checkSamplePermission(sampleDBAdaptor.getStudyId(sampleId), sampleId, userId, SampleAclEntry.SamplePermissions.SHARE);
        return aclDBAdaptor.get(sampleId, null, MongoDBAdaptorFactory.SAMPLE_COLLECTION);
    }

    @Override
    public QueryResult<SampleAclEntry> getSampleAcl(String userId, long sampleId, String member) throws CatalogException {
        sampleDBAdaptor.checkId(sampleId);

        long studyId = sampleDBAdaptor.getStudyId(sampleId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkSamplePermission(studyId, sampleId, userId, SampleAclEntry.SamplePermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(sampleId, Arrays.asList(member), MongoDBAdaptorFactory.SAMPLE_COLLECTION);
    }

    @Override
    public void resetPermissionsFromAllEntities(long studyId, List<String> members) throws CatalogException {
        aclDBAdaptor.resetMembersFromAllEntries(studyId, members);
    }

    @Override
    public QueryResult<FileAclEntry> getAllFileAcls(String userId, long fileId, boolean checkPermission) throws CatalogException {
        fileDBAdaptor.checkId(fileId);
        if (checkPermission) {
            checkFilePermission(fileDBAdaptor.getStudyIdByFileId(fileId), fileId, userId, FileAclEntry.FilePermissions.SHARE);
        }
        return aclDBAdaptor.get(fileId, null, MongoDBAdaptorFactory.FILE_COLLECTION);
    }

    @Override
    public QueryResult<FileAclEntry> getFileAcl(String userId, long fileId, String member) throws CatalogException {
        fileDBAdaptor.checkId(fileId);

        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(fileId, Arrays.asList(member), MongoDBAdaptorFactory.FILE_COLLECTION);
    }

    @Override
    public QueryResult<IndividualAclEntry> getAllIndividualAcls(String userId, long individualId) throws CatalogException {
        individualDBAdaptor.checkId(individualId);
        checkIndividualPermission(individualDBAdaptor.getStudyId(individualId), individualId, userId,
                IndividualAclEntry.IndividualPermissions.SHARE);
        return aclDBAdaptor.get(individualId, null, MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION);
    }

    @Override
    public QueryResult<IndividualAclEntry> getIndividualAcl(String userId, long individualId, String member) throws CatalogException {
        individualDBAdaptor.checkId(individualId);

        long studyId = individualDBAdaptor.getStudyId(individualId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkIndividualPermission(studyId, individualId, userId, IndividualAclEntry.IndividualPermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(individualId, Arrays.asList(member), MongoDBAdaptorFactory.INDIVIDUAL_COLLECTION);
    }

    @Override
    public QueryResult<CohortAclEntry> getAllCohortAcls(String userId, long cohortId) throws CatalogException {
        cohortDBAdaptor.checkId(cohortId);
        checkCohortPermission(cohortDBAdaptor.getStudyId(cohortId), cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);
        return aclDBAdaptor.get(cohortId, null, MongoDBAdaptorFactory.COHORT_COLLECTION);
    }

    @Override
    public QueryResult<CohortAclEntry> getCohortAcl(String userId, long cohortId, String member) throws CatalogException {
        cohortDBAdaptor.checkId(cohortId);

        long studyId = cohortDBAdaptor.getStudyId(cohortId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkCohortPermission(studyId, cohortId, userId, CohortAclEntry.CohortPermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(cohortId, Arrays.asList(member), MongoDBAdaptorFactory.COHORT_COLLECTION);
    }

    @Override
    public QueryResult<JobAclEntry> getAllJobAcls(String userId, long jobId) throws CatalogException {
        jobDBAdaptor.checkId(jobId);
        checkJobPermission(jobDBAdaptor.getStudyId(jobId), jobId, userId, JobAclEntry.JobPermissions.SHARE);
        return aclDBAdaptor.get(jobId, null, MongoDBAdaptorFactory.JOB_COLLECTION);
    }

    @Override
    public QueryResult<JobAclEntry> getJobAcl(String userId, long jobId, String member) throws CatalogException {
        jobDBAdaptor.checkId(jobId);

        long studyId = jobDBAdaptor.getStudyId(jobId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkJobPermission(studyId, jobId, userId, JobAclEntry.JobPermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            QueryResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(jobId, Arrays.asList(member), MongoDBAdaptorFactory.JOB_COLLECTION);
    }

    @Override
    public QueryResult<FamilyAclEntry> getAllFamilyAcls(String userId, long familyId) throws CatalogException {
        familyDBAdaptor.checkId(familyId);
        checkFamilyPermission(familyDBAdaptor.getStudyId(familyId), familyId, userId, FamilyAclEntry.FamilyPermissions.SHARE);
        return aclDBAdaptor.get(familyId, null, MongoDBAdaptorFactory.FAMILY_COLLECTION);
    }

    @Override
    public QueryResult<FamilyAclEntry> getFamilyAcl(String userId, long familyId, String member) throws CatalogException {
        familyDBAdaptor.checkId(familyId);

        long studyId = familyDBAdaptor.getStudyId(familyId);
        checkMembers(dbAdaptorFactory, studyId, Arrays.asList(member));

        try {
            checkFamilyPermission(studyId, familyId, userId, FamilyAclEntry.FamilyPermissions.SHARE);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        return aclDBAdaptor.get(familyId, Arrays.asList(member), MongoDBAdaptorFactory.FAMILY_COLLECTION);
    }

    private void checkAskingOwnPermissions(String userId, String member, long studyId) throws CatalogException {
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

    @Override
    public List<QueryResult<StudyAclEntry>> setStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException {
        // We obtain which of those members are actually users to add them to the @members group automatically
        List<String> userList = members.stream()
                .filter(member -> !member.startsWith("@"))
                .collect(Collectors.toList());
        if (userList.size() > 0) {
            // We first add the member to the @members group in case they didn't belong already
            for (Long studyId : studyIds) {
                studyDBAdaptor.addUsersToGroup(studyId, MEMBERS_GROUP, userList);
            }
        }

        aclDBAdaptor.setToMembers(studyIds, members, permissions, MongoDBAdaptorFactory.STUDY_COLLECTION);
        return aclDBAdaptor.get(studyIds, members, MongoDBAdaptorFactory.STUDY_COLLECTION);
    }

    @Override
    public List<QueryResult<StudyAclEntry>> addStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException {
        // We obtain which of those members are actually users to add them to the @members group automatically
        List<String> userList = members.stream()
                .filter(member -> !member.startsWith("@"))
                .collect(Collectors.toList());
        if (userList.size() > 0) {
            // We first add the member to the @members group in case they didn't belong already
            for (Long studyId : studyIds) {
                studyDBAdaptor.addUsersToGroup(studyId, MEMBERS_GROUP, userList);
            }
        }
        aclDBAdaptor.addToMembers(studyIds, members, permissions, MongoDBAdaptorFactory.STUDY_COLLECTION);
        return aclDBAdaptor.get(studyIds, members, MongoDBAdaptorFactory.STUDY_COLLECTION);
    }

    @Override
    public List<QueryResult<StudyAclEntry>> removeStudyAcls(List<Long> studyIds, List<String> members, @Nullable List<String> permissions)
            throws CatalogException {
        aclDBAdaptor.removeFromMembers(studyIds, members, permissions, MongoDBAdaptorFactory.STUDY_COLLECTION);
        return aclDBAdaptor.get(studyIds, members, MongoDBAdaptorFactory.STUDY_COLLECTION);
    }

    private <E extends AbstractAclEntry> List<QueryResult<E>> getAcls(List<Long> ids, List<String> members, String entity)
            throws CatalogException {
        return aclDBAdaptor.get(ids, members, entity);
    }

    @Override
    public <E extends AbstractAclEntry> List<QueryResult<E>> setAcls(long studyId, List<Long> ids, List<String> members,
                                                                     List<String> permissions, String entity) throws CatalogException {
        if (ids == null || ids.size() == 0) {
            logger.warn("Missing identifiers to set acls");
            return Collections.emptyList();
        }

        // We obtain which of those members are actually users to add them to the @members group automatically
        List<String> userList = members.stream()
                .filter(member -> !member.startsWith("@"))
                .collect(Collectors.toList());
        if (userList.size() > 0) {
            // We first add the member to the @members group in case they didn't belong already
            studyDBAdaptor.addUsersToGroup(studyId, MEMBERS_GROUP, userList);
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
    public <E extends AbstractAclEntry> List<QueryResult<E>> addAcls(long studyId, List<Long> ids, List<String> members,
                                                                     List<String> permissions, String entity) throws CatalogException {
        if (ids == null || ids.size() == 0) {
            logger.warn("Missing identifiers to add acls");
            return Collections.emptyList();
        }

        // We obtain which of those members are actually users to add them to the @members group automatically
        List<String> userList = members.stream()
                .filter(member -> !member.startsWith("@"))
                .collect(Collectors.toList());
        if (userList.size() > 0) {
            // We first add the member to the @members group in case they didn't belong already
            studyDBAdaptor.addUsersToGroup(studyId, MEMBERS_GROUP, userList);
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

    @Override
    public <E extends AbstractAclEntry> List<QueryResult<E>> replicateAcls(long studyId, List<Long> ids, List<E> aclEntries,
                                                                           String entity) throws CatalogException {
        if (ids == null || ids.size() == 0) {
            logger.warn("Missing identifiers to set acls");
            return Collections.emptyList();
        }

        long startTime = System.currentTimeMillis();
        aclDBAdaptor.setAcls(ids, aclEntries, entity);
        int dbTime = (int) (System.currentTimeMillis() - startTime);

        List<QueryResult<E>> aclResultList = getAcls(ids, null, entity);

        for (QueryResult<E> aclEntryQueryResult : aclResultList) {
            aclEntryQueryResult.setDbTime(aclEntryQueryResult.getDbTime() + dbTime);
        }

        return aclResultList;
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

    public static void checkPermissions(List<String> permissions, Function<String, Enum> getValue) throws CatalogException {
        for (String permission : permissions) {
            try {
                getValue.apply(permission);
            } catch (IllegalArgumentException e) {
                throw new CatalogException("The permission " + permission + " is not a correct permission.");
            }
        }
    }

}
