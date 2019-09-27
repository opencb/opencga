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

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.Group;
import org.opencb.opencga.core.models.GroupParams;
import org.opencb.opencga.core.models.PermissionRule;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.permissions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 12/05/16.
 */
public class CatalogAuthorizationManager implements AuthorizationManager {

    public static final String MEMBERS_GROUP = "@members";
    public static final String ADMINS_GROUP = "@admins";
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
    private final PanelDBAdaptor panelDBAdaptor;
    private final FamilyDBAdaptor familyDBAdaptor;
    private final ClinicalAnalysisDBAdaptor clinicalAnalysisDBAdaptor;

    // List of Acls defined for the special users (admin, daemon...) read from the main configuration file.
    private static final List<StudyAclEntry> SPECIAL_ACL_LIST = Arrays.asList(
            new StudyAclEntry(ADMIN, Arrays.asList(StudyAclEntry.StudyPermissions.VIEW_FILE_HEADERS.name(),
                    StudyAclEntry.StudyPermissions.VIEW_FILE_CONTENTS.name(), StudyAclEntry.StudyPermissions.VIEW_FILES.name(),
                    StudyAclEntry.StudyPermissions.WRITE_FILES.name(), StudyAclEntry.StudyPermissions.UPLOAD_FILES.name(),
                    StudyAclEntry.StudyPermissions.DELETE_FILES.name(), StudyAclEntry.StudyPermissions.VIEW_JOBS.name(),
                    StudyAclEntry.StudyPermissions.WRITE_JOBS.name())));

    private final boolean openRegister;

    private final AuthorizationDBAdaptor aclDBAdaptor;

    public CatalogAuthorizationManager(DBAdaptorFactory dbFactory, Configuration configuration)
            throws CatalogDBException {
        this.logger = LoggerFactory.getLogger(CatalogAuthorizationManager.class);
        this.aclDBAdaptor = new AuthorizationMongoDBAdaptor(dbFactory);

        this.openRegister = configuration.isOpenRegister();

        this.dbAdaptorFactory = dbFactory;
        projectDBAdaptor = dbFactory.getCatalogProjectDbAdaptor();
        studyDBAdaptor = dbFactory.getCatalogStudyDBAdaptor();
        fileDBAdaptor = dbFactory.getCatalogFileDBAdaptor();
        jobDBAdaptor = dbFactory.getCatalogJobDBAdaptor();
        sampleDBAdaptor = dbFactory.getCatalogSampleDBAdaptor();
        individualDBAdaptor = dbFactory.getCatalogIndividualDBAdaptor();
        cohortDBAdaptor = dbFactory.getCatalogCohortDBAdaptor();
        panelDBAdaptor = dbFactory.getCatalogPanelDBAdaptor();
        familyDBAdaptor = dbFactory.getCatalogFamilyDBAdaptor();
        clinicalAnalysisDBAdaptor = dbFactory.getClinicalAnalysisDBAdaptor();
    }

    public static StudyAclEntry getSpecialPermissions(String member) {
        for (StudyAclEntry studyAclEntry : SPECIAL_ACL_LIST) {
            if (studyAclEntry.getMember().equals(member)) {
                return studyAclEntry;
            }
        }
        return new StudyAclEntry(member, Collections.emptyList());
    }

    @Override
    public boolean isPublicRegistration() {
        return openRegister;
    }

    @Override
    public void checkCanEditProject(long projectId, String userId) throws CatalogException {
        if (projectDBAdaptor.getOwnerId(projectId).equals(userId)) {
            return;
        }
        throw new CatalogAuthorizationException("Permission denied: Only the owner of the project can update it.");
    }

    @Override
    public void checkCanViewProject(long projectId, String userId) throws CatalogException {
        if (userId.equals(ADMIN)) {
            return;
        }
        if (projectDBAdaptor.getOwnerId(projectId).equals(userId)) {
            return;
        }

        // Only members of any study belonging to the project can view the project
        final Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId)
                .append(StudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), userId);

        if (studyDBAdaptor.count(query).first() > 0) {
            return;
        }

        throw CatalogAuthorizationException.deny(userId, "view", "Project", projectId, null);
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
    public void checkCanEditStudy(long studyId, String userId) throws CatalogException {
        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (!ownerId.equals(userId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only owners or administrative users are allowed to modify a study");
        }
    }

    @Override
    public void checkCanViewStudy(long studyId, String userId) throws CatalogException {
        if (ADMIN.equals(userId)) {
            return;
        }

        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (ownerId.equals(userId)) {
            return;
        }

        DataResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
        if (groupBelonging.getNumResults() == 0) {
            throw new CatalogAuthorizationException("Only the members of the study are allowed to see it");
        }
    }

    @Override
    public void checkCanUpdatePermissionRules(long studyId, String userId) throws CatalogException {
        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (!ownerId.equals(userId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only owners or administrative users are allowed to modify a update permission rules");
        }
    }

    @Override
    public void checkCreateDeleteGroupPermissions(long studyId, String userId, String group) throws CatalogException {
        if (group.equals(MEMBERS_GROUP) || group.equals(ADMINS_GROUP)) {
            throw new CatalogAuthorizationException(group + " is a protected group that cannot be created or deleted.");
        }

        String ownerId = studyDBAdaptor.getOwnerId(studyId);
        if (!userId.equals(ADMIN) && !userId.equals(ownerId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only administrative users are allowed to create/remove groups.");
        }
    }

    @Override
    public void checkSyncGroupPermissions(long studyId, String userId, String group) throws CatalogException {
        checkCreateDeleteGroupPermissions(studyId, userId, group);
    }

    @Override
    public void checkUpdateGroupPermissions(long studyId, String userId, String group, GroupParams params) throws CatalogException {
        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (userId.equals(ownerId)) {
            // Granted permission but check it is a valid action
            if (group.equals(MEMBERS_GROUP)
                    && (params.getAction() != GroupParams.Action.ADD && params.getAction() != GroupParams.Action.REMOVE)) {
                throw new CatalogAuthorizationException("Only ADD or REMOVE actions are accepted for @members group.");
            }
            return;
        }

        if (group.equals(ADMINS_GROUP)) {
            throw new CatalogAuthorizationException("Only the owner of the study can assign/remove users to the administrative group.");
        }

        if (!userId.equals(ADMIN) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only administrative users are allowed to assign/remove users to groups.");
        }

        // Check it is a valid action
        if (group.equals(MEMBERS_GROUP)
                && (params.getAction() != GroupParams.Action.ADD && params.getAction() != GroupParams.Action.REMOVE)) {
            throw new CatalogAuthorizationException("Only ADD or REMOVE actions are accepted for @members group.");
        }
    }

    @Override
    public void checkNotAssigningPermissionsToAdminsGroup(List<String> members) throws CatalogException {
        for (String member : members) {
            if (member.equals(ADMINS_GROUP)) {
                throw new CatalogAuthorizationException("Assigning permissions to @admins group is not allowed.");
            }
        }
    }

    @Override
    public void checkCanAssignOrSeePermissions(long studyId, String userId) throws CatalogException {
        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (!ownerId.equals(userId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only owners or administrative users are allowed to assign permissions");
        }
    }

    @Override
    public void checkCanCreateUpdateDeleteVariableSets(long studyId, String userId) throws CatalogException {
        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (!ownerId.equals(userId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only owners or administrative users are allowed to create/update/delete variable "
                    + "sets");
        }
    }

    @Override
    public Boolean checkIsAdmin(String user) {
        return user.equals(ADMIN);
    }

    @Override
    public Boolean checkIsOwnerOrAdmin(long studyId, String userId) throws CatalogException {
        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (!ownerId.equals(userId) && !isAdministrativeUser(studyId, userId)) {
            return false;
        }

        return true;
    }


    private boolean isAdministrativeUser(long studyId, String user) throws CatalogException {
        DataResult<Group> groupBelonging = getGroupBelonging(studyId, user);
        for (Group group : groupBelonging.getResults()) {
            if (group.getId().equals(ADMINS_GROUP)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkFilePermission(long studyId, long fileId, String userId, FileAclEntry.FilePermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), fileId)
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
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
            case VIEW_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.VIEW_FILE_ANNOTATIONS;
                break;
            case WRITE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.WRITE_FILE_ANNOTATIONS;
                break;
            case DELETE_ANNOTATIONS:
                studyPermission = StudyAclEntry.StudyPermissions.DELETE_FILE_ANNOTATIONS;
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
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleId)
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
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
                .append(IndividualDBAdaptor.QueryParams.UID.key(), individualId)
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
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
                .append(JobDBAdaptor.QueryParams.UID.key(), jobId)
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
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
                .append(CohortDBAdaptor.QueryParams.UID.key(), cohortId)
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
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
    public void checkPanelPermission(long studyId, long panelId, String userId, PanelAclEntry.PanelPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.UID.key(), panelId)
                .append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
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
                .append(FamilyDBAdaptor.QueryParams.UID.key(), familyId)
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
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
                .append(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), analysisId)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
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
            default:
                throw new CatalogAuthorizationException("Permission " + permission.toString() + " not found");
        }

        if (checkUserPermission(userId, query, studyPermission, clinicalAnalysisDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "ClinicalAnalysis", analysisId, null);
    }

    @Override
    public DataResult<StudyAclEntry> getAllStudyAcls(String userId, long studyId) throws CatalogException {
        checkCanAssignOrSeePermissions(studyId, userId);
        return aclDBAdaptor.get(studyId, null, Entity.STUDY);
    }

    @Override
    public DataResult<StudyAclEntry> getStudyAcl(String userId, long studyId, String member) throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            DataResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(studyId, Arrays.asList(member), Entity.STUDY);
    }

    @Override
    public DataResult<SampleAclEntry> getAllSampleAcls(long studyId, long sampleId, String userId) throws CatalogException {
        checkCanAssignOrSeePermissions(studyId, userId);
        return aclDBAdaptor.get(sampleId, null, Entity.SAMPLE);
    }

    @Override
    public DataResult<SampleAclEntry> getSampleAcl(long studyId, long sampleId, String userId, String member) throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            DataResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(sampleId, Arrays.asList(member), Entity.SAMPLE);
    }

    @Override
    public void resetPermissionsFromAllEntities(long studyId, List<String> members) throws CatalogException {
        aclDBAdaptor.resetMembersFromAllEntries(studyId, members);
    }

    @Override
    public DataResult<FileAclEntry> getAllFileAcls(long studyId, long fileId, String userId, boolean checkPermission)
            throws CatalogException {
        if (checkPermission) {
            checkCanAssignOrSeePermissions(studyId, userId);
        }
        return aclDBAdaptor.get(fileId, null, Entity.FILE);
    }

    @Override
    public DataResult<FileAclEntry> getFileAcl(long studyId, long fileId, String userId, String member) throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            DataResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(fileId, Arrays.asList(member), Entity.FILE);
    }

    @Override
    public DataResult<IndividualAclEntry> getAllIndividualAcls(long studyId, long individualId, String userId) throws CatalogException {
        checkCanAssignOrSeePermissions(studyId, userId);
        return aclDBAdaptor.get(individualId, null, Entity.INDIVIDUAL);
    }

    @Override
    public DataResult<IndividualAclEntry> getIndividualAcl(long studyId, long individualId, String userId, String member)
            throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            DataResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(individualId, Arrays.asList(member), Entity.INDIVIDUAL);
    }

    @Override
    public DataResult<CohortAclEntry> getAllCohortAcls(long studyId, long cohortId, String userId) throws CatalogException {
        checkCanAssignOrSeePermissions(studyId, userId);
        return aclDBAdaptor.get(cohortId, null, Entity.COHORT);
    }

    @Override
    public DataResult<CohortAclEntry> getCohortAcl(long studyId, long cohortId, String userId, String member) throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            DataResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(cohortId, Arrays.asList(member), Entity.COHORT);
    }

    @Override
    public DataResult<PanelAclEntry> getAllPanelAcls(long studyId, long panelId, String userId) throws CatalogException {
        checkCanAssignOrSeePermissions(studyId, userId);
        return aclDBAdaptor.get(panelId, null, Entity.DISEASE_PANEL);
    }

    @Override
    public DataResult<PanelAclEntry> getPanelAcl(long studyId, long panelId, String userId, String member) throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        return aclDBAdaptor.get(panelId, Arrays.asList(member), Entity.DISEASE_PANEL);
    }

    @Override
    public DataResult<JobAclEntry> getAllJobAcls(long studyId, long jobId, String userId) throws CatalogException {
        checkCanAssignOrSeePermissions(studyId, userId);
        return aclDBAdaptor.get(jobId, null, Entity.JOB);
    }

    @Override
    public DataResult<JobAclEntry> getJobAcl(long studyId, long jobId, String userId, String member) throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        // This code is to calculate the total effective permissions
        // TODO: Take into account effective permissions at some point
//        List<String> members = new ArrayList<>(2);
//        members.add(member);
//        if (!member.startsWith("@") && !member.equalsIgnoreCase("anonymous") && !member.equals("*")) {
//            DataResult<Group> groupBelonging = getGroupBelonging(studyId, member);
//            if (groupBelonging != null && groupBelonging.getNumResults() == 1) {
//                members.add(groupBelonging.first().getName());
//            }
//        }

        return aclDBAdaptor.get(jobId, Arrays.asList(member), Entity.JOB);
    }

    @Override
    public DataResult<FamilyAclEntry> getAllFamilyAcls(long studyId, long familyId, String userId) throws CatalogException {
        checkCanAssignOrSeePermissions(studyId, userId);
        return aclDBAdaptor.get(familyId, null, Entity.FAMILY);
    }

    @Override
    public DataResult<FamilyAclEntry> getFamilyAcl(long studyId, long familyId, String userId, String member) throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        return aclDBAdaptor.get(familyId, Arrays.asList(member), Entity.FAMILY);
    }

    @Override
    public DataResult<ClinicalAnalysisAclEntry> getAllClinicalAnalysisAcls(long studyId, long clinicalAnalysisId, String userId)
            throws CatalogException {
        checkCanAssignOrSeePermissions(studyId, userId);
        return aclDBAdaptor.get(clinicalAnalysisId, null, Entity.CLINICAL_ANALYSIS);
    }

    @Override
    public DataResult<ClinicalAnalysisAclEntry> getClinicalAnalysisAcl(long studyId, long clinicalAnalysisId, String userId, String member)
            throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            checkAskingOwnPermissions(userId, member, studyId);
        }

        return aclDBAdaptor.get(clinicalAnalysisId, Arrays.asList(member), Entity.CLINICAL_ANALYSIS);
    }

    private void checkAskingOwnPermissions(String userId, String member, long studyId) throws CatalogException {
        if (member.startsWith("@")) { //group
            // If the userId does not belong to the group...
            DataResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
            if (groupBelonging.getNumResults() != 1 || !groupBelonging.first().getId().equals(member)) {
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
    public List<DataResult<StudyAclEntry>> setStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException {
        aclDBAdaptor.setToMembers(studyIds, members, permissions);
        return aclDBAdaptor.get(studyIds, members, Entity.STUDY);
    }

    @Override
    public List<DataResult<StudyAclEntry>> addStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException {
        aclDBAdaptor.addToMembers(studyIds, members, permissions);
        return aclDBAdaptor.get(studyIds, members, Entity.STUDY);
    }

    @Override
    public List<DataResult<StudyAclEntry>> removeStudyAcls(List<Long> studyIds, List<String> members, @Nullable List<String> permissions)
            throws CatalogException {
        aclDBAdaptor.removeFromMembers(studyIds, members, permissions, Entity.STUDY);
        return aclDBAdaptor.get(studyIds, members, Entity.STUDY);
    }

    private <E extends AbstractAclEntry> List<DataResult<E>> getAcls(List<Long> ids, List<String> members, Entity entity)
            throws CatalogException {
        return aclDBAdaptor.get(ids, members, entity);
    }

    public <E extends AbstractAclEntry> List<DataResult<E>> setAcls(long studyId, List<Long> ids, List<Long> ids2, List<String> members,
                                                                     List<String> permissions, Entity entity, Entity entity2)
            throws CatalogException {
        if (ids == null || ids.isEmpty()) {
            logger.warn("Missing identifiers to set acls");
            return Collections.emptyList();
        }

        long startTime = System.currentTimeMillis();
        aclDBAdaptor.setToMembers(studyId, ids, ids2, members, permissions, entity, entity2);

        return getAclResultList(ids, members, entity, startTime);
    }

    @Override
    public <E extends AbstractAclEntry> List<DataResult<E>> addAcls(long studyId, List<Long> ids, List<Long> ids2, List<String> members,
                                                                     List<String> permissions, Entity entity, Entity entity2)
            throws CatalogException {
        if (ids == null || ids.isEmpty()) {
            logger.warn("Missing identifiers to add acls");
            return Collections.emptyList();
        }

        long startTime = System.currentTimeMillis();
        aclDBAdaptor.addToMembers(studyId, ids, ids2, members, permissions, entity, entity2);

        return getAclResultList(ids, members, entity, startTime);
    }

    @Override
    public <E extends AbstractAclEntry> List<DataResult<E>> removeAcls(List<Long> ids, List<Long> ids2, List<String> members,
                                                                        @Nullable List<String> permissions, Entity entity, Entity entity2)
            throws CatalogException {
        if (ids == null || ids.isEmpty()) {
            logger.warn("Missing identifiers to remove acls");
            return Collections.emptyList();
        }

        long startTime = System.currentTimeMillis();
        aclDBAdaptor.removeFromMembers(ids, ids2, members, permissions, entity, entity2);

        return getAclResultList(ids, members, entity, startTime);
    }

    <E extends AbstractAclEntry> List<DataResult<E>> getAclResultList(List<Long> ids, List<String> members, Entity entity, long startTime)
            throws CatalogException {
        int dbTime = (int) (System.currentTimeMillis() - startTime);

        List<DataResult<E>> aclResultList = getAcls(ids, members, entity);

        for (DataResult<E> aclEntryDataResult : aclResultList) {
            aclEntryDataResult.setTime(aclEntryDataResult.getTime() + dbTime);
        }
        return aclResultList;
    }

    @Override
    public <E extends AbstractAclEntry> List<DataResult<E>> replicateAcls(long studyId, List<Long> ids, List<E> aclEntries,
                                                                           Entity entity) throws CatalogException {
        if (ids == null || ids.isEmpty()) {
            logger.warn("Missing identifiers to set acls");
            return Collections.emptyList();
        }

        long startTime = System.currentTimeMillis();
        aclDBAdaptor.setAcls(ids, aclEntries, entity);
        int dbTime = (int) (System.currentTimeMillis() - startTime);

        List<DataResult<E>> aclResultList = getAcls(ids, null, entity);

        for (DataResult<E> aclEntryDataResult : aclResultList) {
            aclEntryDataResult.setTime(aclEntryDataResult.getTime() + dbTime);
        }

        return aclResultList;
    }

    @Override
    public void applyPermissionRule(long studyId, PermissionRule permissionRule, Study.Entity entry) throws CatalogException {
        // 1. We obtain which of those members are actually users to add them to the @members group automatically
        List<String> userList = permissionRule.getMembers().stream()
                .filter(member -> !member.startsWith("@"))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(userList)) {
            // We first add the member to the @members group in case they didn't belong already
            studyDBAdaptor.addUsersToGroup(studyId, MEMBERS_GROUP, userList);
        }

        // 2. We can apply the permission rules
        aclDBAdaptor.applyPermissionRules(studyId, permissionRule, entry);
    }

    @Override
    public void removePermissionRuleAndRemovePermissions(Study study, String permissionRuleId, Study.Entity entry)
            throws CatalogException {
        ParamUtils.checkObj(permissionRuleId, "PermissionRule id");
        ParamUtils.checkObj(entry, "Entity");

        aclDBAdaptor.removePermissionRuleAndRemovePermissions(study, permissionRuleId, entry);
    }

    @Override
    public void removePermissionRuleAndRestorePermissions(Study study, String permissionRuleId, Study.Entity entry)
            throws CatalogException {
        ParamUtils.checkObj(permissionRuleId, "PermissionRule id");
        ParamUtils.checkObj(entry, "Entity");

        aclDBAdaptor.removePermissionRuleAndRestorePermissions(study, permissionRuleId, entry);
    }

    @Override
    public void removePermissionRule(long studyId, String permissionRuleId, Study.Entity entry) throws CatalogException {
        ParamUtils.checkObj(permissionRuleId, "PermissionRule id");
        ParamUtils.checkObj(entry, "Entity");

        aclDBAdaptor.removePermissionRule(studyId, permissionRuleId, entry);
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
    DataResult<Group> getGroupBelonging(long studyId, List<String> members) throws CatalogException {
        return studyDBAdaptor.getGroup(studyId, null, members);
    }

    DataResult<Group> getGroupBelonging(long studyId, String members) throws CatalogException {
        return getGroupBelonging(studyId, Arrays.asList(members.split(",")));
    }

    public static void checkPermissions(List<String> permissions, Function<String, Enum> getValue) throws CatalogException {
        for (String permission : permissions) {
            try {
                getValue.apply(permission);
            } catch (IllegalArgumentException e) {
                throw new CatalogAuthorizationException("The permission " + permission + " is not a correct permission.");
            }
        }
    }

}
