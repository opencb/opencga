/*
 * Copyright 2015-2020 OpenCB
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

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
import org.opencb.opencga.core.models.cohort.CohortPermissions;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.FamilyPermissions;
import org.opencb.opencga.core.models.file.FilePermissions;
import org.opencb.opencga.core.models.individual.IndividualPermissions;
import org.opencb.opencga.core.models.job.JobPermissions;
import org.opencb.opencga.core.models.panel.PanelPermissions;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 12/05/16.
 */
public class CatalogAuthorizationManager implements AuthorizationManager {

    public static final String MEMBERS_GROUP = ParamConstants.MEMBERS_GROUP;
    public static final String ADMINS_GROUP = ParamConstants.ADMINS_GROUP;
    private static final String OPENCGA = ParamConstants.OPENCGA_USER_ID;

    private final Logger logger;

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

    private final boolean openRegister;

    private final AuthorizationDBAdaptor aclDBAdaptor;

    public CatalogAuthorizationManager(DBAdaptorFactory dbFactory, Configuration configuration)
            throws CatalogDBException {
        this.logger = LoggerFactory.getLogger(CatalogAuthorizationManager.class);
        this.aclDBAdaptor = new AuthorizationMongoDBAdaptor(dbFactory, configuration);

        this.openRegister = configuration.isOpenRegister();

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
        if (isInstallationAdministrator(userId)) {
            return;
        }
        if (projectDBAdaptor.getOwnerId(projectId).equals(userId)) {
            return;
        }

        // Only members of any study belonging to the project can view the project
        final Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId)
                .append(StudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), userId);

        if (studyDBAdaptor.count(query).getNumMatches() > 0) {
            return;
        }

        throw CatalogAuthorizationException.deny(userId, "view", "Project", projectId, null);
    }

    @Override
    public void checkStudyPermission(long studyId, String userId, StudyPermissions.Permissions permission) throws CatalogException {
        checkStudyPermission(studyId, userId, permission, permission.toString());
    }

    @Override
    public void checkStudyPermission(long studyId, String userId, StudyPermissions.Permissions permission, String message)
            throws CatalogException {
        if (isInstallationAdministrator(userId)) {
            return;
        } else {
            if (studyDBAdaptor.hasStudyPermission(studyId, userId, permission)) {
                return;
            }
        }
        throw CatalogAuthorizationException.deny(userId, message, "Study", studyId, null);
    }

    @Override
    public void checkCanEditStudy(long studyId, String userId) throws CatalogException {
        if (isInstallationAdministrator(userId)) {
            return;
        }

        String ownerId = studyDBAdaptor.getOwnerId(studyId);
        if (!ownerId.equals(userId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only owners or administrative users are allowed to modify a study");
        }
    }

    @Override
    public void checkCanViewStudy(long studyId, String userId) throws CatalogException {
        if (isInstallationAdministrator(userId)) {
            return;
        }

        String ownerId = studyDBAdaptor.getOwnerId(studyId);
        if (ownerId.equals(userId)) {
            return;
        }

        OpenCGAResult<Group> groupBelonging = getGroupBelonging(studyId, userId);
        if (groupBelonging.getNumResults() == 0) {
            throw new CatalogAuthorizationException("Only the members of the study are allowed to see it");
        }
    }

    @Override
    public void checkCanUpdatePermissionRules(long studyId, String userId) throws CatalogException {
        if (isInstallationAdministrator(userId)) {
            return;
        }

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

        if (isInstallationAdministrator(userId)) {
            return;
        }

        String ownerId = studyDBAdaptor.getOwnerId(studyId);
        if (!userId.equals(ownerId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only administrative users are allowed to create/remove groups.");
        }
    }

    @Override
    public void checkSyncGroupPermissions(long studyId, String userId, String group) throws CatalogException {
        checkCreateDeleteGroupPermissions(studyId, userId, group);
    }

    @Override
    public void checkUpdateGroupPermissions(long studyId, String userId, String group, ParamUtils.BasicUpdateAction action)
            throws CatalogException {
        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (userId.equals(ownerId)) {
            // Granted permission but check it is a valid action
            if (group.equals(MEMBERS_GROUP)
                    && (action != ParamUtils.BasicUpdateAction.ADD && action != ParamUtils.BasicUpdateAction.REMOVE)) {
                throw new CatalogAuthorizationException("Only ADD or REMOVE actions are accepted for @members group.");
            }
            return;
        }

        if (group.equals(ADMINS_GROUP)) {
            throw new CatalogAuthorizationException("Only the owner of the study can assign/remove users to the administrative group.");
        }

        if (!isInstallationAdministrator(userId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only administrative users are allowed to assign/remove users to groups.");
        }

        // Check it is a valid action
        if (group.equals(MEMBERS_GROUP) && (action != ParamUtils.BasicUpdateAction.ADD && action != ParamUtils.BasicUpdateAction.REMOVE)) {
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
        if (isInstallationAdministrator(userId)) {
            return;
        }

        String ownerId = studyDBAdaptor.getOwnerId(studyId);
        if (!ownerId.equals(userId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only owners or administrative users are allowed to assign or see all permissions");
        }
    }

    @Override
    public void checkCanCreateUpdateDeleteVariableSets(long studyId, String userId) throws CatalogException {
        if (isInstallationAdministrator(userId)) {
            return;
        }

        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (!ownerId.equals(userId) && !isAdministrativeUser(studyId, userId)) {
            throw new CatalogAuthorizationException("Only owners or administrative users are allowed to create/update/delete variable "
                    + "sets");
        }
    }

    @Override
    public Boolean isInstallationAdministrator(String user) {
        return OPENCGA.equals(user);
    }

    @Override
    public void checkIsInstallationAdministrator(String user) throws CatalogException {
        if (!isInstallationAdministrator(user)) {
            throw new CatalogAuthorizationException("Only ADMINISTRATOR users are allowed to perform this action");
        }
    }

    @Override
    public void checkIsOwnerOrAdmin(long studyId, String userId) throws CatalogException {
        if (isInstallationAdministrator(userId)) {
            return;
        }

        if (!isOwnerOrAdmin(studyId, userId)) {
            throw new CatalogAuthorizationException("Only owners or administrative users are allowed to perform this action");
        }
    }

    @Override
    public Boolean isOwnerOrAdmin(long studyId, String userId) throws CatalogException {
        String ownerId = studyDBAdaptor.getOwnerId(studyId);

        if (!ownerId.equals(userId) && !isAdministrativeUser(studyId, userId)) {
            return false;
        }
        return true;
    }


    private boolean isAdministrativeUser(long studyId, String user) throws CatalogException {
        OpenCGAResult<Group> groupBelonging = getGroupBelonging(studyId, user);
        for (Group group : groupBelonging.getResults()) {
            if (group.getId().equals(ADMINS_GROUP)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkFilePermission(long studyId, long fileId, String userId, FilePermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), fileId)
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(userId, query, fileDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "File", fileId, null);
    }

    private boolean checkUserPermission(String userId, Query query, CoreDBAdaptor dbAdaptor) throws CatalogException {
        if (isInstallationAdministrator(userId)) {
            return true;
        } else {
            if (dbAdaptor.count(query, userId).getNumMatches() == 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkSamplePermission(long studyId, long sampleId, String userId, SamplePermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleId)
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(userId, query, sampleDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Sample", sampleId, null);
    }

    @Override
    public void checkIndividualPermission(long studyId, long individualId, String userId,
                                          IndividualPermissions permission) throws CatalogException {
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.UID.key(), individualId)
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(userId, query, individualDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Individual", individualId, null);
    }

    @Override
    public void checkJobPermission(long studyId, long jobId, String userId, JobPermissions permission) throws CatalogException {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.UID.key(), jobId)
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(userId, query, jobDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Job", jobId, null);
    }

    @Override
    public void checkCohortPermission(long studyId, long cohortId, String userId, CohortPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.UID.key(), cohortId)
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(userId, query, cohortDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Cohort", cohortId, null);

    }

    @Override
    public void checkPanelPermission(long studyId, long panelId, String userId, PanelPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.UID.key(), panelId)
                .append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(userId, query, panelDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Panel", panelId, null);
    }

    @Override
    public void checkFamilyPermission(long studyId, long familyId, String userId, FamilyPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.UID.key(), familyId)
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(userId, query, familyDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Family", familyId, null);

    }

    @Override
    public void checkClinicalAnalysisPermission(long studyId, long analysisId, String userId,
                                                ClinicalAnalysisPermissions permission) throws CatalogException {
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), analysisId)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(userId, query, clinicalAnalysisDBAdaptor)) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "ClinicalAnalysis", analysisId, null);
    }

    @Override
    public OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getAllStudyAcls(String userId, long studyId)
            throws CatalogException {
        checkCanAssignOrSeePermissions(studyId, userId);
        return aclDBAdaptor.get(studyId, null, null, Enums.Resource.STUDY, StudyPermissions.Permissions.class);
    }

    @Override
    public OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(String userId, long studyUid, List<String> members)
            throws CatalogException {
        checkCanSeePermissions(userId, studyUid, members);
        Map<String, List<String>> userGroups = extractUserGroups(studyUid, members);
        return aclDBAdaptor.get(studyUid, members, userGroups, Enums.Resource.STUDY, StudyPermissions.Permissions.class);
    }

    @Override
    public OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(List<Long> studyUids, List<String> members)
            throws CatalogException {
        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> result = OpenCGAResult.empty();
        for (Long studyUid : studyUids) {
            Map<String, List<String>> userGroups = extractUserGroups(studyUid, members);
            result.append(aclDBAdaptor.get(studyUid, members, userGroups, Enums.Resource.STUDY, StudyPermissions.Permissions.class));
        }
        return result;
    }

    @Override
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String userId, long studyUid, List<Long> resourceUids,
                                                                     List<String> members, Enums.Resource resource, Class<T> clazz)
            throws CatalogException {
        checkCanSeePermissions(userId, studyUid, members);
        Map<String, List<String>> userGroups = extractUserGroups(studyUid, members);
        return aclDBAdaptor.get(resourceUids, members, userGroups, resource, clazz);
    }

    @Override
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String userId, long studyUid, List<Long> resourceUids,
                                                                     Enums.Resource resource, Class<T> clazz) throws CatalogException {
        checkCanAssignOrSeePermissions(studyUid, userId);
        return aclDBAdaptor.get(resourceUids, null, null, resource, clazz);
    }

    @Override
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(long studyUid, List<Long> resourceUids, Enums.Resource resource,
                                                                      Class<T> clazz) throws CatalogException {
        return aclDBAdaptor.get(resourceUids, null, null, resource, clazz);
    }

    @Override
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(long studyUid, List<Long> resourceUids, List<String> members,
                                                                      Enums.Resource resource, Class<T> clazz) throws CatalogException {
        Map<String, List<String>> userGroups = extractUserGroups(studyUid, members);
        return aclDBAdaptor.get(resourceUids, members, userGroups, resource, clazz);
    }

    private void checkCanSeePermissions(String userId, long studyId, List<String> members) throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            for (String member : members) {
                checkAskingOwnPermissions(userId, member, studyId);
            }
        }
    }

    private Map<String, List<String>> extractUserGroups(long studyId, List<String> members) throws CatalogException {
        Map<String, List<String>> userGroups = new HashMap<>();
        List<String> userList = members.stream().filter(m -> !m.startsWith("@")).collect(Collectors.toList());
        if (!userList.isEmpty()) {
            // If member is a user, we will also store the groups the user might belong to fetch those permissions as well
            OpenCGAResult<Group> groups = getGroupBelonging(studyId, userList);

            // Fill in map with the results
            for (String user : userList) {
                userGroups.put(user, new LinkedList<>());
            }
            for (Group group : groups.getResults()) {
                for (String user : userList) {
                    if (group.getUserIds().contains(user)) {
                        userGroups.get(user).add(group.getId());
                    }
                }
            }
        }
        return userGroups;
    }

    @Override
    public void resetPermissionsFromAllEntities(long studyId, List<String> members) throws CatalogException {
        aclDBAdaptor.resetMembersFromAllEntries(studyId, members);
    }

    private void checkAskingOwnPermissions(String userId, String member, long studyId) throws CatalogException {
        if (member.startsWith("@")) { //group
            // If the userId does not belong to the group...
            OpenCGAResult<Group> groupsBelonging = getGroupBelonging(studyId, userId);
            for (Group group : groupsBelonging.getResults()) {
                if (group.getId().equals(member)) {
                    return;
                }
            }
            throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                    + member);
        } else {
            // If the userId asking to see the permissions is not asking to see their own permissions
            if (!userId.equals(member)) {
                throw new CatalogAuthorizationException("The user " + userId + " does not have permissions to see the ACLs of "
                        + member);
            }
        }
    }

    @Override
    public void setStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogException {
        aclDBAdaptor.setToMembers(studyIds, members, getImplicitPermissions(permissions, Enums.Resource.STUDY));
    }

    @Override
    public void addStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogException {
        aclDBAdaptor.addToMembers(studyIds, members, getImplicitPermissions(permissions, Enums.Resource.STUDY));
    }

    @Override
    public void removeStudyAcls(List<Long> studyIds, List<String> members, @Nullable List<String> permissions) throws CatalogException {
        removeAcls(members, new CatalogAclParams(studyIds, permissions, Enums.Resource.STUDY));
    }

    @Override
    public void setAcls(long studyUid, List<String> members, List<CatalogAclParams> aclParams) throws CatalogException {
        setImplicitPermissions(aclParams);
        aclDBAdaptor.setToMembers(studyUid, members, aclParams);
    }

    @Override
    public void addAcls(long studyId, List<String> members, List<CatalogAclParams> aclParams) throws CatalogException {
        setImplicitPermissions(aclParams);
        aclDBAdaptor.addToMembers(studyId, members, aclParams);
    }

    @Override
    public void removeAcls(List<String> members, List<CatalogAclParams> aclParams) throws CatalogException {
        setDependentPermissions(aclParams);
        aclDBAdaptor.removeFromMembers(members, aclParams);
    }

    private void setDependentPermissions(List<CatalogAclParams> aclParams) throws CatalogAuthorizationException {
        for (CatalogAclParams aclParam : aclParams) {
            if (aclParam.getPermissions() != null) {
                Set<String> allPermissions = new HashSet<>(aclParam.getPermissions());
                switch (aclParam.getResource()) {
                    case STUDY:
                        allPermissions.addAll(aclParam.getPermissions()
                                .stream()
                                .map(StudyPermissions.Permissions::valueOf)
                                .map(StudyPermissions.Permissions::getDependentPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                        break;
                    case FILE:
                        allPermissions.addAll(aclParam.getPermissions()
                                .stream()
                                .map(FilePermissions::valueOf)
                                .map(FilePermissions::getDependentPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                        break;
                    case SAMPLE:
                        allPermissions.addAll(aclParam.getPermissions()
                                .stream()
                                .map(SamplePermissions::valueOf)
                                .map(SamplePermissions::getDependentPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                        break;
                    case JOB:
                        allPermissions.addAll(aclParam.getPermissions()
                                .stream()
                                .map(JobPermissions::valueOf)
                                .map(JobPermissions::getDependentPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                        break;
                    case INDIVIDUAL:
                        allPermissions.addAll(aclParam.getPermissions()
                                .stream()
                                .map(IndividualPermissions::valueOf)
                                .map(IndividualPermissions::getDependentPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                        break;
                    case COHORT:
                        allPermissions.addAll(aclParam.getPermissions()
                                .stream()
                                .map(CohortPermissions::valueOf)
                                .map(CohortPermissions::getDependentPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                        break;
                    case DISEASE_PANEL:
                        allPermissions.addAll(aclParam.getPermissions()
                                .stream()
                                .map(PanelPermissions::valueOf)
                                .map(PanelPermissions::getDependentPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                        break;
                    case FAMILY:
                        allPermissions.addAll(aclParam.getPermissions()
                                .stream()
                                .map(FamilyPermissions::valueOf)
                                .map(FamilyPermissions::getDependentPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                        break;
                    case CLINICAL_ANALYSIS:
                        allPermissions.addAll(aclParam.getPermissions()
                                .stream()
                                .map(ClinicalAnalysisPermissions::valueOf)
                                .map(ClinicalAnalysisPermissions::getDependentPermissions)
                                .flatMap(List::stream)
                                .collect(Collectors.toSet())
                                .stream().map(Enum::name)
                                .collect(Collectors.toSet())
                        );
                        break;
                    default:
                        throw new CatalogAuthorizationException("Unexpected resource '" + aclParam.getResource() + "'");
                }
                logger.debug("Permissions sent by the user '{}'. Complete list containing dependent permissions '{}'.",
                        aclParam.getPermissions(), allPermissions);

                aclParam.setPermissions(new ArrayList<>(allPermissions));
            }
        }
    }

    private void setImplicitPermissions(List<CatalogAclParams> aclParams) throws CatalogAuthorizationException {
        for (CatalogAclParams aclParam : aclParams) {
            Set<String> allPermissions = new HashSet<>(aclParam.getPermissions());
            switch (aclParam.getResource()) {
                case STUDY:
                    allPermissions.addAll(aclParam.getPermissions()
                            .stream()
                            .map(StudyPermissions.Permissions::valueOf)
                            .map(StudyPermissions.Permissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                    break;
                case FILE:
                    allPermissions.addAll(aclParam.getPermissions()
                            .stream()
                            .map(FilePermissions::valueOf)
                            .map(FilePermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                    break;
                case SAMPLE:
                    allPermissions.addAll(aclParam.getPermissions()
                            .stream()
                            .map(SamplePermissions::valueOf)
                            .map(SamplePermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                    break;
                case JOB:
                    allPermissions.addAll(aclParam.getPermissions()
                            .stream()
                            .map(JobPermissions::valueOf)
                            .map(JobPermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                    break;
                case INDIVIDUAL:
                    allPermissions.addAll(aclParam.getPermissions()
                            .stream()
                            .map(IndividualPermissions::valueOf)
                            .map(IndividualPermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                    break;
                case COHORT:
                    allPermissions.addAll(aclParam.getPermissions()
                            .stream()
                            .map(CohortPermissions::valueOf)
                            .map(CohortPermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                    break;
                case DISEASE_PANEL:
                    allPermissions.addAll(aclParam.getPermissions()
                            .stream()
                            .map(PanelPermissions::valueOf)
                            .map(PanelPermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                    break;
                case FAMILY:
                    allPermissions.addAll(aclParam.getPermissions()
                            .stream()
                            .map(FamilyPermissions::valueOf)
                            .map(FamilyPermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                    break;
                case CLINICAL_ANALYSIS:
                    allPermissions.addAll(aclParam.getPermissions()
                            .stream()
                            .map(ClinicalAnalysisPermissions::valueOf)
                            .map(ClinicalAnalysisPermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                    break;
                default:
                    throw new CatalogAuthorizationException("Unexpected resource '" + aclParam.getResource() + "'");
            }

            logger.debug("Permissions sent by the user '{}'. Complete list containing implicit permissions '{}'.", aclParam,
                    allPermissions);
            aclParam.setPermissions(new ArrayList<>(allPermissions));
        }
    }

    private List<String> getImplicitPermissions(List<String> permissions, Enums.Resource resource) throws CatalogAuthorizationException {
        Set<String> allPermissions = new HashSet<>(permissions);
        switch (resource) {
            case STUDY:
                allPermissions.addAll(permissions
                        .stream()
                        .map(StudyPermissions.Permissions::valueOf)
                        .map(StudyPermissions.Permissions::getImplicitPermissions)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream().map(Enum::name)
                        .collect(Collectors.toSet())
                );
                break;
            case FILE:
                allPermissions.addAll(permissions
                        .stream()
                        .map(FilePermissions::valueOf)
                        .map(FilePermissions::getImplicitPermissions)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream().map(Enum::name)
                        .collect(Collectors.toSet())
                );
                break;
            case SAMPLE:
                allPermissions.addAll(permissions
                        .stream()
                        .map(SamplePermissions::valueOf)
                        .map(SamplePermissions::getImplicitPermissions)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream().map(Enum::name)
                        .collect(Collectors.toSet())
                );
                break;
            case JOB:
                allPermissions.addAll(permissions
                        .stream()
                        .map(JobPermissions::valueOf)
                        .map(JobPermissions::getImplicitPermissions)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream().map(Enum::name)
                        .collect(Collectors.toSet())
                );
                break;
            case INDIVIDUAL:
                allPermissions.addAll(permissions
                        .stream()
                        .map(IndividualPermissions::valueOf)
                        .map(IndividualPermissions::getImplicitPermissions)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream().map(Enum::name)
                        .collect(Collectors.toSet())
                );
                break;
            case COHORT:
                allPermissions.addAll(permissions
                        .stream()
                        .map(CohortPermissions::valueOf)
                        .map(CohortPermissions::getImplicitPermissions)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream().map(Enum::name)
                        .collect(Collectors.toSet())
                );
                break;
            case DISEASE_PANEL:
                allPermissions.addAll(permissions
                        .stream()
                        .map(PanelPermissions::valueOf)
                        .map(PanelPermissions::getImplicitPermissions)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream().map(Enum::name)
                        .collect(Collectors.toSet())
                );
                break;
            case FAMILY:
                allPermissions.addAll(permissions
                        .stream()
                        .map(FamilyPermissions::valueOf)
                        .map(FamilyPermissions::getImplicitPermissions)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream().map(Enum::name)
                        .collect(Collectors.toSet())
                );
                break;
            case CLINICAL_ANALYSIS:
                allPermissions.addAll(permissions
                        .stream()
                        .map(ClinicalAnalysisPermissions::valueOf)
                        .map(ClinicalAnalysisPermissions::getImplicitPermissions)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet())
                        .stream().map(Enum::name)
                        .collect(Collectors.toSet())
                );
                break;
            default:
                throw new CatalogAuthorizationException("Unexpected resource '" + resource + "'");
        }

        logger.debug("Permissions sent by the user '{}'. Complete list containing implicit permissions '{}'.", permissions, allPermissions);

        return new ArrayList<>(allPermissions);
    }

    @Override
    public void replicateAcls(List<Long> uids, AclEntryList<?> aclEntryList, Enums.Resource resource) throws CatalogException {
        if (CollectionUtils.isEmpty(uids)) {
            throw new CatalogDBException("Missing identifiers to set acls");
        }
        if (CollectionUtils.isEmpty(aclEntryList)) {
            return;
        }
        aclDBAdaptor.setAcls(uids, aclEntryList, resource);
    }

    @Override
    public void applyPermissionRule(long studyId, PermissionRule permissionRule, Enums.Entity entry) throws CatalogException {
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
    public void removePermissionRuleAndRemovePermissions(Study study, String permissionRuleId, Enums.Entity entry)
            throws CatalogException {
        ParamUtils.checkObj(permissionRuleId, "PermissionRule id");
        ParamUtils.checkObj(entry, "Entity");

        aclDBAdaptor.removePermissionRuleAndRemovePermissions(study, permissionRuleId, entry);
    }

    @Override
    public void removePermissionRuleAndRestorePermissions(Study study, String permissionRuleId, Enums.Entity entry)
            throws CatalogException {
        ParamUtils.checkObj(permissionRuleId, "PermissionRule id");
        ParamUtils.checkObj(entry, "Entity");

        aclDBAdaptor.removePermissionRuleAndRestorePermissions(study, permissionRuleId, entry);
    }

    @Override
    public void removePermissionRule(long studyId, String permissionRuleId, Enums.Entity entry) throws CatalogException {
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
    OpenCGAResult<Group> getGroupBelonging(long studyId, List<String> members) throws CatalogException {
        return studyDBAdaptor.getGroup(studyId, null, members);
    }

    OpenCGAResult<Group> getGroupBelonging(long studyId, String members) throws CatalogException {
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
