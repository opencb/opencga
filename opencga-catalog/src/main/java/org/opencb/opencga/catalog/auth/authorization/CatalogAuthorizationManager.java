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
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.OrganizationManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.Acl;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisPermissions;
import org.opencb.opencga.core.models.cohort.CohortPermissions;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.FamilyPermissions;
import org.opencb.opencga.core.models.file.FilePermissions;
import org.opencb.opencga.core.models.individual.IndividualPermissions;
import org.opencb.opencga.core.models.job.JobPermissions;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.panel.PanelPermissions;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.PermissionRule;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
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

    private final Logger logger;

    private final DBAdaptorFactory dbAdaptorFactory;
    private final AuthorizationDBAdaptorFactory authorizationDBAdaptorFactory;

    public CatalogAuthorizationManager(DBAdaptorFactory dbFactory, AuthorizationDBAdaptorFactory authorizationDBAdaptorFactory)
            throws CatalogDBException {
        this.logger = LoggerFactory.getLogger(CatalogAuthorizationManager.class);
        this.dbAdaptorFactory = dbFactory;
        this.authorizationDBAdaptorFactory = authorizationDBAdaptorFactory;
    }

    @Override
    public void checkCanEditProject(String organizationId, long projectId, String userId) throws CatalogException {
        if (isOpencgaAdministrator(organizationId, userId) || isOrganizationOwnerOrAdmin(organizationId, userId)) {
            return;
        }
        throw new CatalogAuthorizationException("Permission denied: Only the organization owner or administrators can update the project.");
    }

    @Override
    public void checkCanViewProject(String organizationId, long projectId, String userId) throws CatalogException {
        if (isOpencgaAdministrator(organizationId, userId) || isOrganizationOwnerOrAdmin(organizationId, userId)) {
            return;
        }

        // Only members of any study belonging to the project can view the project
        final Query query = new Query()
                .append(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId)
                .append(StudyDBAdaptor.QueryParams.GROUP_USER_IDS.key(), userId);

        if (dbAdaptorFactory.getCatalogStudyDBAdaptor(organizationId).count(query).getNumMatches() > 0) {
            return;
        }

        throw CatalogAuthorizationException.deny(userId, "view", "Project", projectId, null);
    }

    @Override
    public void checkStudyPermission(String organizationId, long studyUid, JwtPayload payload, StudyPermissions.Permissions permission)
            throws CatalogException {
        String userId = payload.getUserId(organizationId);
        if (isOpencgaAdministrator(organizationId, userId)) {
            return;
        } else {
            if (dbAdaptorFactory.getCatalogStudyDBAdaptor(organizationId).hasStudyPermission(studyUid, userId, permission)) {
                return;
            }
        }
        throw CatalogAuthorizationException.deny(userId, permission.name(), "Study", studyUid, null);
    }

    @Override
    public void checkStudyPermission(String organizationId, long studyId, String userId, StudyPermissions.Permissions permission)
            throws CatalogException {
        if (isOpencgaAdministrator(organizationId, userId) || isOrganizationOwnerOrAdmin(organizationId, userId)) {
            return;
        } else {
            if (dbAdaptorFactory.getCatalogStudyDBAdaptor(organizationId).hasStudyPermission(studyId, userId, permission)) {
                return;
            }
        }
        throw CatalogAuthorizationException.deny(userId, permission.name(), "Study", studyId, null);
    }

    @Override
    public void checkCanEditStudy(String organizationId, long studyId, String userId) throws CatalogException {
        if (!isOpencgaAdministrator(organizationId, userId) && !isStudyAdministrator(organizationId, studyId, userId)) {
            throw CatalogAuthorizationException.notStudyAdmin("modify a study");
        }
    }

    @Override
    public void checkCanViewStudy(String organizationId, long studyId, String userId) throws CatalogException {
        if (isOpencgaAdministrator(organizationId, userId)) {
            return;
        }

        OpenCGAResult<Group> groupBelonging = getGroupBelonging(organizationId, studyId, userId);
        if (groupBelonging.getNumResults() == 0) {
            throw CatalogAuthorizationException.notStudyMember("see it");
        }
    }

    @Override
    public void checkCanUpdatePermissionRules(String organizationId, long studyId, String userId) throws CatalogException {
        if (!isOpencgaAdministrator(organizationId, userId) && !isStudyAdministrator(organizationId, studyId, userId)) {
            throw CatalogAuthorizationException.notStudyAdmin("update the permission rules");
        }
    }

    @Override
    public void checkCreateDeleteGroupPermissions(String organizationId, long studyId, String userId, String group)
            throws CatalogException {
        if (group.equals(MEMBERS_GROUP) || group.equals(ADMINS_GROUP)) {
            throw new CatalogAuthorizationException(group + " is a protected group that cannot be created or deleted.");
        }

        if (!isOpencgaAdministrator(organizationId, userId) && !isStudyAdministrator(organizationId, studyId, userId)) {
            throw CatalogAuthorizationException.notStudyAdmin("create or remove groups.");
        }
    }

    @Override
    public void checkSyncGroupPermissions(String organizationId, long studyUid, String userId, String group) throws CatalogException {
        checkCreateDeleteGroupPermissions(organizationId, studyUid, userId, group);
    }

    @Override
    public void checkUpdateGroupPermissions(String organizationId, long studyId, String userId, String group,
                                            ParamUtils.BasicUpdateAction action) throws CatalogException {
        if (MEMBERS_GROUP.equals(group)
                && (action != ParamUtils.BasicUpdateAction.ADD && action != ParamUtils.BasicUpdateAction.REMOVE)) {
            throw new CatalogAuthorizationException("Only ADD or REMOVE actions are accepted for " + MEMBERS_GROUP + " group.");
        }
        if (ADMINS_GROUP.equals(group) && !isOrganizationOwnerOrAdmin(organizationId, userId)) {
            throw CatalogAuthorizationException.notOrganizationOwnerOrAdmin("assign or remove users to the " + ADMINS_GROUP + " group.");
        }
        if (!isStudyAdministrator(organizationId, studyId, userId)) {
            throw CatalogAuthorizationException.notStudyAdmin("assign or remove users to groups.");
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
    public void checkCanAssignOrSeePermissions(String organizationId, long studyId, String userId) throws CatalogException {
        if (!isOpencgaAdministrator(organizationId, userId) && !isStudyAdministrator(organizationId, studyId, userId)) {
            throw CatalogAuthorizationException.notStudyAdmin("assign or see all permissions");
        }
    }

    @Override
    public void checkCanCreateUpdateDeleteVariableSets(String organizationId, long studyId, String userId) throws CatalogException {
        if (!isOpencgaAdministrator(organizationId, userId) && !isOrganizationOwnerOrAdmin(organizationId, userId)) {
            throw CatalogAuthorizationException.notOrganizationOwnerOrAdmin("create, update or delete variable sets.");
        }
    }

    @Override
    public boolean isOpencgaAdministrator(String organization, String userId) throws CatalogException {
        if (ParamConstants.ADMIN_ORGANIZATION.equals(organization) || userId.startsWith(ParamConstants.ADMIN_ORGANIZATION + ":")) {
            // Check user exists in ADMIN ORGANIZATION
            String user = userId.replace(ParamConstants.ADMIN_ORGANIZATION + ":", "");
            dbAdaptorFactory.getCatalogUserDBAdaptor(ParamConstants.ADMIN_ORGANIZATION).checkId(user);
            return true;
        }
        return false;
    }

    @Override
    public void checkIsOpencgaAdministrator(String organizationId, String userId, String action) throws CatalogException {
        if (!isOpencgaAdministrator(organizationId, userId)) {
            throw CatalogAuthorizationException.opencgaAdminOnlySupportedOperation(action);
        }
    }

    @Override
    public void checkIsOrganizationOwnerOrAdmin(String organizationId, String userId) throws CatalogException {
        if (!isOrganizationOwnerOrAdmin(organizationId, userId)) {
            throw CatalogAuthorizationException.notOrganizationOwnerOrAdmin();
        }
    }

    @Override
    public boolean isOrganizationOwnerOrAdmin(String organizationId, String userId) throws CatalogDBException {
        OrganizationDBAdaptor organizationDBAdaptor = dbAdaptorFactory.getCatalogOrganizationDBAdaptor(organizationId);
        Organization organization = organizationDBAdaptor.get(OrganizationManager.INCLUDE_ORGANIZATION_ADMINS).first();
        return organization.getOwner().equals(userId) || organization.getAdmins().contains(userId);
    }

    @Override
    public void checkIsStudyAdministrator(String organizationId, long studyId, String userId) throws CatalogException {
        if (!isStudyAdministrator(organizationId, studyId, userId)) {
            throw CatalogAuthorizationException.notStudyAdmin("perform this action");
        }
    }

    @Override
    public boolean isStudyAdministrator(String organizationId, long studyId, String user) throws CatalogException {
        OpenCGAResult<Group> groupBelonging = getGroupBelonging(organizationId, studyId, user);
        for (Group group : groupBelonging.getResults()) {
            if (group.getId().equals(ADMINS_GROUP)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkFilePermission(String organizationId, long studyId, long fileId, String userId, FilePermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), fileId)
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(organizationId, userId, query, dbAdaptorFactory.getCatalogFileDBAdaptor(organizationId))) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "File", fileId, null);
    }

    private boolean checkUserPermission(String organizationId, String userId, Query query, CoreDBAdaptor dbAdaptor)
            throws CatalogException {
        if (isOpencgaAdministrator(organizationId, userId) || isOrganizationOwnerOrAdmin(organizationId, userId)) {
            return true;
        } else {
            if (dbAdaptor.count(query, userId).getNumMatches() == 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkSamplePermission(String organizationId, long studyId, long sampleId, String userId, SamplePermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleId)
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(organizationId, userId, query, dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId))) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Sample", sampleId, null);
    }

    @Override
    public void checkIndividualPermission(String organizationId, long studyId, long individualId, String userId,
                                          IndividualPermissions permission) throws CatalogException {
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.UID.key(), individualId)
                .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(organizationId, userId, query, dbAdaptorFactory.getCatalogIndividualDBAdaptor(organizationId))) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Individual", individualId, null);
    }

    @Override
    public void checkJobPermission(String organizationId, long studyId, long jobId, String userId, JobPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.UID.key(), jobId)
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(organizationId, userId, query, dbAdaptorFactory.getCatalogJobDBAdaptor(organizationId))) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Job", jobId, null);
    }

    @Override
    public void checkCohortPermission(String organizationId, long studyId, long cohortId, String userId, CohortPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.UID.key(), cohortId)
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(organizationId, userId, query, dbAdaptorFactory.getCatalogCohortDBAdaptor(organizationId))) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Cohort", cohortId, null);

    }

    @Override
    public void checkPanelPermission(String organizationId, long studyId, long panelId, String userId, PanelPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(PanelDBAdaptor.QueryParams.UID.key(), panelId)
                .append(PanelDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(organizationId, userId, query, dbAdaptorFactory.getCatalogPanelDBAdaptor(organizationId))) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Panel", panelId, null);
    }

    @Override
    public void checkFamilyPermission(String organizationId, long studyId, long familyId, String userId, FamilyPermissions permission)
            throws CatalogException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.UID.key(), familyId)
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(organizationId, userId, query, dbAdaptorFactory.getCatalogFamilyDBAdaptor(organizationId))) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Family", familyId, null);

    }

    @Override
    public void checkClinicalAnalysisPermission(String organizationId, long studyId, long analysisId, String userId,
                                                ClinicalAnalysisPermissions permission) throws CatalogException {
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), analysisId)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(ParamConstants.ACL_PARAM, userId + ":" + permission.name());

        if (checkUserPermission(organizationId, userId, query, dbAdaptorFactory.getClinicalAnalysisDBAdaptor(organizationId))) {
            return;
        }
        throw CatalogAuthorizationException.deny(userId, permission.toString(), "ClinicalAnalysis", analysisId, null);
    }

    @Override
    public List<Acl> getEffectivePermissions(String organizationId, long studyUid, List<String> resourceIdList, List<String> permissions,
                                             Enums.Resource resource) throws CatalogException {
        HashSet<String> resourcePermissions = new HashSet<>(resource.getFullPermissionList());
        for (String permission : permissions) {
            if (!resourcePermissions.contains(permission)) {
                throw new CatalogParameterException("Permission '" + permission + "' does not correspond with any of the available"
                        + " permissions. This is the full list of possible permissions: " + StringUtils.join(resourcePermissions, ", "));
            }
        }

        List<Acl> acls = authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .effectivePermissions(studyUid, resourceIdList, resource);
        if (CollectionUtils.isNotEmpty(permissions)) {
            // Filter out other permissions
            for (Acl acl : acls) {
                List<Acl.Permission> permissionList = new ArrayList<>(permissions.size());
                for (Acl.Permission aclPermission : acl.getPermissions()) {
                    if (permissions.contains(aclPermission.getId())) {
                        permissionList.add(aclPermission);
                    }
                }
                acl.setPermissions(permissionList);
            }
        }
        return acls;
    }

    @Override
    public OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getAllStudyAcls(String organizationId, long studyId, String userId)
            throws CatalogException {
        checkCanAssignOrSeePermissions(organizationId, studyId, userId);
        return authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .get(studyId, null, null, Enums.Resource.STUDY, StudyPermissions.Permissions.class);
    }

    @Override
    public OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(String organizationId, long studyUid, List<String> members,
                                                                                 String userId) throws CatalogException {
        checkCanSeePermissions(organizationId, studyUid, members, userId);
        Map<String, List<String>> userGroups = extractUserGroups(organizationId, studyUid, members);
        return authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .get(studyUid, members, userGroups, Enums.Resource.STUDY, StudyPermissions.Permissions.class);
    }

    @Override
    public OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(String organizationId, List<Long> studyUids,
                                                                                 List<String> members) throws CatalogException {
        OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> result = OpenCGAResult.empty();
        for (Long studyUid : studyUids) {
            Map<String, List<String>> userGroups = extractUserGroups(organizationId, studyUid, members);
            result.append(authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                    .get(studyUid, members, userGroups, Enums.Resource.STUDY, StudyPermissions.Permissions.class));
        }
        return result;
    }

    @Override
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String organizationId, long studyUid, List<Long> resourceUids,
                                                                     List<String> members, Enums.Resource resource, Class<T> clazz,
                                                                     String userId) throws CatalogException {
        checkCanSeePermissions(organizationId, studyUid, members, userId);
        Map<String, List<String>> userGroups = extractUserGroups(organizationId, studyUid, members);
        return authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .get(resourceUids, members, userGroups, resource, clazz);
    }

    @Override
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String organizationId, long studyUid, List<Long> resourceUids,
                                                                     Enums.Resource resource, Class<T> clazz, String userId)
            throws CatalogException {
        checkCanAssignOrSeePermissions(organizationId, studyUid, userId);
        return authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .get(resourceUids, null, null, resource, clazz);
    }

    @Override
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(String organizationId, long studyUid, List<Long> resourceUids,
                                                                      Enums.Resource resource, Class<T> clazz) throws CatalogException {
        return authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .get(resourceUids, null, null, resource, clazz);
    }

    @Override
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(String organizationId, long studyUid, List<Long> resourceUids,
                                                                      List<String> members, Enums.Resource resource, Class<T> clazz)
            throws CatalogException {
        Map<String, List<String>> userGroups = extractUserGroups(organizationId, studyUid, members);
        return authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .get(resourceUids, members, userGroups, resource, clazz);
    }

    private void checkCanSeePermissions(String organizationId, long studyId, List<String> members, String userId) throws CatalogException {
        try {
            checkCanAssignOrSeePermissions(organizationId, studyId, userId);
        } catch (CatalogException e) {
            // It will be OK if the userId asking for the ACLs wants to see its own permissions
            for (String member : members) {
                checkAskingOwnPermissions(organizationId, studyId, member, userId);
            }
        }
    }

    private Map<String, List<String>> extractUserGroups(String organizationId, long studyId, List<String> members) throws CatalogException {
        Map<String, List<String>> userGroups = new HashMap<>();
        List<String> userList = members.stream().filter(m -> !m.startsWith("@")).collect(Collectors.toList());
        if (!userList.isEmpty()) {
            // If member is a user, we will also store the groups the user might belong to fetch those permissions as well
            OpenCGAResult<Group> groups = getGroupBelonging(organizationId, studyId, userList);

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
    public void resetPermissionsFromAllEntities(String organizationId, long studyId, List<String> members) throws CatalogException {
        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .resetMembersFromAllEntries(studyId, members);
    }

    private void checkAskingOwnPermissions(String organizationId, long studyId, String member, String userId) throws CatalogException {
        if (member.startsWith("@")) { //group
            // If the userId does not belong to the group...
            OpenCGAResult<Group> groupsBelonging = getGroupBelonging(organizationId, studyId, userId);
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
    public void setStudyAcls(String organizationId, List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException {
        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .setToMembers(studyIds, members, getImplicitPermissions(permissions, Enums.Resource.STUDY));
    }

    @Override
    public void addStudyAcls(String organizationId, List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException {
        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .addToMembers(studyIds, members, getImplicitPermissions(permissions, Enums.Resource.STUDY));
    }

    @Override
    public void removeStudyAcls(String organizationId, List<Long> studyIds, List<String> members, @Nullable List<String> permissions)
            throws CatalogException {
        removeAcls(organizationId, members, new CatalogAclParams(studyIds, permissions, Enums.Resource.STUDY));
    }

    @Override
    public void setAcls(String organizationId, long studyUid, List<String> members, List<CatalogAclParams> aclParams)
            throws CatalogException {
        setImplicitPermissions(aclParams);
        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .setToMembers(studyUid, members, aclParams);
    }

    @Override
    public void addAcls(String organizationId, long studyId, List<String> members, List<CatalogAclParams> aclParams)
            throws CatalogException {
        setImplicitPermissions(aclParams);
        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId).addToMembers(studyId, members, aclParams);
    }

    @Override
    public void removeAcls(String organizationId, List<String> members, List<CatalogAclParams> aclParams) throws CatalogException {
        setDependentPermissions(aclParams);
        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId).removeFromMembers(members, aclParams);
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
    public void replicateAcls(String organizationId, List<Long> uids, AclEntryList<?> aclEntryList, Enums.Resource resource)
            throws CatalogException {
        if (CollectionUtils.isEmpty(uids)) {
            throw new CatalogDBException("Missing identifiers to set acls");
        }
        if (CollectionUtils.isEmpty(aclEntryList.getAcl())) {
            return;
        }
        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId).setAcls(uids, aclEntryList, resource);
    }

    @Override
    public void applyPermissionRule(String organizationId, long studyId, PermissionRule permissionRule, Enums.Entity entry)
            throws CatalogException {
        // 1. We obtain which of those members are actually users to add them to the @members group automatically
        List<String> userList = permissionRule.getMembers().stream()
                .filter(member -> !member.startsWith("@"))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(userList)) {
            // We first add the member to the @members group in case they didn't belong already
            dbAdaptorFactory.getCatalogStudyDBAdaptor(organizationId).addUsersToGroup(studyId, MEMBERS_GROUP, userList);
        }

        // 2. We can apply the permission rules
        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .applyPermissionRules(studyId, permissionRule, entry);
    }

    @Override
    public void removePermissionRuleAndRemovePermissions(String organizationId, Study study, String permissionRuleId, Enums.Entity entry)
            throws CatalogException {
        ParamUtils.checkObj(permissionRuleId, "PermissionRule id");
        ParamUtils.checkObj(entry, "Entity");

        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .removePermissionRuleAndRemovePermissions(study, permissionRuleId, entry);
    }

    @Override
    public void removePermissionRuleAndRestorePermissions(String organizationId, Study study, String permissionRuleId, Enums.Entity entry)
            throws CatalogException {
        ParamUtils.checkObj(permissionRuleId, "PermissionRule id");
        ParamUtils.checkObj(entry, "Entity");

        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .removePermissionRuleAndRestorePermissions(study, permissionRuleId, entry);
    }

    @Override
    public void removePermissionRule(String organizationId, long studyId, String permissionRuleId, Enums.Entity entry)
            throws CatalogException {
        ParamUtils.checkObj(permissionRuleId, "PermissionRule id");
        ParamUtils.checkObj(entry, "Entity");

        authorizationDBAdaptorFactory.getAuthorizationDBAdaptor(organizationId)
                .removePermissionRule(studyId, permissionRuleId, entry);
    }

    /*
    ====================================
    Auxiliar methods
    ====================================
     */

    /**
     * Retrieves the groupId where the members belongs to.
     *
     * @param organizationId Organization id.
     * @param studyId        study id.
     * @param members        List of user ids.
     * @return the group id of the user. Null if the user does not take part of any group.
     * @throws CatalogException when there is any database error.
     */
    OpenCGAResult<Group> getGroupBelonging(String organizationId, long studyId, List<String> members) throws CatalogException {
        return dbAdaptorFactory.getCatalogStudyDBAdaptor(organizationId).getGroup(studyId, null, members);
    }

    OpenCGAResult<Group> getGroupBelonging(String organizationId, long studyId, String members) throws CatalogException {
        return getGroupBelonging(organizationId, studyId, Arrays.asList(members.split(",")));
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
