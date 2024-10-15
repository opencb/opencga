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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
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
import org.opencb.opencga.core.models.panel.PanelPermissions;
import org.opencb.opencga.core.models.sample.SamplePermissions;
import org.opencb.opencga.core.models.study.PermissionRule;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.*;

/**
 * Created by pfurio on 12/05/16.
 */
public interface AuthorizationManager {

    String ROLE_ADMIN = "admin";
    String ROLE_ANALYST = "analyst";
    String ROLE_VIEW_ONLY = "view_only";
    String ROLE_LOCKED = "locked";

    static EnumSet<StudyPermissions.Permissions> getAdminAcls() {
        EnumSet<StudyPermissions.Permissions> permissions = EnumSet.allOf(StudyPermissions.Permissions.class);
        permissions.remove(NONE);
        return permissions;
    }

    static EnumSet<StudyPermissions.Permissions> getAnalystAcls() {
        return EnumSet.of(WRITE_FILES, VIEW_FILE_HEADER, VIEW_FILE_CONTENT, VIEW_FILES, DOWNLOAD_FILES, UPLOAD_FILES,
                EXECUTE_JOBS, WRITE_JOBS, VIEW_JOBS,
                WRITE_SAMPLES, VIEW_SAMPLES, WRITE_SAMPLE_ANNOTATIONS, VIEW_SAMPLE_ANNOTATIONS, VIEW_AGGREGATED_VARIANTS,
                VIEW_SAMPLE_VARIANTS,
                WRITE_INDIVIDUALS, VIEW_INDIVIDUALS, WRITE_INDIVIDUAL_ANNOTATIONS, VIEW_INDIVIDUAL_ANNOTATIONS,
                WRITE_COHORTS, VIEW_COHORTS, WRITE_COHORT_ANNOTATIONS, VIEW_COHORT_ANNOTATIONS,
                WRITE_PANELS, VIEW_PANELS,
                WRITE_FAMILIES, VIEW_FAMILIES, WRITE_FAMILY_ANNOTATIONS, VIEW_FAMILY_ANNOTATIONS,
                WRITE_CLINICAL_ANALYSIS, VIEW_CLINICAL_ANALYSIS);
    }

    static EnumSet<StudyPermissions.Permissions> getViewOnlyAcls() {
        return EnumSet.of(
                VIEW_FILE_HEADER, VIEW_FILE_CONTENT, VIEW_FILES, DOWNLOAD_FILES, VIEW_JOBS, VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS,
                VIEW_INDIVIDUALS, VIEW_AGGREGATED_VARIANTS, VIEW_SAMPLE_VARIANTS, VIEW_INDIVIDUAL_ANNOTATIONS, VIEW_COHORTS,
                VIEW_COHORT_ANNOTATIONS, VIEW_PANELS, VIEW_FAMILIES, VIEW_FAMILY_ANNOTATIONS, VIEW_CLINICAL_ANALYSIS, EXECUTE_JOBS);
    }

    static EnumSet<StudyPermissions.Permissions> getLockedAcls() {
        return EnumSet.noneOf(StudyPermissions.Permissions.class);
    }

    default void buildAclCheckQuery(String userId, String permission, Query query) {
        if (StringUtils.isNotEmpty(userId) && StringUtils.isNotEmpty(permission) && query != null) {
            query.put(ParamConstants.ACL_PARAM, userId + ":" + permission);
        }
    }

    void checkCanViewOrganization(String organizationId, String userId) throws CatalogException;

    void checkCanViewProject(String organizationId, long projectId, String userId) throws CatalogException;

    void checkCanEditProject(String organizationId, long projectId, String userId) throws CatalogException;

    void checkStudyPermission(String organizationId, long studyUid, JwtPayload payload, StudyPermissions.Permissions permission)
            throws CatalogException;

    void checkStudyPermission(String organizationId, long studyId, String userId, StudyPermissions.Permissions permission)
            throws CatalogException;

    void checkCanEditStudy(String organizationId, long studyId, String userId) throws CatalogException;

    void checkCanViewStudy(String organizationId, long studyId, String userId) throws CatalogException;

    void checkCanUpdatePermissionRules(String organizationId, long studyId, String userId) throws CatalogException;

    void checkCreateDeleteGroupPermissions(String organizationId, long studyId, String userId, String group) throws CatalogException;

    void checkSyncGroupPermissions(String organizationId, long studyId, String userId, String group) throws CatalogException;

    void checkUpdateGroupPermissions(String organizationId, long studyId, String userId, String group, ParamUtils.BasicUpdateAction action)
            throws CatalogException;

    void checkNotAssigningPermissionsToAdminsGroup(List<String> members) throws CatalogException;

    void checkCanAssignOrSeePermissions(String organizationId, long studyId, String userId) throws CatalogException;

    void checkCanCreateUpdateDeleteVariableSets(String organizationId, long studyId, String userId) throws CatalogException;

    default boolean isOpencgaAdministrator(JwtPayload payload) throws CatalogException {
        return isOpencgaAdministrator(payload.getOrganization(), payload.getUserId());
    }

    boolean isOpencgaAdministrator(String organization, String userId) throws CatalogAuthorizationException;

    default void checkIsOpencgaAdministrator(JwtPayload payload) throws CatalogException {
        checkIsOpencgaAdministrator(payload, null);
    }

    default void checkIsOpencgaAdministrator(JwtPayload payload, String action) throws CatalogException {
        checkIsOpencgaAdministrator(payload.getOrganization(), payload.getUserId(), action);
    }

    default void checkIsOpencgaAdministrator(String organization, String userId) throws CatalogException {
        checkIsOpencgaAdministrator(organization, userId, null);
    }

    void checkIsOpencgaAdministrator(String organization, String userId, String action) throws CatalogException;

    /**
     * Check if the given user is the organization owner.
     * If the user is not the owner, it will throw an exception.
     *
     * @param organizationId    Organization id
     * @param userId            User id
     * @throws CatalogException CatalogException
     */
    void checkIsAtLeastOrganizationOwner(String organizationId, String userId) throws CatalogException;

    /**
     * Check if the given user is the owner of the organization or if it is an admin.
     * If the user is not the owner or an admin, it will throw an exception.
     *
     * @param organizationId    Organization id
     * @param userId            User id
     * @throws CatalogException CatalogException
     */
    void checkIsAtLeastOrganizationOwnerOrAdmin(String organizationId, String userId) throws CatalogException;

    /**
     * Check if the given user is the organization owner or one of the Opencga administrators.
     *
     * @param organization Organization
     * @param userId User id
     * @return true if the user is the organization owner or one of the Opencga administrators.
     * @throws CatalogException CatalogDBException
     */
    boolean isAtLeastOrganizationOwner(String organization, String userId) throws CatalogException;

    /**
     * Check if the given user is the owner or one of the admins of the organization or one of the Opencga administrators.
     *
     * @param organization Organization
     * @param userId User id
     * @return true if the user is the owner or an admin of the organization or one of the Opencga administrators.
     * @throws CatalogException CatalogDBException
     */
    boolean isAtLeastOrganizationOwnerOrAdmin(String organization, String userId) throws CatalogException;

    /**
     * Check if the user is part of the {@link ParamConstants#ADMINS_GROUP} group of the study.
     * Keep in mind that all organization admins and the organization owner are also study admins.
     * It does not include the opencga admins.
     *
     * @param organizationId Organization id
     * @param studyId        Study id
     * @param userId         User id
     * @return true if the user is part of the admins group of the study
     * @throws CatalogException CatalogException
     */
    boolean isAtLeastStudyAdministrator(String organizationId, long studyId, String userId) throws CatalogException;

    /**
     * Check if the user is part of the {@link ParamConstants#ADMINS_GROUP} group of the study.
     * Keep in mind that all organization admins and the organization owner are also study admins.
     * It does not include the opencga admins.
     * If the check fails, it will throw an exception.
     *
     * @param organizationId Organization id
     * @param studyId       Study id
     * @param userId       User id
     * @throws CatalogException CatalogException
     */
    void checkIsAtLeastStudyAdministrator(String organizationId, long studyId, String userId) throws CatalogException;

    void checkFilePermission(String organizationId, long studyId, long fileId, String userId, FilePermissions permission)
            throws CatalogException;

    void checkSamplePermission(String organizationId, long studyId, long sampleId, String userId, SamplePermissions permission)
            throws CatalogException;

    void checkIndividualPermission(String organizationId, long studyId, long individualId, String userId, IndividualPermissions permission)
            throws CatalogException;

    void checkJobPermission(String organizationId, long studyId, long jobId, String userId, JobPermissions permission)
            throws CatalogException;

    void checkCohortPermission(String organizationId, long studyId, long cohortId, String userId, CohortPermissions permission)
            throws CatalogException;

    void checkPanelPermission(String organizationId, long studyId, long panelId, String userId, PanelPermissions permission)
            throws CatalogException;

    void checkFamilyPermission(String organizationId, long studyId, long familyId, String userId, FamilyPermissions permission)
            throws CatalogException;

    void checkClinicalAnalysisPermission(String organizationId, long studyId, long analysisId, String userId,
                                         ClinicalAnalysisPermissions permission) throws CatalogException;

    default List<Acl> getEffectivePermissions(String organizationId, long studyUid, String resourceId, Enums.Resource resource)
            throws CatalogException {
        return getEffectivePermissions(organizationId, studyUid, Collections.singletonList(resourceId), Collections.emptyList(), resource);
    }

    default List<Acl> getEffectivePermissions(String organizationId, long studyUid, List<String> resourceIdList, Enums.Resource resource)
            throws CatalogException {
        return getEffectivePermissions(organizationId, studyUid, resourceIdList, Collections.emptyList(), resource);
    }

    default List<Acl> getEffectivePermissions(String organizationId, long studyUid, String resourceId, String permission,
                                              Enums.Resource resource) throws CatalogException {
        return getEffectivePermissions(organizationId, studyUid, Collections.singletonList(resourceId),
                Collections.singletonList(permission), resource);
    }

    default List<Acl> getEffectivePermissions(String organizationId, long studyUid, List<String> resourceIdList, String permission,
                                              Enums.Resource resource) throws CatalogException {
        return getEffectivePermissions(organizationId, studyUid, resourceIdList, Collections.singletonList(permission), resource);
    }

    List<Acl> getEffectivePermissions(String organizationId, long studyUid, List<String> resourceIdList, List<String> permission,
                                      Enums.Resource resource) throws CatalogException;

    //------------------------- Study ACL -----------------------------

    /**
     * Return all the ACLs defined in the study.
     *
     * @param organizationId Organization id.
     * @param studyId        study id.
     * @param userId         user id asking for the ACLs.
     * @return a list of studyAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the study does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getAllStudyAcls(String organizationId, long studyId, String userId)
            throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param organizationId Organization id.
     * @param studyId        study id.
     * @param member         member whose permissions will be retrieved.
     * @param userId         user asking for the ACL.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    default OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(String organizationId, long studyId, String member,
                                                                                  String userId) throws CatalogException {
        return getStudyAcl(organizationId, studyId, Collections.singletonList(member), userId);
    }

    /**
     * Return the ACL defined for the member.
     *
     * @param organizationId Organization id.
     * @param studyId        study id.
     * @param members        members whose permissions will be retrieved.
     * @param userId         user asking for the ACL.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(String organizationId, long studyId, List<String> members,
                                                                          String userId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param organizationId Organization id.
     * @param studyUid       study id.
     * @param members        members whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    default OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(String organizationId, long studyUid,
                                                                                  List<String> members) throws CatalogException {
        return getStudyAcl(organizationId, Collections.singletonList(studyUid), members);
    }

    /**
     * Return the ACL defined for the member.
     *
     * @param organizationId Organization id.
     * @param studyUids      study uids.
     * @param members        members whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(String organizationId, List<Long> studyUids, List<String> members)
            throws CatalogException;

    //------------------------- End of study ACL ----------------------

    /**
     * Return the ACL defined for the member.
     *
     * @param <T>            Permissions enum type.
     * @param organizationId Organization id.
     * @param studyId        study uid.
     * @param resourceUid    Resource uid.
     * @param members        members whose permissions will be retrieved.
     * @param resource       Resource where those permissions need to be checked.
     * @param clazz          Permissions enum class.
     * @param userId         user asking for the ACL.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    default <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String organizationId, long studyId, long resourceUid,
                                                                      List<String> members, Enums.Resource resource, Class<T> clazz,
                                                                      String userId) throws CatalogException {
        return getAcl(organizationId, studyId, Collections.singletonList(resourceUid), members, resource, clazz, userId);
    }

    /**
     * Return the ACL defined for the member.
     *
     * @param <T>            Permissions enum type.
     * @param organizationId Organization id.
     * @param studyId        study uid.
     * @param resourceUids   List of resource uids.
     * @param members        members whose permissions will be retrieved.
     * @param resource       Resource where those permissions need to be checked.
     * @param clazz          Permissions enum class.
     * @param userId         user asking for the ACL.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String organizationId, long studyId, List<Long> resourceUids,
                                                              List<String> members, Enums.Resource resource, Class<T> clazz, String userId)
            throws CatalogException;

    default <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(String organizationId, long studyUid, long resourceUid,
                                                                       Enums.Resource resource, Class<T> clazz) throws CatalogException {
        return getAcls(organizationId, studyUid, Collections.singletonList(resourceUid), resource, clazz);
    }

    /**
     * Return the ACLs of the resources asked.
     *
     * @param <T>            Permissions enum type.
     * @param organizationId Organization id.
     * @param studyId        study uid.
     * @param resourceUids   List of resource uid.
     * @param resource       Resource where those permissions need to be checked.
     * @param clazz          Permissions enum class.
     * @param userId         user asking for the ACL.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String organizationId, long studyId, List<Long> resourceUids,
                                                              Enums.Resource resource, Class<T> clazz, String userId)
            throws CatalogException;

    <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(String organizationId, long studyUid, List<Long> resourceUids,
                                                               Enums.Resource resource, Class<T> clazz) throws CatalogException;

    <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(String organizationId, long studyUid, List<Long> resourceUids,
                                                               List<String> members, Enums.Resource resource, Class<T> clazz)
            throws CatalogException;

    void setStudyAcls(String organizationId, List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogException;

    void addStudyAcls(String organizationId, List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogException;

    void removeStudyAcls(String organizationId, List<Long> studyIds, List<String> members, @Nullable List<String> permissions)
            throws CatalogException;

    default void setAcls(String organizationId, long studyUid, List<String> members, CatalogAclParams... aclParams)
            throws CatalogException {
        setAcls(organizationId, studyUid, members, Arrays.asList(aclParams));
    }

    void setAcls(String organizationId, long studyUid, List<String> members, List<CatalogAclParams> aclParams) throws CatalogException;

    default void addAcls(String organizationId, long studyId, List<String> members, CatalogAclParams... aclParams) throws CatalogException {
        addAcls(organizationId, studyId, members, Arrays.asList(aclParams));
    }

    void addAcls(String organizationId, long studyId, List<String> members, List<CatalogAclParams> aclParams) throws CatalogException;

    default void removeAcls(String organizationId, List<String> members, CatalogAclParams... aclParams) throws CatalogException {
        removeAcls(organizationId, members, Arrays.asList(aclParams));
    }

    void removeAcls(String organizationId, List<String> members, List<CatalogAclParams> aclParams) throws CatalogException;

    void replicateAcls(String organizationId, List<Long> uids, AclEntryList<?> aclEntryList, Enums.Resource resource)
            throws CatalogException;

    void resetPermissionsFromAllEntities(String organizationId, long studyId, List<String> members) throws CatalogException;

    void applyPermissionRule(String organizationId, long studyId, PermissionRule permissionRule, Enums.Entity entry)
            throws CatalogException;

    void removePermissionRuleAndRemovePermissions(String organizationId, Study study, String permissionRuleId, Enums.Entity entry)
            throws CatalogException;

    void removePermissionRuleAndRestorePermissions(String organizationId, Study study, String permissionRuleId, Enums.Entity entry)
            throws CatalogException;

    void removePermissionRule(String organizationId, long studyId, String permissionRuleId, Enums.Entity entry) throws CatalogException;

    class CatalogAclParams {
        private List<Long> ids;
        private List<String> permissions;
        private Enums.Resource resource;

        public CatalogAclParams() {
        }

        public CatalogAclParams(List<Long> ids, List<String> permissions, Enums.Resource resource) {
            this.ids = ids;
            this.permissions = permissions;
            this.resource = resource;
        }

        public static void addToList(List<Long> ids, List<String> permissions, Enums.Resource resource,
                                     List<CatalogAclParams> aclParamsList) {
            if (ids != null && !ids.isEmpty()) {
                aclParamsList.add(new CatalogAclParams(ids, permissions, resource));
            }
        }

        public List<Long> getIds() {
            return ids;
        }

        public CatalogAclParams setIds(List<Long> ids) {
            this.ids = ids;
            return this;
        }

        public List<String> getPermissions() {
            return permissions;
        }

        public CatalogAclParams setPermissions(List<String> permissions) {
            this.permissions = permissions;
            return this;
        }

        public Enums.Resource getResource() {
            return resource;
        }

        public CatalogAclParams setResource(Enums.Resource resource) {
            this.resource = resource;
            return this;
        }
    }

}
