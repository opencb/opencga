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

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
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

    boolean isPublicRegistration();

    void checkCanViewProject(long projectId, String userId) throws CatalogException;

    void checkCanEditProject(long projectId, String userId) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyPermissions.Permissions permission) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyPermissions.Permissions permission, String message)
            throws CatalogException;

    void checkCanEditStudy(long studyId, String userId) throws CatalogException;

    void checkCanViewStudy(long studyId, String userId) throws CatalogException;

    void checkCanUpdatePermissionRules(long studyId, String userId) throws CatalogException;

    void checkCreateDeleteGroupPermissions(long studyId, String userId, String group) throws CatalogException;

    void checkSyncGroupPermissions(long studyId, String userId, String group) throws CatalogException;

    void checkUpdateGroupPermissions(long studyId, String userId, String group, ParamUtils.BasicUpdateAction action)
            throws CatalogException;

    void checkNotAssigningPermissionsToAdminsGroup(List<String> members) throws CatalogException;

    void checkCanAssignOrSeePermissions(long studyId, String userId) throws CatalogException;

    void checkCanCreateUpdateDeleteVariableSets(long studyId, String userId) throws CatalogException;

    Boolean isInstallationAdministrator(String user);

    void checkIsInstallationAdministrator(String user) throws CatalogException;

    void checkIsOwnerOrAdmin(long studyId, String userId) throws CatalogException;

    Boolean isOwnerOrAdmin(long studyId, String userId) throws CatalogException;

    void checkFilePermission(long studyId, long fileId, String userId, FilePermissions permission) throws CatalogException;

    void checkSamplePermission(long studyId, long sampleId, String userId, SamplePermissions permission)
            throws CatalogException;

    void checkIndividualPermission(long studyId, long individualId, String userId, IndividualPermissions permission)
            throws CatalogException;

    void checkJobPermission(long studyId, long jobId, String userId, JobPermissions permission) throws CatalogException;

    void checkCohortPermission(long studyId, long cohortId, String userId, CohortPermissions permission)
            throws CatalogException;

    void checkPanelPermission(long studyId, long panelId, String userId, PanelPermissions permission)
            throws CatalogException;

    void checkFamilyPermission(long studyId, long familyId, String userId, FamilyPermissions permission)
            throws CatalogException;

    void checkClinicalAnalysisPermission(long studyId, long analysisId, String userId,
                                         ClinicalAnalysisPermissions permission) throws CatalogException;

    //------------------------- Study ACL -----------------------------

    /**
     * Return all the ACLs defined in the study.
     *
     * @param userId  user id asking for the ACLs.
     * @param studyId study id.
     * @return a list of studyAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the study does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getAllStudyAcls(String userId, long studyId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId  user asking for the ACL.
     * @param studyId study id.
     * @param member  member whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    default OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(String userId, long studyId, String member)
            throws CatalogException {
        return getStudyAcl(userId, studyId, Collections.singletonList(member));
    }

    /**
     * Return the ACL defined for the member.
     *
     * @param userId  user asking for the ACL.
     * @param studyId study id.
     * @param members  members whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(String userId, long studyId, List<String> members)
            throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param studyUid study id.
     * @param members  members whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    default OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(long studyUid, List<String> members)
            throws CatalogException {
        return getStudyAcl(Collections.singletonList(studyUid), members);
    }

    /**
     * Return the ACL defined for the member.
     *
     * @param studyUids study uids.
     * @param members  members whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<StudyPermissions.Permissions>> getStudyAcl(List<Long> studyUids, List<String> members)
            throws CatalogException;

    //------------------------- End of study ACL ----------------------

    /**
     * Return the ACL defined for the member.
     *
     * @param userId  user asking for the ACL.
     * @param studyId study uid.
     * @param resourceUid Resource uid.
     * @param members  members whose permissions will be retrieved.
     * @param resource Resource where those permissions need to be checked.
     * @param clazz Permissions enum class.
     * @return the studyAcl for the member.
     * @param <T> Permissions enum type.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    default <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String userId, long studyId, long resourceUid, List<String> members,
                                                              Enums.Resource resource, Class<T> clazz) throws CatalogException {
        return getAcl(userId, studyId, Collections.singletonList(resourceUid), members, resource, clazz);
    }

    /**
     * Return the ACL defined for the member.
     *
     * @param userId  user asking for the ACL.
     * @param studyId study uid.
     * @param resourceUids List of resource uids.
     * @param members  members whose permissions will be retrieved.
     * @param resource Resource where those permissions need to be checked.
     * @param clazz Permissions enum class.
     * @return the studyAcl for the member.
     * @param <T> Permissions enum type.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String userId, long studyId, List<Long> resourceUids, List<String> members,
                                                              Enums.Resource resource, Class<T> clazz) throws CatalogException;

    default <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(long studyUid, long resourceUid, Enums.Resource resource,
                                                               Class<T> clazz) throws CatalogException {
        return getAcls(studyUid, Collections.singletonList(resourceUid), resource, clazz);
    }

    /**
     * Return the ACLs of the resources asked.
     *
     * @param userId  user asking for the ACL.
     * @param studyId study uid.
     * @param resourceUids List of resource uid.
     * @param resource Resource where those permissions need to be checked.
     * @param clazz Permissions enum class.
     * @return the studyAcl for the member.
     * @param <T> Permissions enum type.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcl(String userId, long studyId, List<Long> resourceUids, Enums.Resource resource,
                                                              Class<T> clazz) throws CatalogException;

    <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(long studyUid, List<Long> resourceUids, Enums.Resource resource,
                                                               Class<T> clazz) throws CatalogException;

    <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(long studyUid, List<Long> resourceUids, List<String> members,
                                                               Enums.Resource resource, Class<T> clazz) throws CatalogException;

    void setStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogException;

    void addStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogException;

    void removeStudyAcls(List<Long> studyIds, List<String> members, @Nullable List<String> permissions) throws CatalogException;

    default void setAcls(long studyUid, List<String> members, CatalogAclParams... aclParams) throws CatalogException {
        setAcls(studyUid, members, Arrays.asList(aclParams));
    }

    void setAcls(long studyUid, List<String> members, List<CatalogAclParams> aclParams) throws CatalogException;

    default void addAcls(long studyId, List<String> members, CatalogAclParams... aclParams) throws CatalogException {
        addAcls(studyId, members, Arrays.asList(aclParams));
    }

    void addAcls(long studyId, List<String> members, List<CatalogAclParams> aclParams) throws CatalogException;

    default void removeAcls(List<String> members, CatalogAclParams... aclParams) throws CatalogException {
        removeAcls(members, Arrays.asList(aclParams));
    }

    void removeAcls(List<String> members, List<CatalogAclParams> aclParams) throws CatalogException;

    void replicateAcls(List<Long> uids, AclEntryList<?> aclEntryList, Enums.Resource resource) throws CatalogException;

    void resetPermissionsFromAllEntities(long studyId, List<String> members) throws CatalogException;

    void applyPermissionRule(long studyId, PermissionRule permissionRule, Enums.Entity entry) throws CatalogException;

    void removePermissionRuleAndRemovePermissions(Study study, String permissionRuleId, Enums.Entity entry) throws CatalogException;

    void removePermissionRuleAndRestorePermissions(Study study, String permissionRuleId, Enums.Entity entry) throws CatalogException;

    void removePermissionRule(long studyId, String permissionRuleId, Enums.Entity entry) throws CatalogException;

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
