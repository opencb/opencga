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
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclEntry;
import org.opencb.opencga.core.models.cohort.CohortAclEntry;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.FamilyAclEntry;
import org.opencb.opencga.core.models.file.FileAclEntry;
import org.opencb.opencga.core.models.individual.IndividualAclEntry;
import org.opencb.opencga.core.models.job.JobAclEntry;
import org.opencb.opencga.core.models.panel.PanelAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.PermissionRule;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.opencb.opencga.core.models.study.StudyAclEntry.StudyPermissions.*;

/**
 * Created by pfurio on 12/05/16.
 */
public interface AuthorizationManager {

    String ROLE_ADMIN = "admin";
    String ROLE_ANALYST = "analyst";
    String ROLE_VIEW_ONLY = "view_only";
    String ROLE_LOCKED = "locked";

    static EnumSet<StudyAclEntry.StudyPermissions> getAdminAcls() {
        return EnumSet.allOf(StudyAclEntry.StudyPermissions.class);
    }

    static EnumSet<StudyAclEntry.StudyPermissions> getAnalystAcls() {
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

    static EnumSet<StudyAclEntry.StudyPermissions> getViewOnlyAcls() {
        return EnumSet.of(
                VIEW_FILE_HEADER, VIEW_FILE_CONTENT, VIEW_FILES, DOWNLOAD_FILES, VIEW_JOBS, VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS,
                VIEW_INDIVIDUALS, VIEW_AGGREGATED_VARIANTS, VIEW_SAMPLE_VARIANTS, VIEW_INDIVIDUAL_ANNOTATIONS, VIEW_COHORTS,
                VIEW_COHORT_ANNOTATIONS, VIEW_PANELS, VIEW_FAMILIES, VIEW_FAMILY_ANNOTATIONS, VIEW_CLINICAL_ANALYSIS, EXECUTE_JOBS);
    }

    static EnumSet<StudyAclEntry.StudyPermissions> getLockedAcls() {
        return EnumSet.noneOf(StudyAclEntry.StudyPermissions.class);
    }

    boolean isPublicRegistration();

    void checkCanViewProject(long projectId, String userId) throws CatalogException;

    void checkCanEditProject(long projectId, String userId) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyAclEntry.StudyPermissions permission) throws CatalogException;

    void checkStudyPermission(long studyId, String userId, StudyAclEntry.StudyPermissions permission, String message)
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

    void checkFilePermission(long studyId, long fileId, String userId, FileAclEntry.FilePermissions permission) throws CatalogException;

    void checkSamplePermission(long studyId, long sampleId, String userId, SampleAclEntry.SamplePermissions permission)
            throws CatalogException;

    void checkIndividualPermission(long studyId, long individualId, String userId, IndividualAclEntry.IndividualPermissions permission)
            throws CatalogException;

    void checkJobPermission(long studyId, long jobId, String userId, JobAclEntry.JobPermissions permission) throws CatalogException;

    void checkCohortPermission(long studyId, long cohortId, String userId, CohortAclEntry.CohortPermissions permission)
            throws CatalogException;

    void checkPanelPermission(long studyId, long panelId, String userId, PanelAclEntry.PanelPermissions permission)
            throws CatalogException;

    void checkFamilyPermission(long studyId, long familyId, String userId, FamilyAclEntry.FamilyPermissions permission)
            throws CatalogException;

    void checkClinicalAnalysisPermission(long studyId, long analysisId, String userId,
                                         ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions permission) throws CatalogException;

    //------------------------- Study ACL -----------------------------

    /**
     * Return all the ACLs defined in the study.
     *
     * @param userId  user id asking for the ACLs.
     * @param studyId study id.
     * @return a list of studyAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the study does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<StudyAclEntry.StudyPermissions>> getAllStudyAcls(String userId, long studyId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId  user asking for the ACL.
     * @param studyId study id.
     * @param member  member whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<StudyAclEntry.StudyPermissions>> getStudyAcl(String userId, long studyId, String member)
            throws CatalogException;

    //------------------------- End of study ACL ----------------------

    //------------------------- Sample ACL -----------------------------

    /**
     * Return all the ACLs defined for the sample.
     *
     *
     * @param studyId study id.
     * @param sampleId sample id.
     * @param userId   user id asking for the ACLs.
     * @return a list of sampleAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<SampleAclEntry.SamplePermissions>> getAllSampleAcls(long studyId, long sampleId, String userId)
            throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     *
     * @param studyId study id.
     * @param sampleId sample id.
     * @param userId   user asking for the ACL.
     * @param member   member whose permissions will be retrieved.
     * @return the SampleAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<SampleAclEntry.SamplePermissions>> getSampleAcl(long studyId, long sampleId, String userId, String member)
            throws CatalogException;

    //------------------------- End of sample ACL ----------------------


    //------------------------- File ACL -----------------------------

    /**
     * Return all the ACLs defined for the file.
     *
     *
     * @param studyId study id.
     * @param fileId file id.
     * @return a list of FileAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    default OpenCGAResult<AclEntryList<FileAclEntry.FilePermissions>> getAllFileAcls(long studyId, long fileId) throws CatalogException {
        return getAllFileAcls(studyId, fileId, "", false);
    }

    /**
     * Return all the ACLs defined for the file.
     *
     *
     * @param studyId study id.
     * @param fileId file id.
     * @param userId user id asking for the ACLs.
     * @param checkPermission Boolean indicating whether to check the SHARE permission and possibly fail or not. Added to be able to
     *                        propagate permissions to children files/folders when a user with WRITE permissions links or creates but it
     *                        is not able to see all the ACLs in the parent folder.
     * @return a list of FileAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<FileAclEntry.FilePermissions>> getAllFileAcls(long studyId, long fileId, String userId,
                                                                             boolean checkPermission) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     *
     * @param studyId study id.
     * @param fileId file id.
     * @param userId user asking for the ACL.
     * @param member member whose permissions will be retrieved.
     * @return the FileAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<FileAclEntry.FilePermissions>> getFileAcl(long studyId, long fileId, String userId, String member)
            throws CatalogException;

    //------------------------- End of file ACL ----------------------

    //------------------------- Individual ACL -----------------------------

    /**
     * Return all the ACLs defined for the individual.
     *
     *
     * @param studyId study id.
     * @param individualId individual id.
     * @param userId       user id asking for the ACLs.
     * @return a list of IndividualAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<IndividualAclEntry.IndividualPermissions>> getAllIndividualAcls(long studyId, long individualId,
                                                                                               String userId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     *
     * @param studyId study id.
     * @param individualId individual id.
     * @param userId       user asking for the ACL.
     * @param member       member whose permissions will be retrieved.
     * @return the IndividualAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<IndividualAclEntry.IndividualPermissions>> getIndividualAcl(long studyId, long individualId, String userId,
                                                                                           String member) throws CatalogException;

    //------------------------- End of individual ACL ----------------------

    //------------------------- Cohort ACL -----------------------------

    /**
     * Return all the ACLs defined for the cohort.
     *
     *
     * @param studyId study id.
     * @param cohortId cohort id.
     * @param userId   user id asking for the ACLs.
     * @return a list of CohortAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the cohort does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<CohortAclEntry.CohortPermissions>> getAllCohortAcls(long studyId, long cohortId, String userId)
            throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     *
     * @param studyId study id.
     * @param cohortId cohort id.
     * @param userId   user asking for the ACL.
     * @param member   member whose permissions will be retrieved.
     * @return the CohortAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<CohortAclEntry.CohortPermissions>> getCohortAcl(long studyId, long cohortId, String userId, String member)
            throws CatalogException;

    //------------------------- End of cohort ACL ----------------------

    //------------------------- Panel ACL -----------------------------

    /**
     * Return all the ACLs defined for the panel.
     *
     *
     * @param studyId study id.
     * @param panelId panel id.
     * @param userId   user id asking for the ACLs.
     * @return a list of DiseasePanelAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the panel does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<PanelAclEntry.PanelPermissions>> getAllPanelAcls(long studyId, long panelId, String userId)
            throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     *
     * @param studyId study id.
     * @param panelId panel id.
     * @param userId   user asking for the ACL.
     * @param member   member whose permissions will be retrieved.
     * @return the DiseasePanelAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<PanelAclEntry.PanelPermissions>> getPanelAcl(long studyId, long panelId, String userId, String member)
            throws CatalogException;

    //------------------------- End of panel ACL ----------------------

    //------------------------- Job ACL -----------------------------

    /**
     * Return all the ACLs defined for the job.
     *
     *
     * @param studyId study id.
     * @param jobId  job id.
     * @param userId user id asking for the ACLs.
     * @return a list of JobAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the sample does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<JobAclEntry.JobPermissions>> getAllJobAcls(long studyId, long jobId, String userId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     *
     * @param studyId study id.
     * @param jobId  job id.
     * @param userId user asking for the ACL.
     * @param member member whose permissions will be retrieved.
     * @return the JobAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<JobAclEntry.JobPermissions>> getJobAcl(long studyId, long jobId, String userId, String member)
            throws CatalogException;

    /**
     * Return all the ACLs defined for the family.
     *
     *
     * @param studyId study id.
     * @param familyId family id.
     * @param userId user id asking for the ACLs.
     * @return a list of FamilyAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the family does not have proper permissions.
     */
    OpenCGAResult<AclEntryList<FamilyAclEntry.FamilyPermissions>> getAllFamilyAcls(long studyId, long familyId, String userId)
            throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     *
     * @param studyId study id.
     * @param familyId  family id.
     * @param userId user asking for the ACL.
     * @param member member whose permissions will be retrieved.
     * @return the FamilyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<FamilyAclEntry.FamilyPermissions>> getFamilyAcl(long studyId, long familyId, String userId, String member)
            throws CatalogException;

    /**
     * Return all the ACLs defined for the clinical analysis.
     *
     *
     * @param studyId study id.
     * @param clinicalAnalysisId Clinical analysis id.
     * @param userId user id asking for the ACLs.
     * @return a list of ClinicalAnalysisAcls.
     * @throws CatalogException when the user asking to retrieve all the ACLs defined in the clinical analysis does not have proper
     * permissions.
     */
    OpenCGAResult<AclEntryList<ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions>> getAllClinicalAnalysisAcls(long studyId,
                                                                                                                 long clinicalAnalysisId,
                                                                                                                 String userId)
            throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     *
     * @param studyId study id.
     * @param clinicalAnalysisId  Clinical analysis id.
     * @param userId user asking for the ACL.
     * @param member member whose permissions will be retrieved.
     * @return the ClinicalAnalysisAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<AclEntryList<ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions>> getClinicalAnalysisAcl(long studyId,
                                                                                                             long clinicalAnalysisId,
                                                                                                             String userId, String member)
            throws CatalogException;


    <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> getAcls(List<Long> resourceUids, List<String> members, Enums.Resource resource,
                                                               Class<T> clazz) throws CatalogException;

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
