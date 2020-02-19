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

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

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
        return EnumSet.of(
                StudyAclEntry.StudyPermissions.WRITE_FILES, StudyAclEntry.StudyPermissions.VIEW_FILE_HEADERS,
                StudyAclEntry.StudyPermissions.VIEW_FILE_CONTENTS, StudyAclEntry.StudyPermissions.VIEW_FILES,
                StudyAclEntry.StudyPermissions.DOWNLOAD_FILES, StudyAclEntry.StudyPermissions.UPLOAD_FILES,
                StudyAclEntry.StudyPermissions.WRITE_JOBS, StudyAclEntry.StudyPermissions.VIEW_JOBS,
                StudyAclEntry.StudyPermissions.WRITE_SAMPLES, StudyAclEntry.StudyPermissions.VIEW_SAMPLES,
                StudyAclEntry.StudyPermissions.WRITE_SAMPLE_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.WRITE_INDIVIDUALS, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS,
                StudyAclEntry.StudyPermissions.WRITE_INDIVIDUAL_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.WRITE_COHORTS, StudyAclEntry.StudyPermissions.VIEW_COHORTS,
                StudyAclEntry.StudyPermissions.WRITE_COHORT_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.WRITE_PANELS, StudyAclEntry.StudyPermissions.VIEW_PANELS,
                StudyAclEntry.StudyPermissions.WRITE_FAMILIES, StudyAclEntry.StudyPermissions.VIEW_FAMILIES,
                StudyAclEntry.StudyPermissions.WRITE_FAMILY_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS, StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS);
    }

    static EnumSet<StudyAclEntry.StudyPermissions> getViewOnlyAcls() {
        return EnumSet.of(
                StudyAclEntry.StudyPermissions.VIEW_FILE_HEADERS, StudyAclEntry.StudyPermissions.VIEW_FILE_CONTENTS,
                StudyAclEntry.StudyPermissions.VIEW_FILES, StudyAclEntry.StudyPermissions.DOWNLOAD_FILES,
                StudyAclEntry.StudyPermissions.VIEW_JOBS, StudyAclEntry.StudyPermissions.VIEW_SAMPLES,
                StudyAclEntry.StudyPermissions.VIEW_SAMPLE_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_INDIVIDUALS,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_COHORTS,
                StudyAclEntry.StudyPermissions.VIEW_COHORT_ANNOTATIONS, StudyAclEntry.StudyPermissions.VIEW_PANELS,
                StudyAclEntry.StudyPermissions.VIEW_FAMILIES, StudyAclEntry.StudyPermissions.VIEW_FAMILY_ANNOTATIONS,
                StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS);
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

    void checkUpdateGroupPermissions(long studyId, String userId, String group, ParamUtils.UpdateAction action) throws CatalogException;

    void checkNotAssigningPermissionsToAdminsGroup(List<String> members) throws CatalogException;

    void checkCanAssignOrSeePermissions(long studyId, String userId) throws CatalogException;

    void checkCanCreateUpdateDeleteVariableSets(long studyId, String userId) throws CatalogException;

    Boolean checkIsAdmin(String user);

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
    OpenCGAResult<Map<String, List<String>>> getAllStudyAcls(String userId, long studyId) throws CatalogException;

    /**
     * Return the ACL defined for the member.
     *
     * @param userId  user asking for the ACL.
     * @param studyId study id.
     * @param member  member whose permissions will be retrieved.
     * @return the studyAcl for the member.
     * @throws CatalogException if the user does not have proper permissions to see the member permissions.
     */
    OpenCGAResult<Map<String, List<String>>> getStudyAcl(String userId, long studyId, String member) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getAllSampleAcls(long studyId, long sampleId, String userId) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getSampleAcl(long studyId, long sampleId, String userId, String member)
            throws CatalogException;

    //------------------------- End of sample ACL ----------------------


    //------------------------- File ACL -----------------------------

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
    OpenCGAResult<Map<String, List<String>>> getAllFileAcls(long studyId, long fileId, String userId, boolean checkPermission)
            throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getFileAcl(long studyId, long fileId, String userId, String member) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getAllIndividualAcls(long studyId, long individualId, String userId) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getIndividualAcl(long studyId, long individualId, String userId, String member)
            throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getAllCohortAcls(long studyId, long cohortId, String userId) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getCohortAcl(long studyId, long cohortId, String userId, String member)
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
    OpenCGAResult<Map<String, List<String>>> getAllPanelAcls(long studyId, long panelId, String userId) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getPanelAcl(long studyId, long panelId, String userId, String member) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getAllJobAcls(long studyId, long jobId, String userId) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getJobAcl(long studyId, long jobId, String userId, String member) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getAllFamilyAcls(long studyId, long familyId, String userId) throws CatalogException;

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
    OpenCGAResult<Map<String, List<String>>> getFamilyAcl(long studyId, long familyId, String userId, String member)
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
    OpenCGAResult<Map<String, List<String>>> getAllClinicalAnalysisAcls(long studyId, long clinicalAnalysisId, String userId)
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
    OpenCGAResult<Map<String, List<String>>> getClinicalAnalysisAcl(long studyId, long clinicalAnalysisId, String userId, String member)
            throws CatalogException;


    OpenCGAResult<Map<String, List<String>>> setStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException;

    OpenCGAResult<Map<String, List<String>>> addStudyAcls(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogException;

    OpenCGAResult<Map<String, List<String>>> removeStudyAcls(List<Long> studyIds, List<String> members, @Nullable List<String> permissions)
            throws CatalogException;

    default OpenCGAResult<Map<String, List<String>>> setAcls(long studyId, List<Long> ids, List<String> members, List<String> permissions,
                                                          Enums.Resource resource) throws CatalogException {
        return setAcls(studyId, ids, null, members, permissions, resource, null);
    }

    OpenCGAResult<Map<String, List<String>>> setAcls(long studyId, List<Long> ids1, List<Long> ids2, List<String> members,
                                                     List<String> permissions, Enums.Resource resource1, Enums.Resource resource2)
            throws CatalogException;

    default OpenCGAResult<Map<String, List<String>>> addAcls(long studyId, List<Long> ids, List<String> members, List<String> permissions,
                                                          Enums.Resource resource) throws CatalogException {
        return addAcls(studyId, ids, null, members, permissions, resource, null);
    }

    OpenCGAResult<Map<String, List<String>>> addAcls(long studyId, List<Long> ids1, List<Long> ids2, List<String> members,
                                                     List<String> permissions, Enums.Resource resource, Enums.Resource resource2)
            throws CatalogException;

    default OpenCGAResult<Map<String, List<String>>> removeAcls(List<Long> ids, List<String> members, @Nullable List<String> permissions,
                                                             Enums.Resource resource) throws CatalogException {
        return removeAcls(ids, null, members, permissions, resource, null);
    }

    OpenCGAResult<Map<String, List<String>>> removeAcls(List<Long> ids1, List<Long> ids2, List<String> members,
                                                        @Nullable List<String> permissions, Enums.Resource resource,
                                                        Enums.Resource resource2) throws CatalogException;

    OpenCGAResult<Map<String, List<String>>> replicateAcls(long studyId, List<Long> ids, Map<String, List<String>> aclEntries,
                                                           Enums.Resource resource)
            throws CatalogException;

    void resetPermissionsFromAllEntities(long studyId, List<String> members) throws CatalogException;

    void applyPermissionRule(long studyId, PermissionRule permissionRule, Enums.Entity entry) throws CatalogException;

    void removePermissionRuleAndRemovePermissions(Study study, String permissionRuleId, Enums.Entity entry) throws CatalogException;

    void removePermissionRuleAndRestorePermissions(Study study, String permissionRuleId, Enums.Entity entry) throws CatalogException;

    void removePermissionRule(long studyId, String permissionRuleId, Enums.Entity entry) throws CatalogException;
}
