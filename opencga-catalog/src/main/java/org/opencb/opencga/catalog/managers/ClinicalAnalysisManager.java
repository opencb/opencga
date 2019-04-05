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

package org.opencb.opencga.catalog.managers;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.ClinicalAnalysisAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.DUE_DATE;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisManager extends ResourceManager<ClinicalAnalysis> {

    private UserManager userManager;
    private StudyManager studyManager;

    protected static Logger logger = LoggerFactory.getLogger(ClinicalAnalysisManager.class);

    public static final QueryOptions INCLUDE_CLINICAL_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key()));

    ClinicalAnalysisManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                                   DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                                   Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    QueryResult<ClinicalAnalysis> internalGet(long studyUid, String entry, QueryOptions options, String user) throws CatalogException {
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        if (UUIDUtils.isOpenCGAUUID(entry)) {
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), entry);
        }

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
//                ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(),
//                ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
//                ClinicalAnalysisDBAdaptor.QueryParams.RELEASE.key(), ClinicalAnalysisDBAdaptor.QueryParams.ID.key(),
//                ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key()));
        QueryResult<ClinicalAnalysis> analysisQueryResult = clinicalDBAdaptor.get(query, queryOptions, user);
        if (analysisQueryResult.getNumResults() == 0) {
            analysisQueryResult = clinicalDBAdaptor.get(query, queryOptions);
            if (analysisQueryResult.getNumResults() == 0) {
                throw new CatalogException("Clinical analysis " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the clinical analysis "
                        + entry);
            }
        } else if (analysisQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one clinical analysis found based on " + entry);
        } else {
            return analysisQueryResult;
        }
    }

    @Override
    QueryResult<ClinicalAnalysis> internalGet(long studyUid, List<String> entryList, QueryOptions options, String user, boolean silent)
            throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing clinical analysis entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();

        Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        ClinicalAnalysisDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            ClinicalAnalysisDBAdaptor.QueryParams param = ClinicalAnalysisDBAdaptor.QueryParams.ID;
            if (UUIDUtils.isOpenCGAUUID(entry)) {
                param = ClinicalAnalysisDBAdaptor.QueryParams.UUID;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        query.put(idQueryParam.key(), uniqueList);

        QueryResult<ClinicalAnalysis> analysisQueryResult = clinicalDBAdaptor.get(query, queryOptions, user);

        if (silent || analysisQueryResult.getNumResults() == uniqueList.size()) {
            return analysisQueryResult;
        }
        // Query without adding the user check
        QueryResult<ClinicalAnalysis> resultsNoCheck = clinicalDBAdaptor.get(query, queryOptions);

        if (resultsNoCheck.getNumResults() == analysisQueryResult.getNumResults()) {
            throw new CatalogException("Missing clinical analyses. Some of the clinical analyses could not be found.");
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the clinical "
                    + "analyses.");
        }
    }

    @Override
    public QueryResult<ClinicalAnalysis> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.get(query, options, userId);

        if (queryResult.getNumResults() == 0 && query.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.UID.key())) {
            List<Long> analysisList = query.getAsLongList(ClinicalAnalysisDBAdaptor.QueryParams.UID.key());
            for (Long analysisId : analysisList) {
                authorizationManager.checkClinicalAnalysisPermission(study.getUid(), analysisId, userId,
                        ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW);
            }
        }

        return queryResult;
    }

    @Override
    public DBIterator<ClinicalAnalysis> iterator(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return clinicalDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis, QueryOptions options,
                                                String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(clinicalAnalysis, "clinicalAnalysis");
        ParamUtils.checkAlias(clinicalAnalysis.getId(), "id");
        ParamUtils.checkObj(clinicalAnalysis.getType(), "type");
        ParamUtils.checkObj(clinicalAnalysis.getDueDate(), "dueDate");
        if (clinicalAnalysis.getAnalyst() != null && StringUtils.isNotEmpty(clinicalAnalysis.getAnalyst().getAssignee())) {
            // We obtain the users with access to the study
            Set<String> users = new HashSet<>(catalogManager.getStudyManager().getGroup(studyStr, "members", token).first()
                    .getUserIds());
            if (!users.contains(clinicalAnalysis.getAnalyst().getAssignee())) {
                throw new CatalogException("Cannot assign clinical analysis to " + clinicalAnalysis.getAnalyst().getAssignee()
                        + ". User not found or with no access to the study.");
            }
            clinicalAnalysis.getAnalyst().setAssignedBy(userId);
        }

        if (TimeUtils.toDate(clinicalAnalysis.getDueDate()) == null) {
            throw new CatalogException("Unrecognised due date. Accepted format is: yyyyMMddHHmmss");
        }

        if (clinicalAnalysis.getFamily() != null && clinicalAnalysis.getProband() != null) {
            if (StringUtils.isEmpty(clinicalAnalysis.getProband().getId())) {
                throw new CatalogException("Missing proband id");
            }
            // Validate the proband has also been added within the family
            if (clinicalAnalysis.getFamily().getMembers() == null) {
                throw new CatalogException("Missing members information in the family");
            }
            boolean found = false;
            for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                if (StringUtils.isNotEmpty(member.getId()) && clinicalAnalysis.getProband().getId().equals(member.getId())) {
                    found = true;
                }
            }
            if (!found) {
                throw new CatalogException("Missing proband in the family");
            }
        }

        clinicalAnalysis.setProband(getFullValidatedMember(clinicalAnalysis.getProband(), study, token));
        clinicalAnalysis.setFamily(getFullValidatedFamily(clinicalAnalysis.getFamily(), study, token));
        validateClinicalAnalysisFields(clinicalAnalysis, study, token);

        clinicalAnalysis.setCreationDate(TimeUtils.getTime());
        clinicalAnalysis.setDescription(ParamUtils.defaultString(clinicalAnalysis.getDescription(), ""));
        if (clinicalAnalysis.getStatus() != null && StringUtils.isNotEmpty(clinicalAnalysis.getStatus().getName())) {
            clinicalAnalysis.setStatus(new ClinicalAnalysis.ClinicalStatus(clinicalAnalysis.getStatus().getName()));
        } else {
            clinicalAnalysis.setStatus(new ClinicalAnalysis.ClinicalStatus());
        }
        clinicalAnalysis.setRelease(catalogManager.getStudyManager().getCurrentRelease(study, userId));
        clinicalAnalysis.setAttributes(ParamUtils.defaultObject(clinicalAnalysis.getAttributes(), Collections.emptyMap()));
        clinicalAnalysis.setInterpretations(ParamUtils.defaultObject(clinicalAnalysis.getInterpretations(), ArrayList::new));
        clinicalAnalysis.setPriority(ParamUtils.defaultObject(clinicalAnalysis.getPriority(), ClinicalAnalysis.Priority.MEDIUM));
        clinicalAnalysis.setFlags(ParamUtils.defaultObject(clinicalAnalysis.getFlags(), ArrayList::new));
        clinicalAnalysis.setConsent(ParamUtils.defaultObject(clinicalAnalysis.getConsent(), new ClinicalConsent()));

        validateRoleToProband(clinicalAnalysis);

        clinicalAnalysis.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.CLINICAL));
        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.insert(study.getUid(), clinicalAnalysis, options);

        return queryResult;
    }

    private void validateRoleToProband(ClinicalAnalysis clinicalAnalysis) {
        // Get as many automatic roles as possible
        Map<String, ClinicalAnalysis.FamiliarRelationship> roleToProband = new HashMap<>();
        if (clinicalAnalysis.getProband() != null && StringUtils.isNotEmpty(clinicalAnalysis.getProband().getId())) {
            roleToProband.put(clinicalAnalysis.getProband().getId(), ClinicalAnalysis.FamiliarRelationship.PROBAND);

            String motherId = null;
            String fatherId = null;
            if (clinicalAnalysis.getProband().getFather() != null
                    && StringUtils.isNotEmpty(clinicalAnalysis.getProband().getFather().getId())) {
                fatherId = clinicalAnalysis.getProband().getFather().getId();
                roleToProband.put(fatherId, ClinicalAnalysis.FamiliarRelationship.FATHER);
            }
            if (clinicalAnalysis.getProband().getMother() != null
                    && StringUtils.isNotEmpty(clinicalAnalysis.getProband().getMother().getId())) {
                motherId = clinicalAnalysis.getProband().getMother().getId();
                roleToProband.put(motherId, ClinicalAnalysis.FamiliarRelationship.MOTHER);
            }

            if (clinicalAnalysis.getFamily() != null && ListUtils.isNotEmpty(clinicalAnalysis.getFamily().getMembers())
                    && motherId != null && fatherId != null) {
                // We look for possible brothers or sisters of the proband
                for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                    if (!roleToProband.containsKey(member.getId())) {
                        if (member.getFather() != null && fatherId.equals(member.getFather().getId()) && member.getMother() != null
                                && motherId.equals(member.getMother().getId())) {
                            // They are siblings for sure
                            if (member.getSex() == null || IndividualProperty.Sex.UNKNOWN.equals(member.getSex())
                                    || IndividualProperty.Sex.UNDETERMINED.equals(member.getSex())) {
                                roleToProband.put(member.getId(), ClinicalAnalysis.FamiliarRelationship.FULL_SIBLING);
                            } else if (IndividualProperty.Sex.MALE.equals(member.getSex())) {
                                roleToProband.put(member.getId(), ClinicalAnalysis.FamiliarRelationship.FULL_SIBLING_M);
                            } else if (IndividualProperty.Sex.FEMALE.equals(member.getSex())) {
                                roleToProband.put(member.getId(), ClinicalAnalysis.FamiliarRelationship.FULL_SIBLING_F);
                            }
                        } else {
                            // We don't know the relation
                            roleToProband.put(member.getId(), ClinicalAnalysis.FamiliarRelationship.UNKNOWN);
                        }
                    }
                }
            }
        }

        if (MapUtils.isNotEmpty(clinicalAnalysis.getRoleToProband())) {
            // We will always keep the roles provided by the user and add any other that might be missing
            for (String memberId : roleToProband.keySet()) {
                if (!clinicalAnalysis.getRoleToProband().containsKey(memberId)) {
                    clinicalAnalysis.getRoleToProband().put(memberId, roleToProband.get(memberId));
                }
            }
        } else {
            // Set automatic roles
            clinicalAnalysis.setRoleToProband(roleToProband);
        }
    }

    void validateClinicalAnalysisFields(ClinicalAnalysis clinicalAnalysis, Study study, String sessionId) throws CatalogException {
        // Validate the proband exists if the family is provided
        if (clinicalAnalysis.getFamily() != null && ListUtils.isNotEmpty(clinicalAnalysis.getFamily().getMembers())) {
            // Find the proband
            Individual proband = null;
            for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                if (member.getId().equals(clinicalAnalysis.getProband().getId())) {
                    proband = member;
                }
            }

            if (proband == null) {
                throw new CatalogException("Missing proband in array of members of family");
            }

            Set<Long> familyProbandSamples = proband.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet());
            List<Long> probandSample = clinicalAnalysis.getProband().getSamples().stream().map(Sample::getUid).collect(Collectors.toList());

            if (probandSample.size() != familyProbandSamples.size() || !familyProbandSamples.containsAll(probandSample)) {
                throw new CatalogException("Samples in proband from family and proband in clinical analysis differ");
            }
        }

        // Validate the files
        if (clinicalAnalysis.getFiles() != null && !clinicalAnalysis.getFiles().isEmpty()) {
            // We extract all the samples
            Map<String, Long> sampleMap = new HashMap<>();
            if (clinicalAnalysis.getFamily() != null && clinicalAnalysis.getFamily().getMembers() != null) {
                for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                    if (member.getSamples() != null) {
                        for (Sample sample : member.getSamples()) {
                            sampleMap.put(sample.getId(), sample.getUid());
                        }
                    }
                }
            } else if (clinicalAnalysis.getProband() != null && clinicalAnalysis.getProband().getSamples() != null) {
                for (Sample sample : clinicalAnalysis.getProband().getSamples()) {
                    sampleMap.put(sample.getId(), sample.getUid());
                }
            }

            for (String sampleKey : clinicalAnalysis.getFiles().keySet()) {
                if (!sampleMap.containsKey(sampleKey)) {
                    throw new CatalogException("Missing association from individual to sample " + sampleKey);
                }
            }

            // Validate that files are related to the associated samples and get full file information
            for (Map.Entry<String, List<File>> entry : clinicalAnalysis.getFiles().entrySet()) {
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.ID.key(), entry.getValue().stream().map(File::getId).collect(Collectors.toList()))
                        .append(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleMap.get(entry.getKey()));

                QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(study.getFqn(), query, QueryOptions.empty(),
                        sessionId);
                if (fileQueryResult.getNumResults() < entry.getValue().size()) {
                    throw new CatalogException("Some or all of the files associated to sample " + entry.getKey() + " could not be found"
                            + " or are not actually associated to the sample");
                }

                // Replace the files
                clinicalAnalysis.getFiles().put(entry.getKey(), fileQueryResult.getResult());
            }
        }
    }

    private Family getFullValidatedFamily(Family family, Study study, String sessionId) throws CatalogException {
        if (family == null) {
            return null;
        }

        if (StringUtils.isEmpty(family.getId())) {
            throw new CatalogException("Missing family id");
        }

        // List of members relevant for the clinical analysis
        List<Individual> selectedMembers = family.getMembers();

        QueryResult<Family> familyQueryResult = catalogManager.getFamilyManager().get(study.getFqn(), family.getId(), new QueryOptions(),
                sessionId);
        if (familyQueryResult.getNumResults() == 0) {
            throw new CatalogException("Family " + family.getId() + " not found");
        }
        Family finalFamily = familyQueryResult.first();

        if (ListUtils.isNotEmpty(selectedMembers)) {
            if (ListUtils.isEmpty(finalFamily.getMembers())) {
                throw new CatalogException("Family " + family.getId() + " does not have any members associated");
            }

            Map<String, Individual> memberMap = new HashMap<>();
            for (Individual member : finalFamily.getMembers()) {
                memberMap.put(member.getId(), member);
            }

            List<Individual> finalMembers = new ArrayList<>(selectedMembers.size());
            for (Individual selectedMember : selectedMembers) {
                Individual fullMember = memberMap.get(selectedMember.getId());
                if (fullMember == null) {
                    throw new CatalogException("Member " + selectedMember.getId() + " does not belong to family " + family.getId());
                }
                fullMember.setSamples(selectedMember.getSamples());
                finalMembers.add(getFullValidatedMember(fullMember, study, sessionId));
            }

            finalFamily.setMembers(finalMembers);
        } else {
            if (ListUtils.isNotEmpty(finalFamily.getMembers())) {
                Query query = new Query()
                        .append(IndividualDBAdaptor.QueryParams.UID.key(), finalFamily.getMembers().stream()
                                .map(Individual::getUid).collect(Collectors.toList()))
                        .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                QueryResult<Individual> individuals =
                        individualDBAdaptor.get(query, QueryOptions.empty(), catalogManager.getUserManager().getUserId(sessionId));
                finalFamily.setMembers(individuals.getResult());
            }
        }

        return finalFamily;
    }

    private Individual getFullValidatedMember(Individual member, Study study, String sessionId) throws CatalogException {
        if (member == null) {
            return null;
        }

        if (StringUtils.isEmpty(member.getId())) {
            throw new CatalogException("Missing member id");
        }

        Individual finalMember;

        // List of samples relevant for the clinical analysis
        List<Sample> samples = member.getSamples();

        if (member.getUid() <= 0) {
            QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(study.getFqn(), member.getId(),
                    new QueryOptions(), sessionId);
            if (individualQueryResult.getNumResults() == 0) {
                throw new CatalogException("Member " + member.getId() + " not found");
            }

            finalMember = individualQueryResult.first();
        } else {
            finalMember = member;
            if (ListUtils.isNotEmpty(samples) && StringUtils.isEmpty(samples.get(0).getUuid())) {
                // We don't have the full sample information...
                QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(study.getFqn(),
                        finalMember.getId(), new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key()),
                        sessionId);
                if (individualQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Member " + finalMember.getId() + " not found");
                }

                finalMember.setSamples(individualQueryResult.first().getSamples());
            }
        }

        if (ListUtils.isNotEmpty(finalMember.getSamples())) {
            Map<String, Sample> sampleMap = new HashMap<>();
            for (Sample sample : finalMember.getSamples()) {
                sampleMap.put(sample.getId(), sample);
            }

            List<Sample> finalSampleList = new ArrayList<>(samples.size());

            // We keep only the original list of samples passed
            for (Sample sample : samples) {
                finalSampleList.add(sampleMap.get(sample.getId()));
            }

            finalMember.setSamples(finalSampleList);
        }

        return finalMember;
    }

    @Override
    public QueryResult<ClinicalAnalysis> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options,
                                                String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ClinicalAnalysis clinicalAnalysis = internalGet(study.getUid(), entryStr, QueryOptions.empty(), userId).first();

        authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(), userId,
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            ClinicalAnalysisDBAdaptor.QueryParams queryParam = ClinicalAnalysisDBAdaptor.QueryParams.getParam(param.getKey());
            switch (queryParam) {
                case ID:
                    ParamUtils.checkAlias(parameters.getString(queryParam.key()), "id");
                    break;
//                case INTERPRETATIONS:
//                    // Get the interpretation uid
//                    List<LinkedHashMap<String, Object>> interpretationList = (List<LinkedHashMap<String, Object>>) param.getValue();
//                    for (LinkedHashMap<String, Object> interpretationMap : interpretationList) {
//                        LinkedHashMap<String, Object> fileMap = (LinkedHashMap<String, Object>) interpretationMap.get("file");
//                        MyResource<File> fileResource = catalogManager.getFileManager().getUid(String.valueOf(fileMap.get("path")),
//                                studyStr, sessionId);
//                        fileMap.put(FileDBAdaptor.QueryParams.UID.key(), fileResource.getResource().getUid());
//                    }
//                    break;
                case DUE_DATE:
                    if (TimeUtils.toDate(parameters.getString(DUE_DATE.key())) == null) {
                        throw new CatalogException("Unrecognised due date. Accepted format is: yyyyMMddHHmmss");
                    }
                    break;
                case ANALYST:
                    LinkedHashMap<String, Object> assigned = (LinkedHashMap<String, Object>) param.getValue();
                    // We obtain the users with access to the study
                    Set<String> users = new HashSet<>(catalogManager.getStudyManager().getGroup(studyStr, "members", token).first()
                            .getUserIds());
                    if (StringUtils.isNotEmpty(String.valueOf(assigned.get("assignee")))
                            && !users.contains(String.valueOf(assigned.get("assignee")))) {
                        throw new CatalogException("Cannot assign clinical analysis to " + assigned.get("assignee")
                                + ". User not found or with no access to the study.");
                    }
                    assigned.put("assignedBy", userId);
                    break;
                case CONSENT:
                    ClinicalConsent consent = parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.CONSENT.key(), ClinicalConsent.class);
                    if (consent == null) {
                        throw new CatalogException("Unknown 'consent' format");
                    }
                    break;
                case FILES:
                case STATUS:
                case DISORDER:
                case FAMILY:
                case PROBAND:
                case COMMENTS:
                case FLAGS:
                case ROLE_TO_PROBAND:
                    break;
                default:
                    throw new CatalogException("Cannot update " + queryParam);
            }
        }

        // Compare to the current clinical analysis to validate updates
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key())) {
            Family family = parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), Family.class);
            family = getFullValidatedFamily(family, study, token);
            clinicalAnalysis.setFamily(family);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), family);
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key())) {
            Individual proband = parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), Individual.class);
            proband = getFullValidatedMember(proband, study, token);
            clinicalAnalysis.setProband(proband);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), proband);
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key())) {
            Map<String, List<File>> files = (Map<String, List<File>>) parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key());
            clinicalAnalysis.setFiles(files);
        }

        validateClinicalAnalysisFields(clinicalAnalysis, study, token);
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key())) {
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), clinicalAnalysis.getFiles());
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.ROLE_TO_PROBAND.key())
                && (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key())
                || parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key()))) {
            // We need to validate the role to proband
            Map<String, String> map = parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.ROLE_TO_PROBAND.key(), Map.class);
            for (String memberId : map.keySet()) {
                clinicalAnalysis.getRoleToProband().put(memberId, ClinicalAnalysis.FamiliarRelationship.valueOf(map.get(memberId)));
            }
            validateRoleToProband(clinicalAnalysis);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.ROLE_TO_PROBAND.key(), clinicalAnalysis.getRoleToProband());
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key())) {
            Map<String, Object> status = (Map<String, Object>) parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key());
            if (!(status instanceof Map) || StringUtils.isEmpty(String.valueOf(status.get("name")))
                    || !ClinicalAnalysis.ClinicalStatus.isValid(String.valueOf(status.get("name")))) {
                throw new CatalogException("Missing or invalid status");
            }
        }

        return clinicalDBAdaptor.update(clinicalAnalysis.getUid(), parameters, QueryOptions.empty());
    }

    public QueryResult<ClinicalAnalysis> search(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(study, query, userId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.get(query, options, userId);
//            authorizationManager.filterClinicalAnalysis(userId, studyId, queryResultAux.getResult());

        return queryResult;
    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        if (query.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key())) {
            Family family = catalogManager.getFamilyManager().internalGet(study.getUid(),
                    query.getString(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key()), FamilyManager.INCLUDE_FAMILY_IDS, userId).first();
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_UID.key(), family.getUid());
            query.remove(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key());
        }
        if (query.containsKey("sample")) {
            Sample sample = catalogManager.getSampleManager().internalGet(study.getUid(), query.getString("sample"),
                    SampleManager.INCLUDE_SAMPLE_IDS, userId).first();
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE_UID.key(), sample.getUid());
            query.remove("sample");
        }
        if (query.containsKey("analystAssignee")) {
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.ANALYST_ASSIGNEE.key(), query.get("analystAssignee"));
            query.remove("analystAssignee");
        }
        if (query.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key())) {
            QueryResult<Individual> probandQueryResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    query.getString(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key()), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_UID.key(), probandQueryResult.first().getUid());
            query.remove(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key());
        }
    }

    public QueryResult<ClinicalAnalysis> count(String studyStr, Query query, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(study, query, userId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Long> queryResultAux = clinicalDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_CLINICAL_ANALYSIS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        return null;
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        return null;
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(study, query, userId);

        // Add study id to the query
        query.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult queryResult = clinicalDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<ClinicalAnalysisAclEntry>> getAcls(String studyStr, List<String> clinicalList, String member, boolean silent,
                                                     String sessionId) throws CatalogException {
        List<QueryResult<ClinicalAnalysisAclEntry>> clinicalAclList = new ArrayList<>(clinicalList.size());
        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
                ClinicalAnalysisDBAdaptor.QueryParams.ID.key()));
        QueryResult<ClinicalAnalysis> queryResult = internalGet(study.getUid(), clinicalList, queryOptions, user, silent);

        for (ClinicalAnalysis clinicalAnalysis : queryResult.getResult()) {
            try {
                QueryResult<ClinicalAnalysisAclEntry> allClinicalAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allClinicalAcls = authorizationManager.getClinicalAnalysisAcl(study.getUid(), clinicalAnalysis.getUid(), user, member);
                } else {
                    allClinicalAcls = authorizationManager.getAllClinicalAnalysisAcls(study.getUid(), clinicalAnalysis.getUid(), user);
                }
                allClinicalAcls.setId(clinicalAnalysis.getId());
                clinicalAclList.add(allClinicalAcls);
            } catch (CatalogException e) {
                if (!silent) {
                    throw e;
                }
            }
        }
        return clinicalAclList;
    }

    public List<QueryResult<ClinicalAnalysisAclEntry>> updateAcl(String studyStr, List<String> clinicalList, String memberIds,
                                                       AclParams clinicalAclParams, String sessionId) throws CatalogException {
        if (clinicalList == null || clinicalList.isEmpty()) {
            throw new CatalogException("Update ACL: Missing 'clinicalAnalysis' parameter");
        }

        if (clinicalAclParams.getAction() == null) {
            throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
        }

        List<String> permissions = Collections.emptyList();
        if (StringUtils.isNotEmpty(clinicalAclParams.getPermissions())) {
            permissions = Arrays.asList(clinicalAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
            checkPermissions(permissions, ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions::valueOf);
        }

        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);
        QueryResult<ClinicalAnalysis> queryResult = internalGet(study.getUid(), clinicalList, INCLUDE_CLINICAL_IDS, user, false);

        authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), user);

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(study.getUid(), members);

        switch (clinicalAclParams.getAction()) {
            case SET:
                // Todo: Remove this in 1.4
                List<String> allClinicalPermissions = EnumSet.allOf(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.class)
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList());
                return authorizationManager.setAcls(study.getUid(), queryResult.getResult().stream()
                                .map(ClinicalAnalysis::getUid)
                                .collect(Collectors.toList()), members, permissions,
                        allClinicalPermissions, Entity.CLINICAL_ANALYSIS);
            case ADD:
                return authorizationManager.addAcls(study.getUid(), queryResult.getResult().stream()
                        .map(ClinicalAnalysis::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.CLINICAL_ANALYSIS);
            case REMOVE:
                return authorizationManager.removeAcls(queryResult.getResult().stream()
                                .map(ClinicalAnalysis::getUid).collect(Collectors.toList()),
                        members, permissions, Entity.CLINICAL_ANALYSIS);
            case RESET:
                return authorizationManager.removeAcls(queryResult.getResult().stream()
                                .map(ClinicalAnalysis::getUid).collect(Collectors.toList()),
                        members, null, Entity.CLINICAL_ANALYSIS);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

}
