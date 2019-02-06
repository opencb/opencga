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

import org.apache.commons.lang3.StringUtils;
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

    protected static Logger logger = LoggerFactory.getLogger(ClinicalAnalysisManager.class);

    ClinicalAnalysisManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                                   DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                                   Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
    }

    @Override
    ClinicalAnalysis smartResolutor(long studyUid, String entry, String user) throws CatalogException {
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        if (UUIDUtils.isOpenCGAUUID(entry)) {
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), entry);
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(),
                ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
                ClinicalAnalysisDBAdaptor.QueryParams.RELEASE.key(), ClinicalAnalysisDBAdaptor.QueryParams.ID.key(),
                ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key()));
        QueryResult<ClinicalAnalysis> analysisQueryResult = clinicalDBAdaptor.get(query, options, user);
        if (analysisQueryResult.getNumResults() == 0) {
            analysisQueryResult = clinicalDBAdaptor.get(query, options);
            if (analysisQueryResult.getNumResults() == 0) {
                throw new CatalogException("Clinical analysis " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the clinical analysis "
                        + entry);
            }
        } else if (analysisQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one clinical analysis found based on " + entry);
        } else {
            return analysisQueryResult.first();
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
                                                String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(clinicalAnalysis, "clinicalAnalysis");
        ParamUtils.checkAlias(clinicalAnalysis.getId(), "id");
        ParamUtils.checkObj(clinicalAnalysis.getType(), "type");
        ParamUtils.checkObj(clinicalAnalysis.getDueDate(), "dueDate");
        if (clinicalAnalysis.getAnalyst() != null && StringUtils.isNotEmpty(clinicalAnalysis.getAnalyst().getAssignee())) {
            // We obtain the users with access to the study
            Set<String> users = new HashSet<>(catalogManager.getStudyManager().getGroup(studyStr, "members", sessionId).first()
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

        clinicalAnalysis.setProband(getFullValidatedMember(clinicalAnalysis.getProband(), study, sessionId));
        clinicalAnalysis.setFamily(getFullValidatedFamily(clinicalAnalysis.getFamily(), study, sessionId));
        validateClinicalAnalysisFields(clinicalAnalysis, study, sessionId);

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

        clinicalAnalysis.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.CLINICAL));
        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.insert(study.getUid(), clinicalAnalysis, options);

        return queryResult;
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

    private void validateMembers(ClinicalAnalysis clinicalAnalysis, Study study, String sessionId) throws CatalogException {
        Individual proband = clinicalAnalysis.getProband();

        if (proband == null) {
            throw new CatalogException("Missing subject in clinical analysis");
        }

        MyResource<Individual> resource = catalogManager.getIndividualManager().getUid(proband.getId(), study.getFqn(), sessionId);
        proband.setUid(resource.getResource().getUid());

//        List<String> sampleIds = proband.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
//        MyResources<Sample> sampleResources = catalogManager.getSampleManager().getUids(sampleIds, study.getFqn(), sessionId);
//        if (sampleResources.getResourceList().size() < proband.getSamples().size()) {
//            throw new CatalogException("Missing some samples. Found " + sampleResources.getResourceList().size() + " out of "
//                    + proband.getSamples().size());
//        }
//        // We associate the samples to the subject
//        proband.setSamples(sampleResources.getResourceList());
//
//        // Check those samples are actually samples from the proband
//        Query query = new Query()
//                .append(SampleDBAdaptor.QueryParams.UID.key(), proband.getSamples().stream()
//                        .map(Sample::getUid)
//                        .collect(Collectors.toList()))
//                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_UID.key(), proband.getUid());
//        QueryResult<Sample> countSamples = catalogManager.getSampleManager().count(study.getFqn(), query, sessionId);
//        if (countSamples.getNumTotalResults() < proband.getSamples().size()) {
//            throw new CatalogException("Not all the samples belong to the proband. Only " + countSamples.getNumTotalResults()
//                    + " out of the " + proband.getSamples().size() + " belong to the individual.");
//        }
    }

    private void validateFamilyAndProband(ClinicalAnalysis clinicalAnalysis, Study study, String sessionId) throws CatalogException {
        if (clinicalAnalysis.getFamily() != null && StringUtils.isNotEmpty(clinicalAnalysis.getFamily().getId())) {
            MyResource<Family> familyResource = catalogManager.getFamilyManager().getUid(clinicalAnalysis.getFamily().getId(),
                    study.getFqn(), sessionId);
            clinicalAnalysis.setFamily(familyResource.getResource());

            // Check the proband is an actual member of the family
            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.UID.key(), familyResource.getResource().getUid())
                    .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), clinicalAnalysis.getProband().getUid());
            QueryResult<Family> count = catalogManager.getFamilyManager().count(study.getFqn(), query, sessionId);
            if (count.getNumTotalResults() == 0) {
                throw new CatalogException("The member " + clinicalAnalysis.getProband().getId() + " does not belong to the family "
                        + clinicalAnalysis.getFamily().getId());
            }
        }
    }

    @Override
    public QueryResult<ClinicalAnalysis> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options,
                                                String sessionId) throws CatalogException {
        MyResource<ClinicalAnalysis> resource = getUid(entryStr, studyStr, sessionId);
        authorizationManager.checkClinicalAnalysisPermission(resource.getStudy().getUid(), resource.getResource().getUid(),
                resource.getUser(), ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            ClinicalAnalysisDBAdaptor.QueryParams queryParam = ClinicalAnalysisDBAdaptor.QueryParams.getParam(param.getKey());
            switch (queryParam) {
                case ID:
                    ParamUtils.checkAlias(parameters.getString(queryParam.key()), "id");
                    break;
                case INTERPRETATIONS:
                    // Get the file uid
                    List<LinkedHashMap<String, Object>> interpretationList = (List<LinkedHashMap<String, Object>>) param.getValue();
                    for (LinkedHashMap<String, Object> interpretationMap : interpretationList) {
                        LinkedHashMap<String, Object> fileMap = (LinkedHashMap<String, Object>) interpretationMap.get("file");
                        MyResource<File> fileResource = catalogManager.getFileManager().getUid(String.valueOf(fileMap.get("path")),
                                studyStr, sessionId);
                        fileMap.put(FileDBAdaptor.QueryParams.UID.key(), fileResource.getResource().getUid());
                    }
                    break;
                case DUE_DATE:
                    if (TimeUtils.toDate(parameters.getString(DUE_DATE.key())) == null) {
                        throw new CatalogException("Unrecognised due date. Accepted format is: yyyyMMddHHmmss");
                    }
                    break;
                case ANALYST:
                    LinkedHashMap<String, Object> assigned = (LinkedHashMap<String, Object>) param.getValue();
                    // We obtain the users with access to the study
                    Set<String> users = new HashSet<>(catalogManager.getStudyManager().getGroup(studyStr, "members", sessionId).first()
                            .getUserIds());
                    if (StringUtils.isNotEmpty(String.valueOf(assigned.get("assignee")))
                            && !users.contains(String.valueOf(assigned.get("assignee")))) {
                        throw new CatalogException("Cannot assign clinical analysis to " + assigned.get("assignee")
                                + ". User not found or with no access to the study.");
                    }
                    assigned.put("assignedBy", resource.getUser());
                    break;
                case FILES:
                case STATUS:
                case DISORDER:
                case FAMILY:
                case PROBAND:
                case COMMENTS:
                case FLAGS:
                    break;
                default:
                    throw new CatalogException("Cannot update " + queryParam);
            }
        }

        // Fetch current clinical analysis to validate updates
        Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), resource.getResource().getUid());
        QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = get(studyStr, query, new QueryOptions(), sessionId);
        ClinicalAnalysis clinicalAnalysis = clinicalAnalysisQueryResult.first();

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key())) {
            Family family = parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), Family.class);
            family = getFullValidatedFamily(family, resource.getStudy(), sessionId);
            clinicalAnalysis.setFamily(family);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), family);
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key())) {
            Individual proband = parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), Individual.class);
            proband = getFullValidatedMember(proband, resource.getStudy(), sessionId);
            clinicalAnalysis.setProband(proband);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), proband);
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key())) {
            Map<String, List<File>> files = (Map<String, List<File>>) parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key());
            clinicalAnalysis.setFiles(files);
        }

        validateClinicalAnalysisFields(clinicalAnalysis, resource.getStudy(), sessionId);
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key())) {
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), clinicalAnalysis.getFiles());
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key())) {
            Map<String, Object> status = (Map<String, Object>) parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key());
            if (!(status instanceof Map) || StringUtils.isEmpty(String.valueOf(status.get("name")))
                    || !ClinicalAnalysis.ClinicalStatus.isValid(String.valueOf(status.get("name")))) {
                throw new CatalogException("Missing or invalid status");
            }
        }

        return clinicalDBAdaptor.update(resource.getResource().getUid(), parameters, QueryOptions.empty());
    }

//    public QueryResult<ClinicalAnalysis> updateInterpretation(String studyStr, String clinicalAnalysisStr,
//                                                List<ClinicalAnalysis.ClinicalInterpretation> interpretations,
//                                                ClinicalAnalysis.Action action, QueryOptions queryOptions, String sessionId)
//            throws CatalogException {
//
//        MyResourceId resource = getId(clinicalAnalysisStr, studyStr, sessionId);
//        authorizationManager.checkClinicalAnalysisPermission(resource.getStudyId(), resource.getResourceId(), resource.getUser(),
//                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);
//
//        ParamUtils.checkObj(interpretations, "interpretations");
//        ParamUtils.checkObj(action, "action");
//
//        if (action != ClinicalAnalysis.Action.REMOVE) {
//            validateInterpretations(interpretations, String.valueOf(resource.getStudyId()), sessionId);
//        }
//
//        switch (action) {
//            case ADD:
//                for (ClinicalAnalysis.ClinicalInterpretation interpretation : interpretations) {
//                    clinicalDBAdaptor.addInterpretation(resource.getResourceId(), interpretation);
//                }
//                break;
//            case SET:
//                clinicalDBAdaptor.setInterpretations(resource.getResourceId(), interpretations);
//                break;
//            case REMOVE:
//                for (ClinicalAnalysis.ClinicalInterpretation interpretation : interpretations) {
//                    clinicalDBAdaptor.removeInterpretation(resource.getResourceId(), interpretation.getId());
//                }
//                break;
//            default:
//                throw new CatalogException("Unexpected action found");
//        }
//
//        return clinicalDBAdaptor.get(resource.getResourceId(), queryOptions);
//    }


    public QueryResult<ClinicalAnalysis> search(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(query, study, sessionId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.get(query, options, userId);
//            authorizationManager.filterClinicalAnalysis(userId, studyId, queryResultAux.getResult());

        return queryResult;
    }

    private void fixQueryObject(Query query, Study study, String sessionId) throws CatalogException {
        if (query.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key())) {
            MyResource<Family> familyResource = catalogManager.getFamilyManager()
                    .getUid(query.getString(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key()), study.getFqn(), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_UID.key(), familyResource.getResource().getUid());
            query.remove(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key());
        }
        if (query.containsKey("sample")) {
            MyResource<Sample> sampleResource = catalogManager.getSampleManager().getUid(query.getString("sample"), study.getFqn(),
                    sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE_UID.key(), sampleResource.getResource().getUid());
            query.remove("sample");
        }
        if (query.containsKey("analystAssignee")) {
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.ANALYST_ASSIGNEE.key(), query.get("analystAssignee"));
            query.remove("analystAssignee");
        }
        if (query.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key())) {
            MyResource<Individual> probandResource = catalogManager.getIndividualManager()
                    .getUid(query.getString(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key()), study.getFqn(), sessionId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_UID.key(), probandResource.getResource().getUid());
            query.remove(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key());
        }
    }

    public QueryResult<ClinicalAnalysis> count(String studyStr, Query query, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(query, study, sessionId);

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

        fixQueryObject(query, study, sessionId);

        // Add study id to the query
        query.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult queryResult = clinicalDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<ClinicalAnalysisAclEntry>> getAcls(String studyStr, List<String> clinicalList, String member, boolean silent,
                                                     String sessionId) throws CatalogException {
        List<QueryResult<ClinicalAnalysisAclEntry>> clinicalAclList = new ArrayList<>(clinicalList.size());
        for (String clinicalAnalysis : clinicalList) {
            try {
                MyResource<ClinicalAnalysis> resource = getUid(clinicalAnalysis, studyStr, sessionId);

                QueryResult<ClinicalAnalysisAclEntry> allClinicalAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allClinicalAcls = authorizationManager.getClinicalAnalysisAcl(resource.getStudy().getUid(),
                            resource.getResource().getUid(), resource.getUser(), member);
                } else {
                    allClinicalAcls = authorizationManager.getAllClinicalAnalysisAcls(resource.getStudy().getUid(),
                            resource.getResource().getUid(), resource.getUser());
                }
                allClinicalAcls.setId(clinicalAnalysis);
                clinicalAclList.add(allClinicalAcls);
            } catch (CatalogException e) {
                if (silent) {
                    clinicalAclList.add(new QueryResult<>(clinicalAnalysis, 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
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

        MyResources<ClinicalAnalysis> resource = getUids(clinicalList, studyStr, sessionId);
        authorizationManager.checkCanAssignOrSeePermissions(resource.getStudy().getUid(), resource.getUser());

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(resource.getStudy().getUid(), members);

        switch (clinicalAclParams.getAction()) {
            case SET:
                // Todo: Remove this in 1.4
                List<String> allClinicalPermissions = EnumSet.allOf(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.class)
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList());
                return authorizationManager.setAcls(resource.getStudy().getUid(), resource.getResourceList().stream()
                                .map(ClinicalAnalysis::getUid)
                                .collect(Collectors.toList()), members, permissions,
                        allClinicalPermissions, Entity.CLINICAL_ANALYSIS);
            case ADD:
                return authorizationManager.addAcls(resource.getStudy().getUid(), resource.getResourceList().stream()
                        .map(ClinicalAnalysis::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.CLINICAL_ANALYSIS);
            case REMOVE:
                return authorizationManager.removeAcls(resource.getResourceList().stream()
                                .map(ClinicalAnalysis::getUid).collect(Collectors.toList()),
                        members, permissions, Entity.CLINICAL_ANALYSIS);
            case RESET:
                return authorizationManager.removeAcls(resource.getResourceList().stream()
                                .map(ClinicalAnalysis::getUid).collect(Collectors.toList()),
                        members, null, Entity.CLINICAL_ANALYSIS);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

}
