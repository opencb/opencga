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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisManager extends ResourceManager<ClinicalAnalysis> {

    private UserManager userManager;
    private StudyManager studyManager;

    protected static Logger logger = LoggerFactory.getLogger(ClinicalAnalysisManager.class);

    public static final QueryOptions INCLUDE_CLINICAL_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key()));
    public static final QueryOptions INCLUDE_CLINICAL_INTERPRETATIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS_ID.key()));

    ClinicalAnalysisManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                            DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.CLINICAL_ANALYSIS;
    }

    @Override
    OpenCGAResult<ClinicalAnalysis> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        ParamUtils.checkIsSingleID(entry);

        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UuidUtils.isOpenCgaUuid(entry)) {
            queryCopy.put(ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), entry);
        }

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
//                ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(),
//                ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
//                ClinicalAnalysisDBAdaptor.QueryParams.RELEASE.key(), ClinicalAnalysisDBAdaptor.QueryParams.ID.key(),
//                ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key()));
        OpenCGAResult<ClinicalAnalysis> analysisDataResult = clinicalDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
        if (analysisDataResult.getNumResults() == 0) {
            analysisDataResult = clinicalDBAdaptor.get(queryCopy, queryOptions);
            if (analysisDataResult.getNumResults() == 0) {
                throw new CatalogException("Clinical Analysis '" + entry + "' not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. '" + user + "' is not allowed to see the Clinical Analysis '"
                        + entry + "'.");
            }
        } else if (analysisDataResult.getNumResults() > 1) {
            throw new CatalogException("More than one clinical analysis found based on '" + entry + "'.");
        } else {
            return analysisDataResult;
        }
    }

    @Override
    InternalGetDataResult<ClinicalAnalysis> internalGet(long studyUid, List<String> entryList, @Nullable Query query,
                                                        QueryOptions options, String user, boolean ignoreException)
            throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing clinical analysis entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();

        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        Function<ClinicalAnalysis, String> clinicalStringFunction = ClinicalAnalysis::getId;
        ClinicalAnalysisDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            ClinicalAnalysisDBAdaptor.QueryParams param = ClinicalAnalysisDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = ClinicalAnalysisDBAdaptor.QueryParams.UUID;
                clinicalStringFunction = ClinicalAnalysis::getUuid;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<ClinicalAnalysis> analysisDataResult = clinicalDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        if (ignoreException || analysisDataResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, clinicalStringFunction, analysisDataResult, ignoreException, false);
        }
        // Query without adding the user check
        OpenCGAResult<ClinicalAnalysis> resultsNoCheck = clinicalDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == analysisDataResult.getNumResults()) {
            throw CatalogException.notFound("clinical analyses",
                    getMissingFields(uniqueList, analysisDataResult.getResults(), clinicalStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the clinical "
                    + "analyses.");
        }
    }

    @Override
    public DBIterator<ClinicalAnalysis> iterator(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return clinicalDBAdaptor.iterator(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis, QueryOptions options,
                                               String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysis", clinicalAnalysis)
                .append("options", options)
                .append("token", token);
        try {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS);

            options = ParamUtils.defaultObject(options, QueryOptions::new);
            ParamUtils.checkObj(clinicalAnalysis, "clinicalAnalysis");
            ParamUtils.checkAlias(clinicalAnalysis.getId(), "id");
            ParamUtils.checkObj(clinicalAnalysis.getType(), "type");
            ParamUtils.checkObj(clinicalAnalysis.getDueDate(), "dueDate");
            ParamUtils.checkObj(clinicalAnalysis.getProband(), "proband");
            clinicalAnalysis.setStatus(ParamUtils.defaultObject(clinicalAnalysis.getStatus(), CustomStatus::new));
            clinicalAnalysis.setInternal(ParamUtils.defaultObject(clinicalAnalysis.getInternal(), ClinicalAnalysisInternal::new));
            clinicalAnalysis.getInternal().setStatus(ParamUtils.defaultObject(clinicalAnalysis.getInternal().getStatus(),
                    ClinicalAnalysisStatus::new));

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

            if (StringUtils.isEmpty(clinicalAnalysis.getProband().getId())) {
                throw new CatalogException("Missing proband id");
            }

            if (clinicalAnalysis.getType() == ClinicalAnalysis.Type.FAMILY) {
                ParamUtils.checkObj(clinicalAnalysis.getFamily(), "family");
                if (StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
                    throw new CatalogException("Missing family id");
                }

                OpenCGAResult<Family> familyDataResult = catalogManager.getFamilyManager().get(study.getFqn(),
                        clinicalAnalysis.getFamily().getId(), new QueryOptions(), token);
                if (familyDataResult.getNumResults() == 0) {
                    throw new CatalogException("Family " + clinicalAnalysis.getFamily().getId() + " not found");
                }
                Family family = familyDataResult.first();

                // Get full sample information of each member
                for (Individual member : family.getMembers()) {
                    if (member.getSamples() != null && !member.getSamples().isEmpty()) {
                        Query query = new Query(SampleDBAdaptor.QueryParams.UID.key(),
                                member.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()));
                        OpenCGAResult<Sample> sampleResult = sampleDBAdaptor.get(study.getUid(), query, new QueryOptions(), userId);
                        member.setSamples(sampleResult.getResults());
                    }
                }

                boolean found = false;
                for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                    if (StringUtils.isNotEmpty(member.getId()) && clinicalAnalysis.getProband().getId().equals(member.getId())) {
                        if (member.getSamples() != null && !member.getSamples().isEmpty()) {
                            // Validate proband has no samples or it has exactly the same ones
                            if (clinicalAnalysis.getProband().getSamples() != null) {
                                Set<String> sampleIdsInFamily = member.getSamples().stream().map(Sample::getId).collect(Collectors.toSet());
                                Set<String> sampleIdsInProband = clinicalAnalysis.getProband().getSamples().stream().map(Sample::getId)
                                        .collect(Collectors.toSet());
                                if (sampleIdsInFamily.size() != sampleIdsInProband.size()
                                        || !sampleIdsInFamily.containsAll(sampleIdsInProband)) {
                                    throw new CatalogException("Sample ids '" + sampleIdsInProband + "' found in proband field do not "
                                            + "match the sample ids found for proband under family field '" + sampleIdsInFamily + "'.");
                                }
                            }
                        }

                        found = true;
                    }
                }
                if (!found) {
                    throw new CatalogException("Missing proband in the family");
                }

                boolean membersProvided = false;
                boolean samplesProvided = false;
                if (clinicalAnalysis.getFamily() != null && clinicalAnalysis.getFamily().getMembers() != null
                        && !clinicalAnalysis.getFamily().getMembers().isEmpty()) {
                    membersProvided = true;
                    for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                        if (member.getSamples() != null && !member.getSamples().isEmpty()) {
                            samplesProvided = true;
                            break;
                        }
                    }
                }

                if (membersProvided) {
                    // Validate and filter out members and samples
                    Map<String, Individual> individualMap = new HashMap<>();
                    for (Individual member : family.getMembers()) {
                        individualMap.put(member.getId(), member);
                    }

                    List<Individual> filteredMemberList = new ArrayList<>(clinicalAnalysis.getFamily().getMembers().size());
                    for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                        if (!individualMap.containsKey(member.getId())) {
                            throw new CatalogException("Member '" + member.getId() + "' could not be found in family '" + family.getId()
                                    + "'.");
                        }
                        filteredMemberList.add(individualMap.get(member.getId()));
                    }

                    family.setMembers(filteredMemberList);

                    if (samplesProvided) {
                        // For each member, we will filter out the samples selected by the user for the analysis
                        for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                            List<Sample> sampleList;

                            if (member.getSamples() == null || member.getSamples().size() == 0) {
                                sampleList = individualMap.get(member.getId()).getSamples();
//                                throw new CatalogException("Missing sample(s) for member '" + member.getId() + "'.");
                            } else {
                                Map<String, Sample> sampleMap = new HashMap<>();
                                for (Sample sample : individualMap.get(member.getId()).getSamples()) {
                                    sampleMap.put(sample.getId(), sample);
                                }

                                sampleList = new ArrayList<>(member.getSamples().size());
                                for (Sample sample : member.getSamples()) {
                                    if (!sampleMap.containsKey(sample.getId())) {
                                        throw new CatalogException("Sample '" + sample.getId() + "' could not be found in member '"
                                                + member.getId() + "'.");
                                    }
                                    sampleList.add(sampleMap.get(sample.getId()));
                                }
                            }

                            // Edit the list of samples of the member from the 'family' object
                            individualMap.get(member.getId()).setSamples(sampleList);
                        }
                    }
                }

                // Validate there is only one sample per member
                for (Individual member : family.getMembers()) {
                    if (member.getSamples() == null || member.getSamples().isEmpty()) {
                        throw new CatalogException("Missing sample for member '" + member.getId() + "'");
                    }
                    if (member.getSamples().size() > 1) {
                        throw new CatalogException("More than one sample found for member '" + member.getId() + "'.");
                    }
                }

                clinicalAnalysis.setFamily(family);
                Individual proband = null;
                for (Individual member : family.getMembers()) {
                    if (member.getId().equals(clinicalAnalysis.getProband().getId())) {
                        proband = member;
                        break;
                    }
                }
                if (proband == null) {
                    throw new CatalogException("Proband '" + clinicalAnalysis.getProband().getId() + "' not found in family");
                }
                clinicalAnalysis.setProband(proband);
            } else {
                OpenCGAResult<Individual> individualOpenCGAResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                        clinicalAnalysis.getProband().getId(), new Query(), new QueryOptions(), userId);
                if (individualOpenCGAResult.getNumResults() == 0) {
                    throw new CatalogException("Proband '" + clinicalAnalysis.getProband().getId() + "' not found.");
                }

                Individual proband = individualOpenCGAResult.first();
                if (clinicalAnalysis.getProband().getSamples() != null && !clinicalAnalysis.getProband().getSamples().isEmpty()) {
                    Map<String, Sample> sampleMap = new HashMap<>();
                    for (Sample sample : proband.getSamples()) {
                        sampleMap.put(sample.getId(), sample);
                    }

                    List<Sample> sampleList = new ArrayList<>(clinicalAnalysis.getProband().getSamples().size());
                    for (Sample sample : clinicalAnalysis.getProband().getSamples()) {
                        if (!sampleMap.containsKey(sample.getId())) {
                            throw new CatalogException("Sample '" + sample.getId() + "' could not be found in proband '" + proband.getId()
                                    + "'.");
                        }
                        sampleList.add(sampleMap.get(sample.getId()));
                    }
                    proband.setSamples(sampleList);
                }

                clinicalAnalysis.setProband(proband);

                if (proband.getSamples() == null || proband.getSamples().isEmpty()) {
                    throw new CatalogException("Missing samples for proband '" + proband.getId() + "'");
                }

                if (clinicalAnalysis.getType() == ClinicalAnalysis.Type.CANCER) {
                    // Validate there are up to 2 samples
                    if (proband.getSamples().size() > 2) {
                        throw new CatalogException("More than two samples found for proband '" + proband.getId() + "'.");
                    }
                    boolean somatic = false;
                    for (Sample sample : proband.getSamples()) {
                        if (somatic && sample.isSomatic()) {
                            throw new CatalogException("Found more than one somatic sample for proband '" + proband.getId() + "'.");
                        }
                        somatic = somatic || sample.isSomatic();
                    }
                    if (!somatic) {
                        throw new CatalogException("Could not find any somatic sample for proband '" + proband.getId() + "'.");
                    }
                }
            }

            if (clinicalAnalysis.getFiles() != null && !clinicalAnalysis.getFiles().isEmpty()) {
                validateFiles(study, clinicalAnalysis, userId);
            } else {
                obtainFiles(study, clinicalAnalysis, userId);
            }

            clinicalAnalysis.setCreationDate(TimeUtils.getTime());
            clinicalAnalysis.setDescription(ParamUtils.defaultString(clinicalAnalysis.getDescription(), ""));
            clinicalAnalysis.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));
            clinicalAnalysis.setAttributes(ParamUtils.defaultObject(clinicalAnalysis.getAttributes(), Collections.emptyMap()));
            clinicalAnalysis.setInterpretations(ParamUtils.defaultObject(clinicalAnalysis.getInterpretations(), ArrayList::new));
            clinicalAnalysis.setPriority(ParamUtils.defaultObject(clinicalAnalysis.getPriority(), Enums.Priority.MEDIUM));
            clinicalAnalysis.setFlags(ParamUtils.defaultObject(clinicalAnalysis.getFlags(), ArrayList::new));
            clinicalAnalysis.setConsent(ParamUtils.defaultObject(clinicalAnalysis.getConsent(), new ClinicalConsent()));

            validateRoleToProband(clinicalAnalysis);
            sortMembersFromFamily(clinicalAnalysis);

            clinicalAnalysis.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.CLINICAL));
            OpenCGAResult result = clinicalDBAdaptor.insert(study.getUid(), clinicalAnalysis, options);

            auditManager.auditCreate(userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(), clinicalAnalysis.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            OpenCGAResult<ClinicalAnalysis> queryResult = clinicalDBAdaptor.get(study.getUid(), clinicalAnalysis.getId(),
                    QueryOptions.empty());
            queryResult.setTime(queryResult.getTime() + result.getTime());

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(), "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void validateRoleToProband(ClinicalAnalysis clinicalAnalysis) throws CatalogException {
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
                                roleToProband.put(member.getId(), ClinicalAnalysis.FamiliarRelationship.BROTHER);
                            } else if (IndividualProperty.Sex.FEMALE.equals(member.getSex())) {
                                roleToProband.put(member.getId(), ClinicalAnalysis.FamiliarRelationship.SISTER);
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

        // Validate that proband, mother and father only exists once.
        Map<ClinicalAnalysis.FamiliarRelationship, Long> roleCount = clinicalAnalysis.getRoleToProband().values().stream().
                collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        for (Map.Entry<ClinicalAnalysis.FamiliarRelationship, Long> roleEntry : roleCount.entrySet()) {
            switch (roleEntry.getKey()) {
                case PROBAND:
                case FATHER:
                case MOTHER:
                    if (roleEntry.getValue() > 1) {
                        throw new CatalogException("Found duplicated " + roleEntry.getKey() + " role");
                    }
                    break;
                default:
                    break;
            }
        }

    }

    private void obtainFiles(Study study, ClinicalAnalysis clinicalAnalysis, String userId) throws CatalogException {
        Map<Long, String> sampleMap = new HashMap<>();
        if (clinicalAnalysis.getFamily() != null && clinicalAnalysis.getFamily().getMembers() != null) {
            for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                if (member.getSamples() != null) {
                    for (Sample sample : member.getSamples()) {
                        sampleMap.put(sample.getUid(), sample.getId());
                    }
                }
            }
        } else if (clinicalAnalysis.getProband() != null && clinicalAnalysis.getProband().getSamples() != null) {
            for (Sample sample : clinicalAnalysis.getProband().getSamples()) {
                sampleMap.put(sample.getUid(), sample.getId());
            }
        }

        if (clinicalAnalysis.getFiles() != null && !clinicalAnalysis.getFiles().isEmpty()) {
            throw new CatalogException("Cannot obtain map of files if this is already provided");
        }

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleMap.keySet())
                .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), Arrays.asList(File.Bioformat.ALIGNMENT, File.Bioformat.VARIANT,
                        File.Bioformat.COVERAGE));
        OpenCGAResult<File> fileResults = fileDBAdaptor.get(study.getUid(), query, new QueryOptions(), userId);

        Map<String, List<File>> fileMap = new HashMap<>();
        for (File file : fileResults.getResults()) {
            for (Sample sample : file.getSamples()) {
                if (sampleMap.containsKey(sample.getUid())) {
                    String sampleId = sampleMap.get(sample.getUid());
                    if (!fileMap.containsKey(sampleId)) {
                        fileMap.put(sampleId, new LinkedList<>());
                    }
                    fileMap.get(sampleId).add(file);
                }
            }
        }

        clinicalAnalysis.setFiles(fileMap);
    }

    private void validateFiles(Study study, ClinicalAnalysis clinicalAnalysis, String userId) throws CatalogException {
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

        if (clinicalAnalysis.getFiles() == null || clinicalAnalysis.getFiles().isEmpty()) {
            throw new CatalogException("Found empty map of files");
        }

        if (!clinicalAnalysis.getFiles().keySet().containsAll(sampleMap.keySet())) {
            throw new CatalogException("Map of files contains sample ids not related to any member/proband");
        }

        // Validate the file ids passed are related to the samples
        Map<String, List<File>> fileMap = new LinkedHashMap<>();
        for (Map.Entry<String, List<File>> entry : clinicalAnalysis.getFiles().entrySet()) {
            List<String> fileIds = entry.getValue().stream().map(File::getId).collect(Collectors.toList());
            InternalGetDataResult<File> fileResult = catalogManager.getFileManager().internalGet(study.getUid(), fileIds, new Query(),
                    new QueryOptions(), userId, false);
            // Validate sample id belongs to files
            for (File file : fileResult.getResults()) {
                if (!file.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet()).contains(sampleMap.get(entry.getKey()))) {
                    throw new CatalogException("Associated file '" + file.getPath() + "' seems not to be related to sample '"
                            + entry.getKey() + "'.");
                }
            }
            fileMap.put(entry.getKey(), fileResult.getResults());
        }

        clinicalAnalysis.setFiles(fileMap);
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

        OpenCGAResult<Family> familyDataResult = catalogManager.getFamilyManager().get(study.getFqn(), family.getId(), new QueryOptions(),
                sessionId);
        if (familyDataResult.getNumResults() == 0) {
            throw new CatalogException("Family " + family.getId() + " not found");
        }
        Family finalFamily = familyDataResult.first();

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
                OpenCGAResult<Individual> individuals = individualDBAdaptor.get(study.getUid(), query, QueryOptions.empty(),
                        catalogManager.getUserManager().getUserId(sessionId));
                finalFamily.setMembers(individuals.getResults());
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
            OpenCGAResult<Individual> individualDataResult = catalogManager.getIndividualManager().get(study.getFqn(), member.getId(),
                    new QueryOptions(), sessionId);
            if (individualDataResult.getNumResults() == 0) {
                throw new CatalogException("Member " + member.getId() + " not found");
            }

            finalMember = individualDataResult.first();
        } else {
            finalMember = member;
            if (ListUtils.isNotEmpty(samples) && StringUtils.isEmpty(samples.get(0).getUuid())) {
                // We don't have the full sample information...
                OpenCGAResult<Individual> individualDataResult = catalogManager.getIndividualManager().get(study.getFqn(),
                        finalMember.getId(), new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key()),
                        sessionId);
                if (individualDataResult.getNumResults() == 0) {
                    throw new CatalogException("Member " + finalMember.getId() + " not found");
                }

                finalMember.setSamples(individualDataResult.first().getSamples());
            }
        }

        if (ListUtils.isNotEmpty(finalMember.getSamples())) {
            List<Sample> finalSampleList = null;
            if (ListUtils.isNotEmpty(samples)) {

                Map<String, Sample> sampleMap = new HashMap<>();
                for (Sample sample : finalMember.getSamples()) {
                    sampleMap.put(sample.getId(), sample);
                }

                finalSampleList = new ArrayList<>(samples.size());

                // We keep only the original list of samples passed
                for (Sample sample : samples) {
                    finalSampleList.add(sampleMap.get(sample.getId()));
                }
            }
            finalMember.setSamples(finalSampleList);
        }

        return finalMember;
    }

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, Query query, ClinicalUpdateParams updateParams, QueryOptions options,
                                                  String token) throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, Query query, ClinicalUpdateParams updateParams, boolean ignoreException,
                                                  QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse ClinicalUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        DBIterator<ClinicalAnalysis> iterator;
        try {
            fixQueryObject(study, query, userId);
            query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            iterator = clinicalDBAdaptor.iterator(study.getUid(), query, new QueryOptions(), userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<ClinicalAnalysis> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = iterator.next();
            try {
                OpenCGAResult<ClinicalAnalysis> queryResult = update(study, clinicalAnalysis, updateParams, userId, token);
                result.append(queryResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, clinicalAnalysis.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update clinical analysis {}: {}", clinicalAnalysis.getId(), e.getMessage());
                auditManager.auditUpdate(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, String clinicalId, ClinicalUpdateParams updateParams,
                                               QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse ClinicalUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalId", clinicalId)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<ClinicalAnalysis> result = OpenCGAResult.empty();
        String clinicalUuid = "";
        try {
            OpenCGAResult<ClinicalAnalysis> internalResult = internalGet(study.getUid(), clinicalId, QueryOptions.empty(), userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Clinical analysis '" + clinicalId + "' not found");
            }
            ClinicalAnalysis clinicalAnalysis = internalResult.first();

            // We set the proper values for the audit
            clinicalId = clinicalAnalysis.getId();
            clinicalUuid = clinicalAnalysis.getUuid();

            OpenCGAResult<ClinicalAnalysis> updateResult = update(study, clinicalAnalysis, updateParams, userId, token);
            result.append(updateResult);

            auditManager.auditUpdate(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                    clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, clinicalId, e.getMessage());
            result.getEvents().add(event);

            logger.error("Could not update clinical analysis {}: {}", clinicalId, e.getMessage());
            auditManager.auditUpdate(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalId, clinicalUuid,
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    /**
     * Update a Clinical Analysis from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param clinicalIds  List of clinical analysis ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A OpenCGAResult.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, List<String> clinicalIds, ClinicalUpdateParams updateParams,
                                               QueryOptions options, String token) throws CatalogException {
        return update(studyStr, clinicalIds, updateParams, false, options, token);
    }

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, List<String> clinicalIds, ClinicalUpdateParams updateParams,
                                                  boolean ignoreException, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse ClinicalUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalIds", clinicalIds)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("ignoreException", ignoreException)
                .append("token", token);

        OpenCGAResult<ClinicalAnalysis> result = OpenCGAResult.empty();

        auditManager.initAuditBatch(operationId);
        for (String id : clinicalIds) {
            String clinicalAnalysisId = id;
            String clinicalAnalysisUuid = "";
            try {
                OpenCGAResult<ClinicalAnalysis> internalResult = internalGet(study.getUid(), id, QueryOptions.empty(), userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Clinical analysis '" + id + "' not found");
                }
                ClinicalAnalysis clinicalAnalysis = internalResult.first();

                // We set the proper values for the audit
                clinicalAnalysisId = clinicalAnalysis.getId();
                clinicalAnalysisUuid = clinicalAnalysis.getUuid();

                OpenCGAResult<ClinicalAnalysis> updateResult = update(study, clinicalAnalysis, updateParams, userId, token);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update clinical analysis {}: {}", clinicalAnalysisId, e.getMessage());
                auditManager.auditUpdate(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysisId, clinicalAnalysisUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult<ClinicalAnalysis> update(Study study, ClinicalAnalysis clinicalAnalysis, ClinicalUpdateParams updateParams,
                                                String userId, String token) throws CatalogException {
        authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(), userId,
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        if (clinicalAnalysis.getInterpretations() != null && !clinicalAnalysis.getInterpretations().isEmpty()) {
            // Check things there are no fields that cannot be updated once there are interpretations
            if (updateParams.getFiles() != null && !updateParams.getFiles().isEmpty()) {
                throw new CatalogException("Cannot update file map anymore. Interpretations found in clinical analysis '"
                        + clinicalAnalysis.getId() + "'.");
            }
        }

        ObjectMap parameters = new ObjectMap();
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse ClinicalUpdateParams object: " + e.getMessage(), e);
            }
        }
        ParamUtils.checkUpdateParametersMap(parameters);

        if (StringUtils.isNotEmpty(updateParams.getId())) {
            ParamUtils.checkAlias(updateParams.getId(), "id");
        }
        if (StringUtils.isNotEmpty(updateParams.getDueDate()) && TimeUtils.toDate(updateParams.getDueDate()) == null) {
            throw new CatalogException("Unrecognised due date. Accepted format is: yyyyMMddHHmmss");
        }
        if (updateParams.getAnalyst() != null && StringUtils.isNotEmpty(updateParams.getAnalyst().getAssignee())) {
            // We obtain the users with access to the study
            Set<String> users = new HashSet<>(catalogManager.getStudyManager().getGroup(study.getFqn(), "members", token).first()
                    .getUserIds());
            if (!users.contains(updateParams.getAnalyst().getAssignee())) {
                throw new CatalogException("Cannot assign clinical analysis to " + updateParams.getAnalyst().getAssignee()
                        + ". User not found or with no access to the study.");
            }

            Map<String, Object> map = parameters.getMap(ClinicalAnalysisDBAdaptor.QueryParams.ANALYST.key());
            map.put("assignedBy", userId);
        }

        if (updateParams.getFiles() != null && !updateParams.getFiles().isEmpty()) {
            Map<String, List<File>> files = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : updateParams.getFiles().entrySet()) {
                List<File> fileList = entry.getValue().stream().map(fileId -> new File().setId(fileId)).collect(Collectors.toList());
                files.put(entry.getKey(), fileList);
            }
            clinicalAnalysis.setFiles(files);

            // Validate files
            validateFiles(study, clinicalAnalysis, userId);
        }

        if (updateParams.getFiles() != null && !updateParams.getFiles().isEmpty()) {
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

            if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key())) {
                if (sortMembersFromFamily(clinicalAnalysis)) {
                    parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), clinicalAnalysis.getFamily());
                }
            }
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_STATUS.key())) {
            Map<String, Object> status = (Map<String, Object>) parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_STATUS.key());
            if (!(status instanceof Map) || StringUtils.isEmpty(String.valueOf(status.get("name")))
                    || !ClinicalAnalysisStatus.isValid(String.valueOf(status.get("name")))) {
                throw new CatalogException("Missing or invalid status");
            }
        }

        return clinicalDBAdaptor.update(clinicalAnalysis.getUid(), parameters, QueryOptions.empty());
    }

    /**
     * Sort the family members in the following order: proband, father, mother, others.
     *
     * @param clinicalAnalysis Clinical analysis.
     * @return false if it could not be sorted, true otherwise.
     */
    private boolean sortMembersFromFamily(ClinicalAnalysis clinicalAnalysis) {
        if (clinicalAnalysis.getRoleToProband().isEmpty() || clinicalAnalysis.getFamily() == null
                || ListUtils.isEmpty(clinicalAnalysis.getFamily().getMembers())) {
            return false;
        }

        // Role -> list of individuals
        Map<ClinicalAnalysis.FamiliarRelationship, List<Individual>> roleToProband = new HashMap<>();
        for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
            ClinicalAnalysis.FamiliarRelationship role = clinicalAnalysis.getRoleToProband().get(member.getId());
            if (role == null) {
                return false;
            }
            if (!roleToProband.containsKey(role)) {
                roleToProband.put(role, new ArrayList<>());
            }
            roleToProband.get(role).add(member);
        }

        List<Individual> members = new ArrayList<>(clinicalAnalysis.getFamily().getMembers().size());
        if (roleToProband.containsKey(ClinicalAnalysis.FamiliarRelationship.PROBAND)) {
            members.add(roleToProband.get(ClinicalAnalysis.FamiliarRelationship.PROBAND).get(0));
            roleToProband.remove(ClinicalAnalysis.FamiliarRelationship.PROBAND);
        }
        if (roleToProband.containsKey(ClinicalAnalysis.FamiliarRelationship.FATHER)) {
            members.add(roleToProband.get(ClinicalAnalysis.FamiliarRelationship.FATHER).get(0));
            roleToProband.remove(ClinicalAnalysis.FamiliarRelationship.FATHER);
        }
        if (roleToProband.containsKey(ClinicalAnalysis.FamiliarRelationship.MOTHER)) {
            members.add(roleToProband.get(ClinicalAnalysis.FamiliarRelationship.MOTHER).get(0));
            roleToProband.remove(ClinicalAnalysis.FamiliarRelationship.MOTHER);
        }
        // Add the rest of the members
        for (ClinicalAnalysis.FamiliarRelationship role : roleToProband.keySet()) {
            for (Individual individual : roleToProband.get(role)) {
                members.add(individual);
            }
        }

        clinicalAnalysis.getFamily().setMembers(members);
        return true;
    }

    public OpenCGAResult<ClinicalAnalysis> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        fixQueryObject(study, query, userId);
        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return clinicalDBAdaptor.get(study.getUid(), query, options, userId);
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
            OpenCGAResult<Individual> probandDataResult = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    query.getString(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key()), IndividualManager.INCLUDE_INDIVIDUAL_IDS, userId);
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_UID.key(), probandDataResult.first().getUid());
            query.remove(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key());
        }
    }

    public OpenCGAResult<ClinicalAnalysis> count(String studyId, Query query, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            fixQueryObject(study, query, userId);
            query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Long> queryResultAux = clinicalDBAdaptor.count(query, userId);

            auditManager.auditCount(userId, Enums.Resource.CLINICAL_ANALYSIS, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, Enums.Resource.CLINICAL_ANALYSIS, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> clinicalAnalysisIds, ObjectMap params, String token) throws CatalogException {
        return delete(studyStr, clinicalAnalysisIds, params, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> clinicalAnalysisIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        if (CollectionUtils.isEmpty(clinicalAnalysisIds)) {
            throw new CatalogException("Missing list of Clinical Analysis ids");
        }

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysisIds", clinicalAnalysisIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult result = OpenCGAResult.empty();
        for (String id : clinicalAnalysisIds) {
            String clinicalId = id;
            String clinicalUuid = "";
            try {
                OpenCGAResult<ClinicalAnalysis> internalResult = internalGet(study.getUid(), id, INCLUDE_CLINICAL_INTERPRETATIONS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Clinical Analysis '" + id + "' not found");
                }
                ClinicalAnalysis clinicalAnalysis = internalResult.first();

                // We set the proper values for the audit
                clinicalId = clinicalAnalysis.getId();
                clinicalUuid = clinicalAnalysis.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(), userId,
                            ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.DELETE);
                }

                // Check if the ClinicalAnalysis can be deleted
                checkClinicalAnalysisCanBeDeleted(clinicalAnalysis);

                result.append(clinicalDBAdaptor.delete(clinicalAnalysis));

                auditManager.auditDelete(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete Clinical Analysis " + clinicalId + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, clinicalId, e.getMessage());
                result.getEvents().add(event);

                logger.error(errorMsg);
                auditManager.auditDelete(operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalId, clinicalUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    private void checkClinicalAnalysisCanBeDeleted(ClinicalAnalysis clinicalAnalysis) throws CatalogException {
        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getInterpretations())) {
            throw new CatalogException("Deleting ClinicalAnalysis that contains interpretations is forbidden.");
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        return delete(studyStr, query, params, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        params = ParamUtils.defaultObject(params, ObjectMap::new);

        OpenCGAResult result = OpenCGAResult.empty();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the ClinicalAnalyses to be deleted
        DBIterator<ClinicalAnalysis> iterator;
        try {
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = clinicalDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_CLINICAL_INTERPRETATIONS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.CLINICAL_ANALYSIS, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(), userId,
                            ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.DELETE);
                }

                // Check if the sample can be deleted
                checkClinicalAnalysisCanBeDeleted(clinicalAnalysis);

                result.append(clinicalDBAdaptor.delete(clinicalAnalysis));

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete Clinical Analysis " + clinicalAnalysis.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, clinicalAnalysis.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error(errorMsg);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
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

        OpenCGAResult queryResult = clinicalDBAdaptor.groupBy(query, fields, options, userId);
        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<Map<String, List<String>>> getAcls(String studyStr, List<String> clinicalList, String member,
                                                            boolean ignoreException, String token) throws CatalogException {
        OpenCGAResult<Map<String, List<String>>> clinicalAclList = OpenCGAResult.empty();
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, user);

        InternalGetDataResult<ClinicalAnalysis> queryResult = internalGet(study.getUid(), clinicalList, INCLUDE_CLINICAL_IDS, user,
                ignoreException);

        Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
        if (queryResult.getMissing() != null) {
            missingMap = queryResult.getMissing().stream()
                    .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
        }
        int counter = 0;
        for (String clinicalAnalysis : clinicalList) {
            if (!missingMap.containsKey(clinicalAnalysis)) {
                try {
                    OpenCGAResult<Map<String, List<String>>> allClinicalAcls;
                    if (StringUtils.isNotEmpty(member)) {
                        allClinicalAcls = authorizationManager.getClinicalAnalysisAcl(study.getUid(),
                                queryResult.getResults().get(counter).getUid(), user, member);
                    } else {
                        allClinicalAcls = authorizationManager.getAllClinicalAnalysisAcls(study.getUid(),
                                queryResult.getResults().get(counter).getUid(), user);
                    }
                    clinicalAclList.append(allClinicalAcls);
                } catch (CatalogException e) {
                    if (!ignoreException) {
                        throw e;
                    } else {
                        Event event = new Event(Event.Type.ERROR, clinicalAnalysis, missingMap.get(clinicalAnalysis).getErrorMsg());
                        clinicalAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                                Collections.singletonList(new HashMap()), 0));
                    }
                }
                counter += 1;
            } else {
                Event event = new Event(Event.Type.ERROR, clinicalAnalysis, missingMap.get(clinicalAnalysis).getErrorMsg());
                clinicalAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                        Collections.singletonList(new HashMap()), 0));
            }
        }
        return clinicalAclList;
    }

    public OpenCGAResult<Map<String, List<String>>> updateAcl(String studyStr, List<String> clinicalList, String memberIds,
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
        OpenCGAResult<ClinicalAnalysis> queryResult = internalGet(study.getUid(), clinicalList, INCLUDE_CLINICAL_IDS, user, false);

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
                return authorizationManager.setAcls(study.getUid(), queryResult.getResults().stream()
                        .map(ClinicalAnalysis::getUid)
                        .collect(Collectors.toList()), members, permissions, Enums.Resource.CLINICAL_ANALYSIS);
            case ADD:
                return authorizationManager.addAcls(study.getUid(), queryResult.getResults().stream()
                        .map(ClinicalAnalysis::getUid)
                        .collect(Collectors.toList()), members, permissions, Enums.Resource.CLINICAL_ANALYSIS);
            case REMOVE:
                return authorizationManager.removeAcls(queryResult.getResults().stream()
                                .map(ClinicalAnalysis::getUid).collect(Collectors.toList()),
                        members, permissions, Enums.Resource.CLINICAL_ANALYSIS);
            case RESET:
                return authorizationManager.removeAcls(queryResult.getResults().stream()
                                .map(ClinicalAnalysis::getUid).collect(Collectors.toList()),
                        members, null, Enums.Resource.CLINICAL_ANALYSIS);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

}
