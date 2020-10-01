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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
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
import org.opencb.opencga.core.models.file.FileReferenceParam;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public static final QueryOptions INCLUDE_CATALOG_DATA = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.FILES.key()));
    public static final QueryOptions INCLUDE_CLINICAL_INTERPRETATION_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION_ID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS_ID.key()));
    public static final QueryOptions INCLUDE_CLINICAL_INTERPRETATIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key()));

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

        fixQueryObject(study, query, userId);
        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return clinicalDBAdaptor.iterator(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis, QueryOptions options, String token)
            throws CatalogException {
        return create(studyStr, clinicalAnalysis, false, options, token);
    }

    public OpenCGAResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis, boolean createDefaultInterpretation,
                                                  QueryOptions options, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysis", clinicalAnalysis)
                .append("createDefaultInterpretation", createDefaultInterpretation)
                .append("options", options)
                .append("token", token);
        try {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS);

            options = ParamUtils.defaultObject(options, QueryOptions::new);
            ParamUtils.checkObj(clinicalAnalysis, "clinicalAnalysis");
            ParamUtils.checkAlias(clinicalAnalysis.getId(), "id");
            ParamUtils.checkObj(clinicalAnalysis.getType(), "type");
            ParamUtils.checkObj(clinicalAnalysis.getProband(), "proband");

            if (clinicalAnalysis.getInterpretation() != null && StringUtils.isNotEmpty(clinicalAnalysis.getInterpretation().getId())
                    && createDefaultInterpretation) {
                throw new CatalogException("createDefaultInterpretation flag passed together with interpretation '"
                        + clinicalAnalysis.getInterpretation().getId() + "'. Please, choose between initialising a default interpretation "
                        + "or passing an interpretation id");
            }

            clinicalAnalysis.setStatus(ParamUtils.defaultObject(clinicalAnalysis.getStatus(), CustomStatus::new));
            clinicalAnalysis.setInternal(ParamUtils.defaultObject(clinicalAnalysis.getInternal(), ClinicalAnalysisInternal::new));
            clinicalAnalysis.getInternal().setStatus(ParamUtils.defaultObject(clinicalAnalysis.getInternal().getStatus(),
                    ClinicalAnalysisStatus::new));
            clinicalAnalysis.setDisorder(ParamUtils.defaultObject(clinicalAnalysis.getDisorder(),
                    new Disorder("", "", "", Collections.emptyMap(), "", Collections.emptyList())));
            clinicalAnalysis.setDueDate(ParamUtils.defaultObject(clinicalAnalysis.getDueDate(),
                    TimeUtils.getTime(TimeUtils.add1MonthtoDate(TimeUtils.getDate()))));
            clinicalAnalysis.setComments(ParamUtils.defaultObject(clinicalAnalysis.getComments(), Collections.emptyList()));
            clinicalAnalysis.setAudit(ParamUtils.defaultObject(clinicalAnalysis.getAudit(), Collections.emptyList()));

            if (!clinicalAnalysis.getComments().isEmpty()) {
                // Fill author and date
                for (ClinicalComment comment : clinicalAnalysis.getComments()) {
                    comment.setAuthor(userId);
                    comment.setDate(TimeUtils.getTime());
                }
            }

            // Analyst
            QueryOptions userInclude = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.ID.key(),
                    UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams.EMAIL.key()));
            User user;
            if (clinicalAnalysis.getAnalyst() == null || StringUtils.isEmpty(clinicalAnalysis.getAnalyst().getId())) {
                user = userDBAdaptor.get(userId, userInclude).first();
            } else {
                // Validate user
                OpenCGAResult<User> result = userDBAdaptor.get(clinicalAnalysis.getAnalyst().getId(), userInclude);
                if (result.getNumResults() == 0) {
                    throw new CatalogException("User '" + clinicalAnalysis.getAnalyst().getId() + "' not found");
                }
                user = result.first();
            }
            clinicalAnalysis.setAnalyst(new ClinicalAnalyst(user.getId(), user.getName(), user.getEmail(), userId, TimeUtils.getTime()));

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

                if (clinicalAnalysis.getFamily().getMembers() == null || clinicalAnalysis.getFamily().getMembers().isEmpty()) {
                    // We assign the ones obtained from the db
                    clinicalAnalysis.getFamily().setMembers(family.getMembers());
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

                // Validate the proband has a disorder
                validateDisorder(clinicalAnalysis);
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
            clinicalAnalysis.setModificationDate(TimeUtils.getTime());
            clinicalAnalysis.setDescription(ParamUtils.defaultString(clinicalAnalysis.getDescription(), ""));
            clinicalAnalysis.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));
            clinicalAnalysis.setAttributes(ParamUtils.defaultObject(clinicalAnalysis.getAttributes(), Collections.emptyMap()));
            clinicalAnalysis.setSecondaryInterpretations(ParamUtils.defaultObject(clinicalAnalysis.getSecondaryInterpretations(),
                    ArrayList::new));
            clinicalAnalysis.setPriority(ParamUtils.defaultObject(clinicalAnalysis.getPriority(), Enums.Priority.MEDIUM));
            clinicalAnalysis.setFlags(ParamUtils.defaultObject(clinicalAnalysis.getFlags(), ArrayList::new));
            clinicalAnalysis.setConsent(ParamUtils.defaultObject(clinicalAnalysis.getConsent(), new ClinicalConsent()));

            sortMembersFromFamily(clinicalAnalysis);

            clinicalAnalysis.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.CLINICAL));
            if (createDefaultInterpretation) {
                clinicalAnalysis.setInterpretation(ParamUtils.defaultObject(clinicalAnalysis.getInterpretation(), Interpretation::new));
                clinicalAnalysis.getInterpretation().setId(ParamUtils.defaultString(clinicalAnalysis.getInterpretation().getId(),
                        clinicalAnalysis.getId() + ".1"));
            }

            if (clinicalAnalysis.getInterpretation() != null) {
                catalogManager.getInterpretationManager().validateNewInterpretation(study, clinicalAnalysis.getInterpretation(),
                        clinicalAnalysis.getId(), userId);
            }

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

    private void validateDisorder(ClinicalAnalysis clinicalAnalysis) throws CatalogException {
        if (clinicalAnalysis.getProband() == null) {
            throw new CatalogException("Missing proband");
        }
        if (clinicalAnalysis.getProband().getDisorders() == null || clinicalAnalysis.getProband().getDisorders().isEmpty()) {
            throw new CatalogException("Missing list of proband disorders");
        }
        if (clinicalAnalysis.getDisorder() != null && StringUtils.isNotEmpty(clinicalAnalysis.getDisorder().getId())) {
            boolean found = false;
            for (Disorder disorder : clinicalAnalysis.getProband().getDisorders()) {
                if (clinicalAnalysis.getDisorder().getId().equals(disorder.getId())) {
                    found = true;
                    clinicalAnalysis.setDisorder(disorder);
                }
            }
            if (!found) {
                throw new CatalogException("Disorder '" + clinicalAnalysis.getDisorder().getId() + "' does not seem to be one of the "
                        + "proband disorders");
            }
        }
    }

    private void obtainFiles(Study study, ClinicalAnalysis clinicalAnalysis, String userId) throws CatalogException {
        Set<String> sampleSet = new HashSet<>();
        if (clinicalAnalysis.getFamily() != null && clinicalAnalysis.getFamily().getMembers() != null) {
            for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                if (member.getSamples() != null) {
                    for (Sample sample : member.getSamples()) {
                        sampleSet.add(sample.getId());
                    }
                }
            }
        } else if (clinicalAnalysis.getProband() != null && clinicalAnalysis.getProband().getSamples() != null) {
            for (Sample sample : clinicalAnalysis.getProband().getSamples()) {
                sampleSet.add(sample.getId());
            }
        }

        if (clinicalAnalysis.getFiles() != null && !clinicalAnalysis.getFiles().isEmpty()) {
            throw new CatalogException("Cannot obtain map of files if this is already provided");
        }

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), new ArrayList<>(sampleSet))
                .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), Arrays.asList(File.Bioformat.ALIGNMENT, File.Bioformat.VARIANT,
                        File.Bioformat.COVERAGE));
        OpenCGAResult<File> fileResults = fileDBAdaptor.get(study.getUid(), query, new QueryOptions(), userId);

        Map<String, List<File>> fileMap = new HashMap<>();
        for (File file : fileResults.getResults()) {
            for (String sampleId : file.getSampleIds()) {
                if (sampleSet.contains(sampleId)) {
                    if (!fileMap.containsKey(sampleId)) {
                        fileMap.put(sampleId, new LinkedList<>());
                    }
                    fileMap.get(sampleId).add(file);
                }
            }
        }

        Set<File> caFiles = new HashSet<>();
        for (Map.Entry<String, List<File>> entry : fileMap.entrySet()) {
            caFiles.addAll(entry.getValue());
        }
        clinicalAnalysis.setFiles(new ArrayList<>(caFiles));
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

        // Look for all the samples associated to the files
        Query query = new Query(FileDBAdaptor.QueryParams.ID.key(),
                clinicalAnalysis.getFiles().stream().map(File::getId).collect(Collectors.toList()));
        OpenCGAResult<File> fileResults = fileDBAdaptor.get(study.getUid(), query, new QueryOptions(), userId);

        if (fileResults.getNumResults() != clinicalAnalysis.getFiles().size()) {
            Set<String> fileIds = clinicalAnalysis.getFiles().stream().map(File::getId).collect(Collectors.toSet());
            String notFoundFiles = fileResults.getResults().stream().map(File::getId).filter(f -> !fileIds.contains(f))
                    .collect(Collectors.joining(", "));
            throw new CatalogException("Files '" + notFoundFiles + "' not found");
        }

        // Complete file information
        clinicalAnalysis.setFiles(fileResults.getResults());

        // Validate the file ids passed are related to the samples
        for (File file : clinicalAnalysis.getFiles()) {
            if (CollectionUtils.isNotEmpty(file.getSampleIds())) {
                boolean found = false;
                for (String sampleId : file.getSampleIds()) {
                    if (sampleMap.containsKey(sampleId)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new CatalogException("Clinical analysis file (" + file.getId() + ") contains sample ids not related to any "
                            + "member/proband");
                }
            }
        }

//        for (File caFile : clinicalAnalysis.getFiles()) {
//            List<String> fileIds = caFile.getFiles().stream().map(File::getId).collect(Collectors.toList());
//            InternalGetDataResult<File> fileResult = catalogManager.getFileManager().internalGet(study.getUid(), fileIds, new Query(),
//                    new QueryOptions(), userId, false);
//            // Validate sample id belongs to files
//            for (File file : fileResult.getResults()) {
//                if (!file.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet())
//                        .contains(sampleMap.get(caFile.getSampleId()))) {
//                    throw new CatalogException("Associated file '" + file.getPath() + "' seems not to be related to sample '"
//                            + caFile.getSampleId() + "'.");
//                }
//            }
//        }
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
                OpenCGAResult<ClinicalAnalysis> queryResult = update(study, clinicalAnalysis, updateParams, userId, options);
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

            OpenCGAResult<ClinicalAnalysis> updateResult = update(study, clinicalAnalysis, updateParams, userId, options);
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

                OpenCGAResult<ClinicalAnalysis> updateResult = update(study, clinicalAnalysis, updateParams, userId, options);
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
                                                   String userId, QueryOptions options) throws CatalogException {
        authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(), userId,
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

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

        if (updateParams.getComments() != null && !updateParams.getComments().isEmpty()) {
            List<ClinicalComment> comments = updateParams.getComments().stream()
                    .map(c -> new ClinicalComment(userId, c.getMessage(), c.getTags(), TimeUtils.getTime()))
                    .collect(Collectors.toList());
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), comments);
        }

        if (parameters.get(InterpretationDBAdaptor.QueryParams.ANALYST.key()) != null) {
            if (StringUtils.isNotEmpty(updateParams.getAnalyst().getId())) {
                QueryOptions userOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.ID.key(),
                        UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams.EMAIL.key()));
                // Check user exists
                OpenCGAResult<User> userResult = userDBAdaptor.get(updateParams.getAnalyst().getId(), userOptions);
                if (userResult.getNumResults() == 0) {
                    throw new CatalogException("User '" + updateParams.getAnalyst().getId() + "' not found");
                }

                parameters.put(InterpretationDBAdaptor.QueryParams.ANALYST.key(), new ClinicalAnalyst(userResult.first().getId(),
                        userResult.first().getName(), userResult.first().getEmail(), userId, TimeUtils.getTime()));
            } else {
                // Remove assignee
                parameters.put(InterpretationDBAdaptor.QueryParams.ANALYST.key(), new ClinicalAnalyst("", "", "", userId,
                        TimeUtils.getTime()));
            }
        }

        if (updateParams.getFiles() != null && !updateParams.getFiles().isEmpty()) {
            clinicalAnalysis.setFiles(updateParams.getFiles().stream().map(FileReferenceParam::toFile).collect(Collectors.toList()));

            // Validate files
            validateFiles(study, clinicalAnalysis, userId);
        }

        if (updateParams.getFiles() != null && !updateParams.getFiles().isEmpty()) {
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), clinicalAnalysis.getFiles());
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key())) {
            // Assign the disorder to be updated to the clinicalAnalysis obtained from the DB so it can be checked in context
            clinicalAnalysis.setDisorder(updateParams.getDisorder().toDisorder());
            validateDisorder(clinicalAnalysis);
            // Fill parameter to be updated with complete disorder information
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key(), clinicalAnalysis.getDisorder());
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_STATUS.key())) {
            Map<String, Object> status = (Map<String, Object>) parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_STATUS.key());
            if (!(status instanceof Map) || StringUtils.isEmpty(String.valueOf(status.get("name")))
                    || !ClinicalAnalysisStatus.isValid(String.valueOf(status.get("name")))) {
                throw new CatalogException("Missing or invalid status");
            }
        }

        return clinicalDBAdaptor.update(clinicalAnalysis.getUid(), parameters, options);
    }

    /**
     * Sort the family members in the following order: proband, father, mother, others.
     *
     * @param clinicalAnalysis Clinical analysis.
     * @return false if it could not be sorted, true otherwise.
     */
    private boolean sortMembersFromFamily(ClinicalAnalysis clinicalAnalysis) {
        if (clinicalAnalysis.getFamily() == null || ListUtils.isEmpty(clinicalAnalysis.getFamily().getMembers())
                || clinicalAnalysis.getFamily().getRoles().isEmpty()) {
            return false;
        }

        Family family = clinicalAnalysis.getFamily();
        String probandId = clinicalAnalysis.getProband().getId();
        if (family.getRoles().get(probandId).isEmpty()) {
            return false;
        }

        // Role -> list of individuals
        Map<Family.FamiliarRelationship, List<Individual>> roleToProband = new HashMap<>();
        roleToProband.put(Family.FamiliarRelationship.PROBAND, Collections.singletonList(clinicalAnalysis.getProband()));
        for (Individual member : family.getMembers()) {
            if (member.getId().equals(probandId)) {
                continue;
            }

            Family.FamiliarRelationship role = family.getRoles().get(probandId).get(member.getId());
            if (role == null) {
                return false;
            }
            if (!roleToProband.containsKey(role)) {
                roleToProband.put(role, new ArrayList<>());
            }
            roleToProband.get(role).add(member);
        }

        List<Individual> members = new ArrayList<>(family.getMembers().size());
        if (roleToProband.containsKey(Family.FamiliarRelationship.PROBAND)) {
            members.add(roleToProband.get(Family.FamiliarRelationship.PROBAND).get(0));
            roleToProband.remove(Family.FamiliarRelationship.PROBAND);
        }
        if (roleToProband.containsKey(Family.FamiliarRelationship.FATHER)) {
            members.add(roleToProband.get(Family.FamiliarRelationship.FATHER).get(0));
            roleToProband.remove(Family.FamiliarRelationship.FATHER);
        }
        if (roleToProband.containsKey(Family.FamiliarRelationship.MOTHER)) {
            members.add(roleToProband.get(Family.FamiliarRelationship.MOTHER).get(0));
            roleToProband.remove(Family.FamiliarRelationship.MOTHER);
        }
        // Add the rest of the members
        for (Family.FamiliarRelationship role : roleToProband.keySet()) {
            for (Individual individual : roleToProband.get(role)) {
                members.add(individual);
            }
        }

        family.setMembers(members);
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

    protected void fixQueryObject(Study study, Query query, String user) throws CatalogException {
        super.fixQueryObject(query);

        if (query.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key())) {
            List<String> samples = query.getAsStringList(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key());
            InternalGetDataResult<Sample> result = catalogManager.getSampleManager().internalGet(study.getUid(),
                    samples, SampleManager.INCLUDE_SAMPLE_IDS, user, true);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key(),
                        result.getResults().stream().map(Sample::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key(), -1);
            }
        }
        if (query.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.MEMBER.key())) {
            List<String> members = query.getAsStringList(ClinicalAnalysisDBAdaptor.QueryParams.MEMBER.key());
            InternalGetDataResult<Individual> result = catalogManager.getIndividualManager().internalGet(study.getUid(),
                    members, IndividualManager.INCLUDE_INDIVIDUAL_IDS, user, true);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.MEMBER.key(),
                        result.getResults().stream().map(Individual::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.MEMBER.key(), -1);
            }
        }
        if (query.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key())) {
            List<String> families = query.getAsStringList(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key());
            InternalGetDataResult<Family> result = catalogManager.getFamilyManager().internalGet(study.getUid(),
                    families, FamilyManager.INCLUDE_FAMILY_IDS, user, true);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(),
                        result.getResults().stream().map(Family::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), -1);
            }
        }

        if (query.containsKey("analystAssignee")) {
            query.put(ClinicalAnalysisDBAdaptor.QueryParams.ANALYST_ID.key(), query.get("analystAssignee"));
            query.remove("analystAssignee");
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
                OpenCGAResult<ClinicalAnalysis> internalResult = internalGet(study.getUid(), id, INCLUDE_CLINICAL_INTERPRETATION_IDS,
                        userId);
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
        if (clinicalAnalysis.getInterpretation() != null || CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
            throw new CatalogException("Deleting a Clinical Analysis containing interpretations is forbidden.");
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

            iterator = clinicalDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_CLINICAL_INTERPRETATION_IDS, userId);

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
                                                              AclParams clinicalAclParams, ParamUtils.AclAction action, boolean propagate,
                                                              String token)
            throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, user);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("clinicalList", clinicalList)
                .append("memberIds", memberIds)
                .append("clinicalAclParams", clinicalAclParams)
                .append("action", action)
                .append("token", token);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            auditManager.initAuditBatch(operationUuid);

            if (clinicalList == null || clinicalList.isEmpty()) {
                throw new CatalogException("Update ACL: Missing 'clinicalAnalysis' parameter");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(clinicalAclParams.getPermissions())) {
                permissions = Arrays.asList(clinicalAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions::valueOf);
            }

            OpenCGAResult<ClinicalAnalysis> queryResult = internalGet(study.getUid(), clinicalList, INCLUDE_CATALOG_DATA, user, false);

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

            List<Long> clinicalUidList = queryResult.getResults().stream().map(ClinicalAnalysis::getUid).collect(Collectors.toList());
            List<AuthorizationManager.CatalogAclParams> aclParamsList = new LinkedList<>();
            AuthorizationManager.CatalogAclParams.addToList(clinicalUidList, permissions, Enums.Resource.CLINICAL_ANALYSIS, aclParamsList);

            if (propagate) {
                // Obtain the whole list of implicity permissions
                Set<String> allPermissions = new HashSet<>(permissions);
                if (action == ParamUtils.AclAction.ADD || action == ParamUtils.AclAction.SET) {
                    // We also fetch the implicit permissions just in case
                    allPermissions.addAll(permissions
                            .stream()
                            .map(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions::valueOf)
                            .map(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                }

                // Only propagate VIEW and WRITE permissions
                List<String> propagatedPermissions = new LinkedList<>();
                for (String permission : allPermissions) {
                    if (ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name().equals(permission)
                            || ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE.name().equals(permission)) {
                        propagatedPermissions.add(permission);
                    }
                }

                // Extract the family, individual, sample and file uids to propagate permissions
                List<Long> familyUids = queryResult.getResults().stream()
                        .map(c -> c.getFamily() != null ? c.getFamily().getUid() : 0)
                        .filter(familyUid -> familyUid > 0)
                        .distinct()
                        .collect(Collectors.toList());
                AuthorizationManager.CatalogAclParams.addToList(familyUids, propagatedPermissions, Enums.Resource.FAMILY, aclParamsList);

                List<Long> individualUids = queryResult.getResults().stream()
                        .flatMap(c -> (c.getFamily() != null && !c.getFamily().getMembers().isEmpty())
                                ? c.getFamily().getMembers().stream()
                                : Stream.of(c.getProband()))
                        .map(Individual::getUid)
                        .distinct()
                        .collect(Collectors.toList());
                AuthorizationManager.CatalogAclParams.addToList(individualUids, propagatedPermissions, Enums.Resource.INDIVIDUAL,
                        aclParamsList);

                List<Long> sampleUids = queryResult.getResults().stream()
                        .flatMap(c -> (c.getFamily() != null && !c.getFamily().getMembers().isEmpty())
                                ? c.getFamily().getMembers().stream()
                                : Stream.of(c.getProband()))
                        .flatMap(i -> i.getSamples() != null ? i.getSamples().stream() : Stream.empty())
                        .map(Sample::getUid)
                        .distinct()
                        .collect(Collectors.toList());
                AuthorizationManager.CatalogAclParams.addToList(sampleUids, propagatedPermissions, Enums.Resource.SAMPLE, aclParamsList);

                List<Long> fileUids = queryResult.getResults().stream()
                        .flatMap(c -> c.getFiles() != null ? c.getFiles().stream() : Stream.empty())
                        .map(File::getUid)
                        .distinct()
                        .collect(Collectors.toList());
                AuthorizationManager.CatalogAclParams.addToList(fileUids, propagatedPermissions, Enums.Resource.FILE, aclParamsList);
            }

            OpenCGAResult<Map<String, List<String>>> queryResults;
            switch (action) {
                case SET:
                    queryResults = authorizationManager.setAcls(study.getUid(), members, aclParamsList);
                    break;
                case ADD:
                    queryResults = authorizationManager.addAcls(study.getUid(), members, aclParamsList);
                    break;
                case REMOVE:
                    queryResults = authorizationManager.removeAcls(members, aclParamsList);
                    break;
                case RESET:
                    for (AuthorizationManager.CatalogAclParams aclParams : aclParamsList) {
                        aclParams.setPermissions(null);
                    }
                    queryResults = authorizationManager.removeAcls(members, aclParamsList);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            for (ClinicalAnalysis clinicalAnalysis : queryResult.getResults()) {
                auditManager.audit(operationUuid, user, Enums.Action.UPDATE_ACLS, Enums.Resource.CLINICAL_ANALYSIS,
                        clinicalAnalysis.getId(), clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }

            auditManager.finishAuditBatch(operationUuid);

            return queryResults;
        } catch (CatalogException e) {
            if (clinicalList != null) {
                for (String clinicalId : clinicalList) {
                    auditManager.audit(operationUuid, user, Enums.Action.UPDATE_ACLS, Enums.Resource.CLINICAL_ANALYSIS, clinicalId, "",
                            study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            auditManager.finishAuditBatch(operationUuid);
            throw e;
        }
    }

}
