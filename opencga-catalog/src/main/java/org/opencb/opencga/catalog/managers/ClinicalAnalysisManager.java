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
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.models.ClinicalAnalysisLoadResult;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.*;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.*;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyCreateParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileReferenceParam;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleCreateParams;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsent;
import org.opencb.opencga.core.models.study.configuration.*;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.catalog.utils.ParamUtils.SaveInterpretationAs.PRIMARY;
import static org.opencb.opencga.catalog.utils.ParamUtils.SaveInterpretationAs.SECONDARY;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisManager extends AnnotationSetManager<ClinicalAnalysis> {

    public static final QueryOptions INCLUDE_CLINICAL_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.TYPE.key(), ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key()));
    public static final QueryOptions INCLUDE_CATALOG_DATA = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.LOCKED.key(), ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.REPORTED_FILES.key(), ClinicalAnalysisDBAdaptor.QueryParams.TYPE.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key()));
    public static final QueryOptions INCLUDE_CLINICAL_INTERPRETATION_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION_UID.key(), ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION_ID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS_UID.key(), ClinicalAnalysisDBAdaptor.QueryParams.LOCKED.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS_ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.TYPE.key()));
    public static final QueryOptions INCLUDE_CLINICAL_INTERPRETATIONS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key(), ClinicalAnalysisDBAdaptor.QueryParams.LOCKED.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key(), ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.TYPE.key()));
    protected static Logger logger = LoggerFactory.getLogger(ClinicalAnalysisManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

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

//    @Override
//    OpenCGAResult<ClinicalAnalysis> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
//            throws CatalogException {
//        ParamUtils.checkIsSingleID(entry);
//
//        Query queryCopy = query == null ? new Query() : new Query(query);
//        queryCopy.put(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
//
//        if (UuidUtils.isOpenCgaUuid(entry)) {
//            queryCopy.put(ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), entry);
//        } else {
//            queryCopy.put(ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), entry);
//        }
//
//        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//        OpenCGAResult<ClinicalAnalysis> analysisDataResult = getClinicalAnalysisDBAdaptor(organizationId).get(studyUid, queryCopy,
//        queryOptions, user);
//        if (analysisDataResult.getNumResults() == 0) {
//            analysisDataResult = getClinicalAnalysisDBAdaptor(organizationId).get(queryCopy, queryOptions);
//            if (analysisDataResult.getNumResults() == 0) {
//                throw new CatalogException("Clinical Analysis '" + entry + "' not found");
//            } else {
//                throw new CatalogAuthorizationException("Permission denied. '" + user + "' is not allowed to see the Clinical Analysis '"
//                        + entry + "'.");
//            }
//        } else if (analysisDataResult.getNumResults() > 1) {
//            throw new CatalogException("More than one clinical analysis found based on '" + entry + "'.");
//        } else {
//            return analysisDataResult;
//        }
//    }

    @Override
    InternalGetDataResult<ClinicalAnalysis> internalGet(String organizationId, long studyUid, List<String> entryList, @Nullable Query query,
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

        OpenCGAResult<ClinicalAnalysis> analysisDataResult = getClinicalAnalysisDBAdaptor(organizationId).get(studyUid, queryCopy,
                queryOptions, user);

        if (ignoreException || analysisDataResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, clinicalStringFunction, analysisDataResult, ignoreException, false);
        }
        // Query without adding the user check
        OpenCGAResult<ClinicalAnalysis> resultsNoCheck = getClinicalAnalysisDBAdaptor(organizationId).get(queryCopy, queryOptions);

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

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(sessionId);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        fixQueryObject(organizationId, study, query, userId, sessionId);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getClinicalAnalysisDBAdaptor(organizationId).iterator(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        fixQueryObject(organizationId, study, query, userId, token);
        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getClinicalAnalysisDBAdaptor(organizationId).facet(study.getUid(), query, facet, userId);
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis,
                                                  QueryOptions options, String token) throws CatalogException {
        return create(studyStr, clinicalAnalysis, null, options, token);
    }

    public OpenCGAResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis,
                                                  Boolean skipCreateDefaultInterpretation, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET_AND_CONFIGURATION,
                tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysis", clinicalAnalysis)
                .append("skipCreateDefaultInterpretation", skipCreateDefaultInterpretation)
                .append("options", options)
                .append("token", token);
        try {
            if (study.getInternal() == null || study.getInternal().getConfiguration() == null
                    || study.getInternal().getConfiguration().getClinical() == null) {
                throw new CatalogException("Unexpected error: ClinicalConfiguration is null");
            }
            ClinicalAnalysisStudyConfiguration clinicalConfiguration = study.getInternal().getConfiguration().getClinical();

            authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId,
                    StudyPermissions.Permissions.WRITE_CLINICAL_ANALYSIS);

            options = ParamUtils.defaultObject(options, QueryOptions::new);
            ParamUtils.checkObj(clinicalAnalysis, "clinicalAnalysis");
            ParamUtils.checkIdentifier(clinicalAnalysis.getId(), "id");
            ParamUtils.checkObj(clinicalAnalysis.getType(), "type");
            ParamUtils.checkObj(clinicalAnalysis.getProband(), "proband");

            List<Event> events = new LinkedList<>();

            clinicalAnalysis.setStatus(ParamUtils.defaultObject(clinicalAnalysis.getStatus(), ClinicalStatus::new));
            clinicalAnalysis.setInternal(ClinicalAnalysisInternal.init());
            clinicalAnalysis.setDisorder(ParamUtils.defaultObject(clinicalAnalysis.getDisorder(),
                    new Disorder("", "", "", Collections.emptyMap(), "", Collections.emptyList())));
            clinicalAnalysis.setDueDate(ParamUtils.defaultObject(clinicalAnalysis.getDueDate(),
                    TimeUtils.getTime(TimeUtils.add1MonthtoDate(TimeUtils.getDate()))));
            clinicalAnalysis.setComments(ParamUtils.defaultObject(clinicalAnalysis.getComments(), Collections.emptyList()));
            clinicalAnalysis.setAudit(ParamUtils.defaultObject(clinicalAnalysis.getAudit(), Collections.emptyList()));
            clinicalAnalysis.setQualityControl(ParamUtils.defaultObject(clinicalAnalysis.getQualityControl(),
                    ClinicalAnalysisQualityControl::new));
            clinicalAnalysis.setPanels(ParamUtils.defaultObject(clinicalAnalysis.getPanels(), Collections.emptyList()));
            clinicalAnalysis.setAnnotationSets(ParamUtils.defaultObject(clinicalAnalysis.getAnnotationSets(), Collections.emptyList()));
            clinicalAnalysis.setResponsible(ParamUtils.defaultObject(clinicalAnalysis.getResponsible(), ClinicalResponsible::new));
            clinicalAnalysis.setRequest(ParamUtils.defaultObject(clinicalAnalysis.getRequest(), ClinicalRequest::new));

            // ----------  Check and init report fields
            validateAndInitReport(organizationId, study, clinicalAnalysis.getReport(), userId);

            // ---------- Check and init responsible fields
            ClinicalResponsible responsible = clinicalAnalysis.getResponsible();
            if (StringUtils.isEmpty(responsible.getId())) {
                responsible.setId(userId);
            }
            fillResponsible(organizationId, responsible);

            // ---------- Check and init request fields
            ClinicalRequest request = clinicalAnalysis.getRequest();
            if (StringUtils.isNotEmpty(request.getId())) {
                request.setDate(ParamUtils.checkDateOrGetCurrentDate(request.getDate(), "request.date"));
                fillResponsible(organizationId, request.getResponsible());
            }

            if (clinicalAnalysis.getQualityControl().getComments() != null) {
                for (ClinicalComment comment : clinicalAnalysis.getQualityControl().getComments()) {
                    comment.setDate(TimeUtils.getTime());
                    comment.setAuthor(userId);
                }
            }

            if (!clinicalAnalysis.getComments().isEmpty()) {
                // Fill author and date
                Calendar calendar = Calendar.getInstance();
                for (ClinicalComment comment : clinicalAnalysis.getComments()) {
                    comment.setAuthor(userId);

                    comment.setDate(TimeUtils.getTimeMillis(calendar.getTime()));
                    calendar.add(Calendar.MILLISECOND, 1);
                }
            }

            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getPanels())) {
                // Get panels
                Set<String> panelIds = clinicalAnalysis.getPanels().stream().map(Panel::getId).collect(Collectors.toSet());
                Query query = new Query(PanelDBAdaptor.QueryParams.ID.key(), panelIds);
                OpenCGAResult<org.opencb.opencga.core.models.panel.Panel> panelResult =
                        getPanelDBAdaptor(organizationId).get(study.getUid(), query, PanelManager.INCLUDE_PANEL_IDS, userId);
                if (panelResult.getNumResults() < panelIds.size()) {
                    throw new CatalogException("Some panels were not found or user doesn't have permissions to see them");
                }

                clinicalAnalysis.setPanels(panelResult.getResults());
            }

            // Analyst
            List<User> userList;
            QueryOptions userInclude = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.ID.key(),
                    UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams.EMAIL.key()));
            if (clinicalAnalysis.getAnalysts() == null) {
                userList = getUserDBAdaptor(organizationId).get(userId, userInclude).getResults();
            } else {
                // Validate users
                Set<String> userIds = new HashSet<>();
                for (ClinicalAnalyst analyst : clinicalAnalysis.getAnalysts()) {
                    if (StringUtils.isNotEmpty(analyst.getId())) {
                        userIds.add(analyst.getId());
                    }
                }
                if (CollectionUtils.isNotEmpty(userIds)) {
                    Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), userIds);
                    OpenCGAResult<User> result = getUserDBAdaptor(organizationId).get(query, userInclude);
                    if (result.getNumResults() < userIds.size()) {
                        throw new CatalogException("Some clinical analysts could not be found.");
                    }
                    userList = result.getResults();
                } else {
                    userList = getUserDBAdaptor(organizationId).get(userId, userInclude).getResults();
                }
            }
            List<ClinicalAnalyst> clinicalAnalystList = new ArrayList<>(userList.size());
            for (User user : userList) {
                clinicalAnalystList.add(new ClinicalAnalyst(user.getId(), user.getName(), user.getEmail(), userId, Collections.emptyMap()));
            }
            clinicalAnalysis.setAnalysts(clinicalAnalystList);

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
                        OpenCGAResult<Sample> sampleResult = getSampleDBAdaptor(organizationId).get(study.getUid(), query,
                                new QueryOptions(), userId);
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
                if (proband.getSamples() == null || proband.getSamples().isEmpty()) {
                    throw new CatalogException("Missing samples for proband '" + proband.getId() + "'");
                }

                clinicalAnalysis.setProband(proband);

                // Validate the proband has a disorder
                validateDisorder(clinicalAnalysis);
            } else {
                OpenCGAResult<Individual> individualOpenCGAResult = catalogManager.getIndividualManager()
                        .internalGet(organizationId, study.getUid(), clinicalAnalysis.getProband().getId(), new Query(), new QueryOptions(),
                                userId);
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
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getFiles())) {
                validateFiles(organizationId, study, clinicalAnalysis, userId);
            } else {
                obtainFiles(organizationId, study, clinicalAnalysis, userId);
            }
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getReportedFiles())) {
                validateReportedFiles(organizationId, study, clinicalAnalysis, userId);
            } else {
                clinicalAnalysis.setReportedFiles(Collections.emptyList());
            }

            clinicalAnalysis.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(clinicalAnalysis.getCreationDate(),
                    ClinicalAnalysisDBAdaptor.QueryParams.CREATION_DATE.key()));
            clinicalAnalysis.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(clinicalAnalysis.getModificationDate(),
                    ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
            clinicalAnalysis.setDescription(ParamUtils.defaultString(clinicalAnalysis.getDescription(), ""));
            clinicalAnalysis.setRelease(catalogManager.getStudyManager().getCurrentRelease(study));
            clinicalAnalysis.setAttributes(ParamUtils.defaultObject(clinicalAnalysis.getAttributes(), Collections.emptyMap()));
            clinicalAnalysis.setSecondaryInterpretations(ParamUtils.defaultObject(clinicalAnalysis.getSecondaryInterpretations(),
                    ArrayList::new));
            clinicalAnalysis.setPriority(ParamUtils.defaultObject(clinicalAnalysis.getPriority(), ClinicalPriorityAnnotation::new));
            clinicalAnalysis.setFlags(ParamUtils.defaultObject(clinicalAnalysis.getFlags(), ArrayList::new));
            clinicalAnalysis.setConsent(ParamUtils.defaultObject(clinicalAnalysis.getConsent(), ClinicalConsentAnnotation::new));

            // Validate user-defined parameters
            validateCustomPriorityParameters(clinicalAnalysis, clinicalConfiguration);
            validateCustomFlagParameters(clinicalAnalysis, clinicalConfiguration);
            validateCustomConsentParameters(clinicalAnalysis, clinicalConfiguration);
            validateStatusParameter(clinicalAnalysis, clinicalConfiguration, userId, true);

            if (StringUtils.isNotEmpty(clinicalAnalysis.getStatus().getId())) {
                List<ClinicalStatusValue> clinicalStatusValues = clinicalConfiguration.getStatus();
                for (ClinicalStatusValue clinicalStatusValue : clinicalStatusValues) {
                    if (clinicalAnalysis.getStatus().getId().equals(clinicalStatusValue.getId())) {
                        if (clinicalStatusValue.getType() == ClinicalStatusValue.ClinicalStatusType.CLOSED) {
                            String msg = "Case '" + clinicalAnalysis.getId() + "' created with status '"
                                    + clinicalAnalysis.getStatus().getId() + "', which is of type "
                                    + ClinicalStatusValue.ClinicalStatusType.CLOSED + ". Automatically locking ClinicalAnalysis and"
                                    + " setting CVDB index status to PENDING.";
                            logger.info(msg);
                            clinicalAnalysis.setLocked(true);

                            CvdbIndexStatus cvdbIndexStatus = new CvdbIndexStatus(CvdbIndexStatus.PENDING, "User '" + userId
                                    + "' created case with status '" + clinicalAnalysis.getStatus().getId() + "', which is of type"
                                    + " CLOSED. Automatically setting CVDB index status to " + CvdbIndexStatus.PENDING);
                            clinicalAnalysis.getInternal().setCvdbIndex(cvdbIndexStatus);

                            events.add(new Event(Event.Type.INFO, clinicalAnalysis.getId(), msg));
                        } else if (clinicalStatusValue.getType() == ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE) {
                            String msg = "Case '" + clinicalAnalysis.getId() + "' created with status '"
                                    + clinicalAnalysis.getStatus().getId() + "', which is of type "
                                    + ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE + ". Automatically locking ClinicalAnalysis.";
                            logger.info(msg);
                            clinicalAnalysis.setLocked(true);

                            events.add(new Event(Event.Type.INFO, clinicalAnalysis.getId(), msg));
                        }
                    }
                }
            }

            sortMembersFromFamily(clinicalAnalysis);

            List<ClinicalAudit> clinicalAuditList = new ArrayList<>();

            clinicalAnalysis.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.CLINICAL));
            if (clinicalAnalysis.getInterpretation() == null
                    && (skipCreateDefaultInterpretation == null || !skipCreateDefaultInterpretation)) {
                clinicalAnalysis.setInterpretation(ParamUtils.defaultObject(clinicalAnalysis.getInterpretation(), Interpretation::new));
            }

            if (clinicalAnalysis.getInterpretation() != null) {
                catalogManager.getInterpretationManager().validateNewInterpretation(organizationId, study,
                        clinicalAnalysis.getInterpretation(), clinicalAnalysis, userId);
                clinicalAuditList.add(new ClinicalAudit(userId, ClinicalAudit.Action.CREATE_INTERPRETATION,
                        "Create interpretation '" + clinicalAnalysis.getInterpretation().getId() + "'", TimeUtils.getTime()));
            }

            clinicalAuditList.add(new ClinicalAudit(userId, ClinicalAudit.Action.CREATE_CLINICAL_ANALYSIS,
                    "Create ClinicalAnalysis '" + clinicalAnalysis.getId() + "'", TimeUtils.getTime()));
            OpenCGAResult<ClinicalAnalysis> insert = getClinicalAnalysisDBAdaptor(organizationId).insert(study.getUid(), clinicalAnalysis,
                    study.getVariableSets(), clinicalAuditList, options);
            insert.addEvents(events);

            auditManager.auditCreate(organizationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                    clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated clinical analysis
                OpenCGAResult<ClinicalAnalysis> queryResult = getClinicalAnalysisDBAdaptor(organizationId).get(study.getUid(),
                        clinicalAnalysis.getId(), QueryOptions.empty());
                insert.setResults(queryResult.getResults());
            }

            return insert;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(), "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public ClinicalAnalysisLoadResult load(String studyStr, Path filePath, String token) throws CatalogException, IOException {
        ClinicalAnalysisLoadResult result = new ClinicalAnalysisLoadResult();

        int counter = 0;
        ObjectReader objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(ClinicalAnalysis.class);

        StopWatch stopWatch = StopWatch.createStarted();
        try (BufferedReader br = FileUtils.newBufferedReader(filePath)) {
            while (true) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                ClinicalAnalysis clinicalAnalysis = null;
                try {
                    clinicalAnalysis = objectReader.readValue(line);
                    logger.info("Loading clinical analysis {}...", clinicalAnalysis.getId());
                    load(clinicalAnalysis, studyStr, token);
                    logger.info("... clinical analysis {} loaded !", clinicalAnalysis.getId());
                    counter++;
                } catch (Exception e) {
                    logger.error("Error loading clinical analysis" + (clinicalAnalysis != null ? (": " + clinicalAnalysis.getId()) : "")
                            + ": " + e.getMessage());
                    result.getFailures().put(clinicalAnalysis.getId(), e.getMessage());
                    e.printStackTrace();
                }
            }
        }
        stopWatch.stop();

        result.setNumLoaded(counter)
                .setFilename(filePath.getFileName().toString())
                .setTime((int) stopWatch.getTime(TimeUnit.SECONDS));

        return result;
    }

    private void load(ClinicalAnalysis clinicalAnalysis, String study, String token) throws CatalogException {
        Map<String, List<String>> individualSamples = new HashMap<>();

        // Create samples
        for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
            if (CollectionUtils.isNotEmpty(member.getSamples())) {
                individualSamples.put(member.getId(), member.getSamples().stream().map(Sample::getId).collect(Collectors.toList()));
                for (Sample sample : member.getSamples()) {
                    try {
                        Sample sampleForCreation = SampleCreateParams.of(sample).toSample();
                        sampleForCreation.setIndividualId(null);
                        catalogManager.getSampleManager().create(study, sampleForCreation, QueryOptions.empty(), token);
                    } catch (CatalogException e) {
                        if (!e.getMessage().contains("already exists")) {
                            throw e;
                        }
                    }
                }
            }
        }

        // Create family with individuals
        try {
            Family family = FamilyCreateParams.of(clinicalAnalysis.getFamily()).toFamily();
            catalogManager.getFamilyManager().create(study, family, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            if (!e.getMessage().contains("already exists")) {
                throw e;
            }
        }

        // Associate individuals and samples
        for (Map.Entry<String, List<String>> entry : individualSamples.entrySet()) {
            catalogManager.getIndividualManager().update(study, entry.getKey(), new IndividualUpdateParams().setSamples(
                            entry.getValue().stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList())),
                    QueryOptions.empty(), token);
        }

        for (Panel panel : clinicalAnalysis.getPanels()) {
            try {
                catalogManager.getPanelManager().create(study, panel, QueryOptions.empty(), token);
            } catch (CatalogException e) {
                if (!e.getMessage().contains("already exists")) {
                    throw e;
                }
            }
        }

        // Save primary and secondary interpretations for creating later
        Interpretation primaryInterpretation = clinicalAnalysis.getInterpretation();
        clinicalAnalysis.setInterpretation(null);

        List<Interpretation> secondaryInterpretations = clinicalAnalysis.getSecondaryInterpretations();
        clinicalAnalysis.setSecondaryInterpretations(null);


        // Create clinical analysis
        ClinicalAnalysis caToCreate = ClinicalAnalysisCreateParams.of(clinicalAnalysis).toClinicalAnalysis();
        caToCreate.setAnalysts(Collections.emptyList());
        catalogManager.getClinicalAnalysisManager().create(study, caToCreate, QueryOptions.empty(), token);

        // Create primary interpretation
        if (primaryInterpretation != null) {
            primaryInterpretation.setId(null);
            catalogManager.getInterpretationManager().create(study, clinicalAnalysis.getId(), primaryInterpretation, PRIMARY,
                    QueryOptions.empty(), token);
        }

        // Create secondary interpretations
        for (Interpretation secondaryInterpretation : secondaryInterpretations) {
            secondaryInterpretation.setId(null);
            catalogManager.getInterpretationManager().create(study, clinicalAnalysis.getId(), secondaryInterpretation, SECONDARY,
                    QueryOptions.empty(), token);
        }
    }

    private void validateAndInitReport(String organizationId, Study study, ClinicalReport report, String userId)
            throws CatalogException {
        if (report == null) {
            return;
        }
        if (StringUtils.isNotEmpty(report.getOverview())) {
            report.setDate(ParamUtils.checkDateOrGetCurrentDate(report.getDate(), "report.date"));
        }
        if (report.getComments() != null) {
            for (ClinicalComment comment : report.getComments()) {
                comment.setDate(TimeUtils.getTime());
                comment.setAuthor(userId);
            }
        }
    }

    private void fillResponsible(String organizationId, ClinicalResponsible responsible) throws CatalogException {
        if (responsible == null) {
            return;
        }
        QueryOptions userInclude = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.ID.key(),
                UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams.EMAIL.key()));
        OpenCGAResult<User> result = getUserDBAdaptor(organizationId).get(responsible.getId(), userInclude);
        if (result.getNumResults() == 0) {
            throw new CatalogException("Responsible user '" + responsible.getId() + "' not found.");
        }
        responsible.setName(ParamUtils.defaultString(responsible.getName(), result.first().getName()));
        responsible.setEmail(ParamUtils.defaultString(responsible.getEmail(), result.first().getEmail()));
    }

    private void validateStatusParameter(ClinicalAnalysis clinicalAnalysis, ClinicalAnalysisStudyConfiguration clinicalConfiguration,
                                         String userId, boolean initIfUndefined) throws CatalogException {
        // Status
        if (CollectionUtils.isEmpty(clinicalConfiguration.getStatus())) {
            throw new CatalogException("Missing status configuration in study. Please add a proper set of valid statuses.");
        }
        if (StringUtils.isNotEmpty(clinicalAnalysis.getStatus().getId())) {
            Map<String, ClinicalStatusValue> statusMap = new HashMap<>();
            for (ClinicalStatusValue status : clinicalConfiguration.getStatus()) {
                statusMap.put(status.getId(), status);
            }
            if (!statusMap.containsKey(clinicalAnalysis.getStatus().getId())) {
                throw new CatalogException("Unknown status '" + clinicalAnalysis.getStatus().getId()
                        + "'. The list of valid statuses are: '" + String.join(", ", statusMap.keySet()) + "'");
            }
            ClinicalStatusValue clinicalStatusValue = statusMap.get(clinicalAnalysis.getStatus().getId());
            clinicalAnalysis.getStatus().setDescription(clinicalStatusValue.getDescription());
            clinicalAnalysis.getStatus().setType(clinicalStatusValue.getType());
        } else if (clinicalAnalysis.getStatus().getId() == null) {
            if (initIfUndefined) {
                // Look for first status of type NOT_STARTED
                for (ClinicalStatusValue status : clinicalConfiguration.getStatus()) {
                    if (status.getType() == ClinicalStatusValue.ClinicalStatusType.NOT_STARTED) {
                        clinicalAnalysis.getStatus().setId(status.getId());
                        clinicalAnalysis.getStatus().setDescription(status.getDescription());
                        clinicalAnalysis.getStatus().setType(status.getType());
                        break;
                    }
                }
            } else {
                throw new CatalogException("Missing status id in clinical analysis");
            }
        }
        clinicalAnalysis.getStatus().setDate(TimeUtils.getTime());
        clinicalAnalysis.getStatus().setVersion(GitRepositoryState.getInstance().getBuildVersion());
        clinicalAnalysis.getStatus().setCommit(GitRepositoryState.getInstance().getCommitId());
        clinicalAnalysis.getStatus().setAuthor(userId);
    }

    private void validateCustomConsentParameters(ClinicalAnalysis clinicalAnalysis,
                                                 ClinicalAnalysisStudyConfiguration clinicalConfiguration) throws CatalogException {
        // Consent definition
        if (CollectionUtils.isEmpty(clinicalConfiguration.getConsents())) {
            throw new CatalogException("Missing consents configuration in study. Please add a valid set of consents to the study"
                    + " configuration.");
        }
        Map<String, ClinicalConsent> consentMap = new HashMap<>();
        for (ClinicalConsent consent : clinicalConfiguration.getConsents()) {
            consentMap.put(consent.getId(), consent);
        }
        List<ClinicalConsentParam> consentList = new ArrayList<>(consentMap.size());
        if (clinicalAnalysis.getConsent() != null && CollectionUtils.isNotEmpty(clinicalAnalysis.getConsent().getConsents())) {
            for (ClinicalConsentParam consent : clinicalAnalysis.getConsent().getConsents()) {
                if (consentMap.containsKey(consent.getId())) {
                    consent.setName(consentMap.get(consent.getId()).getName());
                    consent.setDescription(consentMap.get(consent.getId()).getDescription());
                    if (consent.getValue() == null) {
                        consent.setValue(ClinicalConsentParam.Value.UNKNOWN);
                    }
                    consentList.add(consent);

                    // Remove consent id from map
                    consentMap.remove(consent.getId());
                } else {
                    throw new CatalogException("Unknown consent '" + consent.getId() + "'. The available list of consents is: '"
                            + clinicalConfiguration.getConsents()
                            .stream()
                            .map(ClinicalConsent::getId)
                            .collect(Collectors.joining(",")) + "'");
                }
            }

            // Add any consents not defined by the user
            for (ClinicalConsent consent : consentMap.values()) {
                consentList.add(new ClinicalConsentParam(consent.getId(), consent.getName(), consent.getDescription(),
                        ClinicalConsentParam.Value.UNKNOWN));
            }
        } else {
            // Adding all consents to UNKNOWN
            for (ClinicalConsent consent : consentMap.values()) {
                consentList.add(new ClinicalConsentParam(consent.getId(), consent.getName(), consent.getDescription(),
                        ClinicalConsentParam.Value.UNKNOWN));
            }
        }
        clinicalAnalysis.setConsent(new ClinicalConsentAnnotation(consentList, TimeUtils.getTime()));
    }

    private void validateCustomFlagParameters(ClinicalAnalysis clinicalAnalysis, ClinicalAnalysisStudyConfiguration clinicalConfiguration)
            throws CatalogException {
        // Flag definition
        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getFlags())) {
            if (CollectionUtils.isEmpty(clinicalConfiguration.getFlags())) {
                throw new CatalogException("Missing flags configuration. Please add a proper set of valid flags.");
            }
            Map<String, FlagValue> supportedFlags = new HashMap<>();
            for (FlagValue flagValue : clinicalConfiguration.getFlags()) {
                supportedFlags.put(flagValue.getId(), flagValue);
            }

            Map<String, FlagAnnotation> flagMap = new HashMap<>();
            for (FlagAnnotation flag : clinicalAnalysis.getFlags()) {
                if (flagMap.containsKey(flag.getId())) {
                    continue;
                }
                flagMap.put(flag.getId(), flag);
                if (supportedFlags.containsKey(flag.getId())) {
                    flag.setDescription(supportedFlags.get(flag.getId()).getDescription());
                    flag.setDate(TimeUtils.getTime());
                } else {
                    throw new CatalogException("Flag '" + flag.getId() + "' not supported. Supported flags for Clinical Analyses are: '"
                            + String.join(", ", supportedFlags.keySet()) + "'.");
                }
            }
            clinicalAnalysis.setFlags(new ArrayList<>(flagMap.values()));
        }
    }

    private void validateCustomPriorityParameters(ClinicalAnalysis clinicalAnalysis,
                                                  ClinicalAnalysisStudyConfiguration clinicalConfiguration) throws CatalogException {
        // Priority definition
        if (CollectionUtils.isEmpty(clinicalConfiguration.getPriorities())) {
            throw new CatalogException("Missing priority configuration in study. Please add a proper set of valid priorities.");
        }
        ClinicalPriorityValue priority = null;
        if (StringUtils.isNotEmpty(clinicalAnalysis.getPriority().getId())) {
            // Look for the priority
            for (ClinicalPriorityValue tmpPriority : clinicalConfiguration.getPriorities()) {
                if (tmpPriority.getId().equals(clinicalAnalysis.getPriority().getId())) {
                    priority = tmpPriority;
                    break;
                }
            }
            if (priority == null) {
                throw new CatalogException("Cannot set priority '" + clinicalAnalysis.getPriority().getId() + "'. The priority is "
                        + "not one of the supported priorities. Supported priority ids are: '" + clinicalConfiguration.getPriorities()
                        .stream()
                        .map(ClinicalPriorityValue::getId)
                        .collect(Collectors.joining(", ")) + "'.");
            }
        } else {
            for (ClinicalPriorityValue tmpPriority : clinicalConfiguration.getPriorities()) {
                if (tmpPriority.isDefaultPriority()) {
                    priority = tmpPriority;
                    break;
                }
            }
            // If none of the priorities can work as the default, we choose the first one
            if (priority == null) {
                priority = clinicalConfiguration.getPriorities().get(0);
            }
        }
        clinicalAnalysis.setPriority(new ClinicalPriorityAnnotation(priority.getId(), priority.getDescription(), priority.getRank(),
                TimeUtils.getTime()));
    }

    private void validateDisorder(ClinicalAnalysis clinicalAnalysis) throws CatalogException {
        if (clinicalAnalysis.getDisorder() != null && StringUtils.isNotEmpty(clinicalAnalysis.getDisorder().getId())) {
            if (clinicalAnalysis.getProband() == null) {
                throw new CatalogException("Missing proband");
            }
            if (clinicalAnalysis.getProband().getDisorders() == null || clinicalAnalysis.getProband().getDisorders().isEmpty()) {
                throw new CatalogException("Missing list of proband disorders");
            }

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

    private void obtainFiles(String organizationId, Study study, ClinicalAnalysis clinicalAnalysis, String userId) throws CatalogException {
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

        if (!sampleSet.isEmpty()) {
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), new ArrayList<>(sampleSet))
                    .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), Arrays.asList(File.Bioformat.ALIGNMENT, File.Bioformat.VARIANT));
            OpenCGAResult<File> fileResults = getFileDBAdaptor(organizationId).get(study.getUid(), query, FileManager.INCLUDE_FILE_URI_PATH,
                    userId);
            clinicalAnalysis.setFiles(fileResults.getResults());
//            return fileResults.getResults();
        }
//        return Collections.emptyList();
    }

    private List<File> obtainFiles(String organizationId, Study study, String userId, List<File> files) throws CatalogException {
        Query query = new Query(FileDBAdaptor.QueryParams.ID.key(), files.stream().map(File::getId).collect(Collectors.toSet()));
        List<File> results = getFileDBAdaptor(organizationId).get(study.getUid(), query, FileManager.INCLUDE_FILE_URI_PATH, userId)
                .getResults();
        if (results.size() < files.size()) {
            throw new CatalogException("Some of the files were not found");
        }
        return results;
    }

    private void validateFiles(String organizationId, Study study, ClinicalAnalysis clinicalAnalysis, String userId)
            throws CatalogException {
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
        QueryOptions fileOptions = keepFieldInQueryOptions(FileManager.INCLUDE_FILE_URI_PATH, FileDBAdaptor.QueryParams.SAMPLE_IDS.key());
        OpenCGAResult<File> fileResults = getFileDBAdaptor(organizationId).get(study.getUid(), query, fileOptions, userId);

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
    }

    private void validateReportedFiles(String organizationId, Study study, ClinicalAnalysis clinicalAnalysis, String userId)
            throws CatalogException {
        if (CollectionUtils.isEmpty(clinicalAnalysis.getReportedFiles())) {
            throw new CatalogException("Found empty list of reported files");
        }

        // Check all the files exist
        Query query = new Query(FileDBAdaptor.QueryParams.ID.key(),
                clinicalAnalysis.getReportedFiles().stream().map(File::getId).collect(Collectors.toList()));
        OpenCGAResult<File> fileResults = getFileDBAdaptor(organizationId).get(study.getUid(), query, FileManager.INCLUDE_FILE_URI_PATH,
                userId);

        if (fileResults.getNumResults() != clinicalAnalysis.getReportedFiles().size()) {
            Set<String> fileIds = clinicalAnalysis.getReportedFiles().stream().map(File::getId).collect(Collectors.toSet());
            String notFoundFiles = fileResults.getResults().stream().map(File::getId).filter(f -> !fileIds.contains(f))
                    .collect(Collectors.joining(", "));
            throw new CatalogException("Reported files '" + notFoundFiles + "' not found");
        }

        // Complete file information
        clinicalAnalysis.setReportedFiles(fileResults.getResults());
    }

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, Query query, ClinicalAnalysisUpdateParams updateParams,
                                                  QueryOptions options, String token) throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, Query query, ClinicalAnalysisUpdateParams updateParams,
                                                  boolean ignoreException, QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET_AND_CONFIGURATION, tokenPayload);

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
            fixQueryObject(organizationId, study, query, userId, token);
            AnnotationUtils.fixQueryOptionAnnotation(options);
            query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            iterator = getClinicalAnalysisDBAdaptor(organizationId).iterator(study.getUid(), query, new QueryOptions(), userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<ClinicalAnalysis> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = iterator.next();
            try {
                OpenCGAResult<ClinicalAnalysis> queryResult = update(organizationId, study, clinicalAnalysis, updateParams, userId,
                        options);
                result.append(queryResult);

                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, clinicalAnalysis.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Could not update clinical analysis {}: {}", clinicalAnalysis.getId(), e.getMessage());
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, String clinicalId, ClinicalAnalysisUpdateParams updateParams,
                                                  QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET_AND_CONFIGURATION, tokenPayload);

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
            OpenCGAResult<ClinicalAnalysis> internalResult = internalGet(organizationId, study.getUid(), clinicalId, QueryOptions.empty(),
                    userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Clinical analysis '" + clinicalId + "' not found");
            }
            ClinicalAnalysis clinicalAnalysis = internalResult.first();

            // We set the proper values for the audit
            clinicalId = clinicalAnalysis.getId();
            clinicalUuid = clinicalAnalysis.getUuid();

            OpenCGAResult<ClinicalAnalysis> updateResult = update(organizationId, study, clinicalAnalysis, updateParams, userId, options);
            result.append(updateResult);

            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                    clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, clinicalId, e.getMessage());
            result.getEvents().add(event);
            result.setNumErrors(result.getNumErrors() + 1);

            logger.error("Could not update clinical analysis {}: {}", clinicalId, e.getMessage());
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalId, clinicalUuid,
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    /**
     * Update a Clinical Analysis from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of
     *                     [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param clinicalIds  List of clinical analysis ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token        Session id of the user logged in.
     * @return A OpenCGAResult.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, List<String> clinicalIds, ClinicalAnalysisUpdateParams updateParams,
                                                  QueryOptions options, String token) throws CatalogException {
        return update(studyStr, clinicalIds, updateParams, false, options, token);
    }

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, List<String> clinicalIds, ClinicalAnalysisUpdateParams updateParams,
                                                  boolean ignoreException, QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET_AND_CONFIGURATION, tokenPayload);

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
                OpenCGAResult<ClinicalAnalysis> internalResult = internalGet(organizationId, study.getUid(), id, QueryOptions.empty(),
                        userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Clinical analysis '" + id + "' not found");
                }
                ClinicalAnalysis clinicalAnalysis = internalResult.first();

                // We set the proper values for the audit
                clinicalAnalysisId = clinicalAnalysis.getId();
                clinicalAnalysisUuid = clinicalAnalysis.getUuid();

                OpenCGAResult<ClinicalAnalysis> updateResult = update(organizationId, study, clinicalAnalysis, updateParams, userId,
                        options);
                result.append(updateResult);

                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Could not update clinical analysis {}: {}", clinicalAnalysisId, e.getMessage());
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysisId,
                        clinicalAnalysisUuid, study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult<ClinicalAnalysis> update(String organizationId, Study study, ClinicalAnalysis clinicalAnalysis,
                                                   ClinicalAnalysisUpdateParams updateParams, String userId, QueryOptions options)
            throws CatalogException {
        ClinicalAnalysisUpdateParams updateParamsClone;
        try {
            updateParamsClone = JacksonUtils.copy(updateParams, ClinicalAnalysisUpdateParams.class);
        } catch (IOException e) {
            throw new CatalogException("Could not clone ClinicalAnalysisUpdateParams object");
        }
        if (updateParamsClone == null) {
            throw new CatalogException("Empty update parameters. Nothing to update.");
        }

        validateAndInitReport(organizationId, study, updateParamsClone.getReport(), userId);

        // ---------- Check and init responsible fields
        ClinicalResponsible responsible = updateParamsClone.getResponsible();
        if (responsible != null && StringUtils.isNotEmpty(responsible.getId())) {
            fillResponsible(organizationId, responsible);
        }

        // ---------- Check and init request fields
        ClinicalRequest request = updateParamsClone.getRequest();
        if (request != null && StringUtils.isNotEmpty(request.getId())) {
            request.setDate(ParamUtils.checkDateOrGetCurrentDate(request.getDate(), "request.date"));
            fillResponsible(organizationId, request.getResponsible());
        }

        ObjectMap parameters;
        try {
            parameters = updateParamsClone.getUpdateMap();
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse ClinicalUpdateParams object: " + e.getMessage(), e);
        }
        ParamUtils.checkUpdateParametersMap(parameters);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (study.getInternal() == null || study.getInternal().getConfiguration() == null
                || study.getInternal().getConfiguration().getClinical() == null) {
            throw new CatalogException("Unexpected error: ClinicalConfiguration is null");
        }
        ClinicalAnalysisStudyConfiguration clinicalConfiguration = study.getInternal().getConfiguration().getClinical();

        // Get the clinical status that are CLOSED, INCONCLUSIVE and DONE
        Set<String> closedStatus = new HashSet<>();
        Set<String> inconclusiveStatus = new HashSet<>();
        Set<String> doneStatus = new HashSet<>();
        for (ClinicalStatusValue clinicalStatusValue : clinicalConfiguration.getStatus()) {
            if (clinicalStatusValue.getType().equals(ClinicalStatusValue.ClinicalStatusType.CLOSED)) {
                closedStatus.add(clinicalStatusValue.getId());
            } else if (clinicalStatusValue.getType().equals(ClinicalStatusValue.ClinicalStatusType.DONE)) {
                doneStatus.add(clinicalStatusValue.getId());
            } else if (clinicalStatusValue.getType().equals(ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE)) {
                inconclusiveStatus.add(clinicalStatusValue.getId());
            }
        }

        // If the current clinical analysis:
        // - is locked or panelLocked
        // - the user wants to update the locked or panelLocked status
        // - the user wants to update the status to/from a done|closed status
        boolean adminPermissionsChecked = false;
        if (clinicalAnalysis.isLocked() || clinicalAnalysis.isPanelLocked()
                || clinicalAnalysis.getStatus().getType() == ClinicalStatusValue.ClinicalStatusType.CLOSED
                || clinicalAnalysis.getStatus().getType() == ClinicalStatusValue.ClinicalStatusType.DONE
                || clinicalAnalysis.getStatus().getType() == ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE
                || updateParamsClone.getLocked() != null
                || updateParams.getPanelLocked() != null
                || (updateParams.getStatus() != null && (closedStatus.contains(updateParams.getStatus().getId())
                || doneStatus.contains(updateParams.getStatus().getId())))) {
            authorizationManager.checkClinicalAnalysisPermission(organizationId, study.getUid(), clinicalAnalysis.getUid(), userId,
                    ClinicalAnalysisPermissions.ADMIN);

            // Current status is of type CLOSED OR INCONCLUSIVE
            if (clinicalAnalysis.getStatus().getType() == ClinicalStatusValue.ClinicalStatusType.CLOSED
                    || clinicalAnalysis.getStatus().getType() == ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE) {
                // The only allowed action is to remove the INCONCLUSIVE or CLOSED status
                if (updateParams.getStatus() == null || StringUtils.isEmpty(updateParams.getStatus().getId())) {
                    throw new CatalogException("Cannot update a ClinicalAnalysis with a " + clinicalAnalysis.getStatus().getType()
                            + " status. You need to remove the " + clinicalAnalysis.getStatus().getType() + " status to be able "
                            + "to perform further updates on the ClinicalAnalysis.");
                } else if ((clinicalAnalysis.getStatus().getType() == ClinicalStatusValue.ClinicalStatusType.CLOSED
                        && closedStatus.contains(updateParams.getStatus().getId()))
                        || (clinicalAnalysis.getStatus().getType() == ClinicalStatusValue.ClinicalStatusType.INCONCLUSIVE
                        && inconclusiveStatus.contains(updateParams.getStatus().getId()))) {
                    // Users should be able to change from one CLOSED|INCONCLUSIVE status to a different one but we should still control
                    // that no further modifications are made
                    if (parameters.size() > 1) {
                        throw new CatalogException("Cannot update a ClinicalAnalysis with a "
                                + clinicalAnalysis.getStatus().getType() + " status. You need to remove the "
                                + clinicalAnalysis.getStatus().getType() + " status to be able to perform further updates on "
                                + "the ClinicalAnalysis.");
                    } else if (clinicalAnalysis.getStatus().getId().equals(updateParams.getStatus().getId())) {
                        throw new CatalogException("ClinicalAnalysis already have the status '" + clinicalAnalysis.getStatus().getId()
                                + "' of type " + clinicalAnalysis.getStatus().getType());
                    }
                } else if (clinicalAnalysis.getStatus().getType() == ClinicalStatusValue.ClinicalStatusType.CLOSED) {
                    // The user wants to remove the CLOSED status so we set automatically CVDB index status to PENDING_REMOVE
                    CvdbIndexStatus cvdbIndexStatus = new CvdbIndexStatus(CvdbIndexStatus.PENDING_REMOVE, "User '" + userId
                            + "' requested to remove the status '" + clinicalAnalysis.getStatus().getId() + "' of type "
                            + ClinicalStatusValue.ClinicalStatusType.CLOSED + " to set it to '" + updateParams.getStatus().getId() + "'");
                    parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_CVDB_INDEX.key(), cvdbIndexStatus);
                }
            }

            adminPermissionsChecked = true;
        }
        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (!adminPermissionsChecked && updateParamsClone.getAnnotationSets() != null) {
            authorizationManager.checkClinicalAnalysisPermission(organizationId, study.getUid(), clinicalAnalysis.getUid(), userId,
                    ClinicalAnalysisPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if (!adminPermissionsChecked && ((parameters.size() == 1
                && !parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key())) || parameters.size() > 1)) {
            authorizationManager.checkClinicalAnalysisPermission(organizationId, study.getUid(), clinicalAnalysis.getUid(), userId,
                    ClinicalAnalysisPermissions.WRITE);
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
            if (!actionMap.containsKey(AnnotationSetManager.ANNOTATION_SETS)
                    && !actionMap.containsKey(AnnotationSetManager.ANNOTATIONS)) {
                logger.warn("Assuming the user wants to add the list of annotation sets provided");
                actionMap.put(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.BasicUpdateAction.ADD);
                options.put(Constants.ACTIONS, actionMap);
            }
        }

        List<Event> events = new LinkedList<>();
        if (StringUtils.isNotEmpty(clinicalAnalysis.getCreationDate())) {
            ParamUtils.checkDateFormat(clinicalAnalysis.getCreationDate(), ClinicalAnalysisDBAdaptor.QueryParams.CREATION_DATE.key());
        }
        if (StringUtils.isNotEmpty(clinicalAnalysis.getModificationDate())) {
            ParamUtils.checkDateFormat(clinicalAnalysis.getModificationDate(),
                    ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE.key());
        }

        Map<String, Object> actionMap = options.getMap(Constants.ACTIONS);

        if (updateParamsClone.getId() != null) {
            ParamUtils.checkIdentifier(updateParamsClone.getId(), ClinicalAnalysisDBAdaptor.QueryParams.ID.key());
        }
        if (StringUtils.isNotEmpty(updateParamsClone.getDueDate()) && TimeUtils.toDate(updateParamsClone.getDueDate()) == null) {
            throw new CatalogException("Unrecognised due date. Accepted format is: yyyyMMddHHmmss");
        }

        if (updateParamsClone.getComments() != null && !updateParamsClone.getComments().isEmpty()) {
            List<ClinicalComment> comments = new ArrayList<>(updateParamsClone.getComments().size());

            ParamUtils.AddRemoveReplaceAction action = ParamUtils.AddRemoveReplaceAction.from(actionMap,
                    ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveReplaceAction.ADD);

            switch (action) {
                case ADD:
                    // Ensure each comment has a different milisecond
                    Calendar calendar = Calendar.getInstance();
                    for (ClinicalCommentParam comment : updateParamsClone.getComments()) {
                        comments.add(new ClinicalComment(userId, comment.getMessage(), comment.getTags(),
                                TimeUtils.getTimeMillis(calendar.getTime())));
                        calendar.add(Calendar.MILLISECOND, 1);
                    }
                    break;
                case REMOVE:
                case REPLACE:
                    for (ClinicalCommentParam comment : updateParamsClone.getComments()) {
                        if (StringUtils.isEmpty(comment.getDate())) {
                            throw new CatalogException("Missing mandatory 'date' field. This field is mandatory when action is '"
                                    + action + "'.");
                        }
                        comments.add(new ClinicalComment(userId, comment.getMessage(), comment.getTags(), comment.getDate()));
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown comments action " + action);
            }

            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), comments);
        }

        if (parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.ANALYSTS.key()) != null) {
            ParamUtils.BasicUpdateAction action = ParamUtils.BasicUpdateAction.from(actionMap,
                    ClinicalAnalysisDBAdaptor.QueryParams.ANALYSTS.key(), ParamUtils.BasicUpdateAction.ADD);
            List<ClinicalAnalystParam> analystList = updateParamsClone.getAnalysts();
            switch (action) {
                case ADD:
                case SET:
                    QueryOptions userOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.ID.key(),
                            UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams.EMAIL.key()));

                    Set<String> analystIdList = new HashSet<>();
                    for (ClinicalAnalystParam clinicalAnalystParam : analystList) {
                        analystIdList.add(clinicalAnalystParam.getId());
                    }

                    List<ClinicalAnalyst> clinicalAnalystList = new ArrayList<>(analystIdList.size());
                    // Check analysts exist
                    if (!analystIdList.isEmpty()) {
                        Query query = new Query(UserDBAdaptor.QueryParams.ID.key(), analystIdList);
                        OpenCGAResult<User> userResult = getUserDBAdaptor(organizationId).get(query, userOptions);
                        if (userResult.getNumResults() < analystIdList.size()) {
                            throw new CatalogException("Some analysts were not found.");
                        }
                        for (User user : userResult.getResults()) {
                            clinicalAnalystList.add(new ClinicalAnalyst(user.getId(), user.getName(), user.getEmail(), userId,
                                    Collections.emptyMap()));
                        }
                    }
                    parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.ANALYSTS.key(), clinicalAnalystList);
                    break;
                case REMOVE:
                    // Directly add those analysts. No need to check
                    List<ClinicalAnalyst> analysts = analystList.stream().map(ClinicalAnalystParam::toClinicalAnalyst)
                            .collect(Collectors.toList());
                    parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.ANALYSTS.key(), analysts);
                    break;
                default:
                    throw new IllegalStateException("Unknown analysts action " + action);
            }
        }
        if (parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL.key()) != null) {
            ClinicalAnalysisQualityControl qualityControl = updateParamsClone.getQualityControl().toClinicalQualityControl();
            if (qualityControl.getComments() != null) {
                for (ClinicalComment comment : qualityControl.getComments()) {
                    comment.setDate(TimeUtils.getTime());
                    comment.setAuthor(userId);
                }
            }
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL.key(), qualityControl);
        }

        if (CollectionUtils.isNotEmpty(updateParamsClone.getFiles())) {
            clinicalAnalysis.setFiles(updateParamsClone.getFiles()
                    .stream()
                    .map(FileReferenceParam::toFile)
                    .collect(Collectors.toList()));

            // Validate files
            validateFiles(organizationId, study, clinicalAnalysis, userId);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), clinicalAnalysis.getFiles());
        }
        if (CollectionUtils.isNotEmpty(updateParamsClone.getReportedFiles())) {
            clinicalAnalysis.setReportedFiles(updateParamsClone.getReportedFiles()
                    .stream()
                    .map(FileReferenceParam::toFile)
                    .collect(Collectors.toList()));

            // Validate reported files
            validateReportedFiles(organizationId, study, clinicalAnalysis, userId);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.REPORTED_FILES.key(), clinicalAnalysis.getReportedFiles());
        }

        if (CollectionUtils.isNotEmpty(updateParamsClone.getPanels()) && updateParamsClone.getPanelLocked() != null
                && updateParamsClone.getPanelLocked()) {
            throw new CatalogException("Updating the list of panels and setting '"
                    + ClinicalAnalysisDBAdaptor.QueryParams.PANEL_LOCKED.key() + "' to true at the same time is not allowed.");
        }

        if (CollectionUtils.isNotEmpty(updateParamsClone.getPanels())) {
            if (clinicalAnalysis.isPanelLocked() && (updateParamsClone.getPanelLocked() == null || updateParamsClone.getPanelLocked())) {
                throw new CatalogException("Cannot update panels from ClinicalAnalysis '" + clinicalAnalysis.getId() + "'. "
                        + "'panelLocked' field from ClinicalAnalysis is set to true.");
            }

            // Validate and get panels
            List<String> panelIds = updateParamsClone.getPanels().stream().map(PanelReferenceParam::getId).collect(Collectors.toList());
            Query query = new Query(PanelDBAdaptor.QueryParams.ID.key(), panelIds);
            OpenCGAResult<Panel> panelResult =
                    getPanelDBAdaptor(organizationId).get(study.getUid(), query, PanelManager.INCLUDE_PANEL_IDS, userId);
            if (panelResult.getNumResults() < panelIds.size()) {
                throw new CatalogException("Some panels were not found or user doesn't have permissions to see them.");
            }

            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), panelResult.getResults());
        }

        if (updateParamsClone.getPanelLocked() != null && updateParamsClone.getPanelLocked() && !clinicalAnalysis.isPanelLocked()) {
            // if user wants to set panelLock to true
            // We need to check if the CA has interpretations. If so, the interpretations should contain at least one of the case panels
            // in order to set panelLock to true. Otherwise, that action is not allowed.
            Set<String> panelIds = clinicalAnalysis.getPanels().stream().map(Panel::getId).collect(Collectors.toSet());
            String exceptionMsgPrefix = "The interpretation '";
            String exceptionMsgSuffix = "' does not contain any of the case panels. '"
                    + ClinicalAnalysisDBAdaptor.QueryParams.PANEL_LOCKED.key() + "' can only be set to true if all"
                    + " all Interpretations contains a non-empty subset of the panels used by the case.";
            String alternativeExceptionMsgSuffix = "' is using a panel not defined by the case. '"
                    + ClinicalAnalysisDBAdaptor.QueryParams.PANEL_LOCKED.key() + "' can only be set to true if all"
                    + " all Interpretations contains a non-empty subset of the panels used by the case.";
            if (clinicalAnalysis.getInterpretation() != null) {
                if (CollectionUtils.isEmpty(clinicalAnalysis.getInterpretation().getPanels())) {
                    throw new CatalogException(exceptionMsgPrefix + clinicalAnalysis.getInterpretation().getId() + exceptionMsgSuffix);
                }
                for (Panel panel : clinicalAnalysis.getInterpretation().getPanels()) {
                    if (!panelIds.contains(panel.getId())) {
                        throw new CatalogException(exceptionMsgPrefix + clinicalAnalysis.getInterpretation().getId()
                                + alternativeExceptionMsgSuffix);
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                for (Interpretation interpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                    if (CollectionUtils.isEmpty(interpretation.getPanels())) {
                        throw new CatalogException(exceptionMsgPrefix + interpretation.getId() + exceptionMsgSuffix);
                    }
                    for (Panel panel : interpretation.getPanels()) {
                        if (!panelIds.contains(panel.getId())) {
                            throw new CatalogException(exceptionMsgPrefix + interpretation.getId() + alternativeExceptionMsgSuffix);
                        }
                    }
                }
            }
        }

        if (CollectionUtils.isNotEmpty(updateParamsClone.getPanels())) {
            // Get panels
            Query query = new Query(PanelDBAdaptor.QueryParams.ID.key(),
                    updateParamsClone.getPanels().stream().map(PanelReferenceParam::getId).collect(Collectors.toList()));
            OpenCGAResult<Panel> panelResult =
                    getPanelDBAdaptor(organizationId).get(study.getUid(), query, PanelManager.INCLUDE_PANEL_IDS, userId);
            if (panelResult.getNumResults() < updateParamsClone.getPanels().size()) {
                throw new CatalogException("Some panels were not found or user doesn't have permissions to see them");
            }

            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), panelResult.getResults());
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key())) {
            // Assign the disorder to be updated to the clinicalAnalysis obtained from the DB so it can be checked in context
            clinicalAnalysis.setDisorder(updateParamsClone.getDisorder().toDisorder());
            validateDisorder(clinicalAnalysis);
            // Fill parameter to be updated with complete disorder information
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key(), clinicalAnalysis.getDisorder());
        }

        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_STATUS.key())) {
            Map<String, Object> status = (Map<String, Object>) parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_STATUS.key());
            if (!(status instanceof Map) || StringUtils.isEmpty(String.valueOf(status.get("id")))
                    || !InternalStatus.isValid(String.valueOf(status.get("id")))) {
                throw new CatalogException("Missing or invalid status");
            }
        }

        // Validate user-defined parameters
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.PRIORITY.key())) {
            clinicalAnalysis.setPriority(updateParamsClone.getPriority().toClinicalPriorityAnnotation());
            validateCustomPriorityParameters(clinicalAnalysis, clinicalConfiguration);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.PRIORITY.key(), clinicalAnalysis.getPriority());
        }
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key())) {
            Set<String> currentFlags = null;
            if (clinicalAnalysis.getFlags() != null) {
                currentFlags = clinicalAnalysis.getFlags().stream().map(FlagAnnotation::getId).collect(Collectors.toSet());
            }

            clinicalAnalysis.setFlags(updateParamsClone.getFlags().stream().map(FlagValueParam::toFlagAnnotation)
                    .collect(Collectors.toList()));
            validateCustomFlagParameters(clinicalAnalysis, clinicalConfiguration);

            ParamUtils.BasicUpdateAction action = ParamUtils.BasicUpdateAction.from(actionMap,
                    ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(), ParamUtils.BasicUpdateAction.ADD);
            if (action == ParamUtils.BasicUpdateAction.ADD) {
                // Check for duplications
                if (currentFlags != null) {
                    Iterator<FlagAnnotation> iterator = clinicalAnalysis.getFlags().iterator();
                    while (iterator.hasNext()) {
                        FlagAnnotation flag = iterator.next();
                        if (currentFlags.contains(flag.getId())) {
                            iterator.remove();
                        }
                    }
                }
            }

            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(), clinicalAnalysis.getFlags());
        }
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.CONSENT.key())) {
            clinicalAnalysis.setConsent(updateParamsClone.getConsent().toClinicalConsentAnnotation());
            validateCustomConsentParameters(clinicalAnalysis, clinicalConfiguration);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.CONSENT.key(), clinicalAnalysis.getConsent());
        }
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key())) {
            clinicalAnalysis.setStatus(updateParamsClone.getStatus().toClinicalStatus());
            validateStatusParameter(clinicalAnalysis, clinicalConfiguration, userId, false);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key(), clinicalAnalysis.getStatus());

            if (StringUtils.isNotEmpty(updateParamsClone.getStatus().getId())) {
                List<ClinicalStatusValue> clinicalStatusValues = clinicalConfiguration.getStatus();
                for (ClinicalStatusValue clinicalStatusValue : clinicalStatusValues) {
                    if (updateParamsClone.getStatus().getId().equals(clinicalStatusValue.getId())) {
                        if (clinicalStatusValue.getType() == ClinicalStatusValue.ClinicalStatusType.CLOSED) {
                            String msg = "User '" + userId + "' changed case '" + clinicalAnalysis.getId() + "' to status '"
                                    + updateParamsClone.getStatus().getId() + "', which is of type CLOSED. Automatically locking "
                                    + "ClinicalAnalysis and changing CVDB index status to be indexed";
                            logger.info(msg);
                            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.LOCKED.key(), true);
                            events.add(new Event(Event.Type.INFO, clinicalAnalysis.getId(), msg));

                            if (StringUtils.isEmpty(clinicalAnalysis.getInternal().getCvdbIndex().getId())
                                    || clinicalAnalysis.getInternal().getCvdbIndex().getId().equals(CvdbIndexStatus.NONE)) {
                                CvdbIndexStatus cvdbIndexStatus = new CvdbIndexStatus(CvdbIndexStatus.PENDING, "User '" + userId
                                        + "' changed case to status '" + updateParamsClone.getStatus().getId() + "', which is of type"
                                        + " CLOSED. Automatically changing CVDB index status to " + CvdbIndexStatus.PENDING);
                                parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_CVDB_INDEX.key(), cvdbIndexStatus);
                            } else if (clinicalAnalysis.getInternal().getCvdbIndex().getId().equals(CvdbIndexStatus.PENDING_REMOVE)) {
                                CvdbIndexStatus cvdbIndexStatus = new CvdbIndexStatus(CvdbIndexStatus.PENDING_OVERWRITE, "User '" + userId
                                        + "' changed case to status '" + updateParamsClone.getStatus().getId() + "', which is of type"
                                        + " CLOSED. CVDB index was already in " + CvdbIndexStatus.PENDING_REMOVE + ", so automatically"
                                        + " changing CVDB index status to " + CvdbIndexStatus.PENDING_OVERWRITE);
                                parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_CVDB_INDEX.key(), cvdbIndexStatus);
                            } else {
                                logger.warn("CVDB index status is unexpectedly set to '{}'. Although the user is closing the case, OpenCGA"
                                                + " cannot automatically infer which should be the new CVDB index status.",
                                        clinicalAnalysis.getInternal().getCvdbIndex().getId());
                            }
                        }
                    }
                }
            }
        }

        checkUpdateAnnotations(organizationId, study, clinicalAnalysis, parameters, options,
                VariableSet.AnnotableDataModels.CLINICAL_ANALYSIS, getClinicalAnalysisDBAdaptor(organizationId), userId);
        ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.UPDATE_CLINICAL_ANALYSIS,
                "Update ClinicalAnalysis '" + clinicalAnalysis.getId() + "'", TimeUtils.getTime());
        OpenCGAResult<ClinicalAnalysis> update = getClinicalAnalysisDBAdaptor(organizationId).update(clinicalAnalysis.getUid(), parameters,
                study.getVariableSets(), Collections.singletonList(clinicalAudit), options);
        update.addEvents(events);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated clinical analysis
            OpenCGAResult<ClinicalAnalysis> result = getClinicalAnalysisDBAdaptor(organizationId).get(study.getUid(),
                    new Query(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), clinicalAnalysis.getUid()), options, userId);
            update.setResults(result.getResults());
        }
        return update;
    }

    public OpenCGAResult<ClinicalReport> updateReport(String studyStr, String clinicalAnalysisId, ClinicalReport report,
                                                      QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysisId", clinicalAnalysisId)
                .append("report", report)
                .append("options", options)
                .append("token", token);

        String caseId = clinicalAnalysisId;
        String caseUuid = "";
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        String studyId = studyFqn.getStudyId();
        String studyUuid = studyFqn.getStudyUuid();
        try {
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);
            options = ParamUtils.defaultObject(options, QueryOptions::new);
            ClinicalAnalysis clinicalAnalysis = internalGet(organizationId, study.getUid(), clinicalAnalysisId, INCLUDE_CLINICAL_IDS,
                    userId).first();
            authorizationManager.checkClinicalAnalysisPermission(organizationId, study.getUid(), clinicalAnalysis.getUid(), userId,
                    ClinicalAnalysisPermissions.WRITE);
            caseId = clinicalAnalysis.getId();
            caseUuid = clinicalAnalysis.getUuid();

            ObjectMap updateMap;
            try {
                updateMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(report));
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse report object: " + e.getMessage(), e);
            }

            Map<String, Object> actionMap = options.getMap(ParamConstants.ACTION, new HashMap<>());
            if (report.getComments() != null) {
                ParamUtils.AddRemoveReplaceAction basicOperation = ParamUtils.AddRemoveReplaceAction
                        .from(actionMap, ClinicalAnalysisDBAdaptor.ReportQueryParams.COMMENTS.key(), ParamUtils.AddRemoveReplaceAction.ADD);
                if (basicOperation != ParamUtils.AddRemoveReplaceAction.ADD) {
                    for (ClinicalComment comment : report.getComments()) {
                        comment.setDate(TimeUtils.getTime());
                        comment.setAuthor(userId);
                    }
                }
                updateMap.put(ClinicalAnalysisDBAdaptor.ReportQueryParams.COMMENTS.key(), report.getComments());
            }
            if (CollectionUtils.isNotEmpty(report.getSignatures())) {
                ParamUtils.AddRemoveReplaceAction basicOperation = ParamUtils.AddRemoveReplaceAction
                        .from(actionMap, ClinicalAnalysisDBAdaptor.ReportQueryParams.SIGNATURES.key(),
                                ParamUtils.AddRemoveReplaceAction.ADD);
                if (basicOperation != ParamUtils.AddRemoveReplaceAction.ADD) {
                    for (Signature signature : report.getSignatures()) {
                        signature.setDate(TimeUtils.getTime());
                        signature.setSignedBy(userId);
                    }
                }
                updateMap.put(ClinicalAnalysisDBAdaptor.ReportQueryParams.SIGNATURES.key(), report.getComments());
            }
            ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.UPDATE_CLINICAL_ANALYSIS,
                    "Update ClinicalAnalysis '" + clinicalAnalysis.getId() + "' report.", TimeUtils.getTime());

            // Add custom key to ensure it is properly updated
            updateMap = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.REPORT_UPDATE.key(), updateMap);
            OpenCGAResult<ClinicalAnalysis> update = getClinicalAnalysisDBAdaptor(organizationId)
                    .update(clinicalAnalysis.getUid(), updateMap, null, Collections.singletonList(clinicalAudit), options);
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.CLINICAL_ANALYSIS, caseId, caseUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated clinical analysis
                OpenCGAResult<ClinicalAnalysis> result = getClinicalAnalysisDBAdaptor(organizationId).get(study.getUid(),
                        new Query(ClinicalAnalysisDBAdaptor.QueryParams.UID.key(), clinicalAnalysis.getUid()), options, userId);
                update.setResults(result.getResults());
            }
            List<ClinicalReport> reportList = new ArrayList<>(update.getResults().size());
            if (update.getNumResults() > 0) {
                for (ClinicalAnalysis result : update.getResults()) {
                    reportList.add(result.getReport());
                }
            }
            return new OpenCGAResult<>(update.getTime(), update.getEvents(), update.getNumResults(), reportList,
                    update.getNumMatches(), update.getNumInserted(), update.getNumUpdated(), update.getNumDeleted(),
                    update.getNumErrors(), update.getAttributes(), update.getFederationNode());
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.CLINICAL_ANALYSIS, caseId, caseUuid, studyId, studyUuid,
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<?> updateCvdbIndex(String studyFqn, ClinicalAnalysis clinical, CvdbIndexStatus index, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn catalogFqn = CatalogFqn.extractFqnFromStudy(studyFqn, tokenPayload);
        String organizationId = catalogFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyFqn", studyFqn)
                .append("clinical", clinical.getId())
                .append("cvdbIndex", index)
                .append("token", token);

        String studyId = studyFqn;
        String studyUuid = "";
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
            Study study = studyManager.resolveId(catalogFqn, StudyManager.INCLUDE_STUDY_IDS, tokenPayload);
            studyId = study.getFqn();
            studyUuid = study.getUuid();

            ObjectMap valueAsMap = new ObjectMap(getUpdateObjectMapper().writeValueAsString(index));
            ObjectMap params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_CVDB_INDEX.key(), valueAsMap);

            OpenCGAResult<?> update = getClinicalAnalysisDBAdaptor(organizationId).update(clinical.getUid(), params,
                    Collections.emptyList(), Collections.emptyList(), QueryOptions.empty());
            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_INTERNAL, Enums.Resource.CLINICAL_ANALYSIS, clinical.getId(),
                    clinical.getUuid(), studyId, studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return new OpenCGAResult<>(update.getTime(), update.getEvents(), 1, Collections.emptyList(), 1);
        } catch (Exception e) {
            auditManager.audit(organizationId, userId, Enums.Action.UPDATE_INTERNAL, Enums.Resource.CLINICAL_ANALYSIS, clinical.getId(),
                    clinical.getUuid(), studyId, studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                            new Error().setName(e.getMessage())));
            throw new CatalogException("Could not update CVDB Index status: " + e.getMessage(), e);
        }
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

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        fixQueryObject(organizationId, study, query, userId, token);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getClinicalAnalysisDBAdaptor(organizationId).get(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, List<String> fields, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("field", fields)
                .append("query", new Query(query))
                .append("token", token);
        try {
            fixQueryObject(organizationId, study, query, userId, token);

            query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = getClinicalAnalysisDBAdaptor(organizationId).distinct(study.getUid(), fields, query, userId);

            auditManager.auditDistinct(organizationId, userId, Enums.Resource.CLINICAL_ANALYSIS, study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(organizationId, userId, Enums.Resource.CLINICAL_ANALYSIS, study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    protected void fixQueryObject(String organizationId, Study study, Query query, String user, String token) throws CatalogException {
        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, query);

        changeQueryId(query, ParamConstants.CLINICAL_DISORDER_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key());
        changeQueryId(query, ParamConstants.CLINICAL_ANALYST_ID_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.ANALYSTS_ID.key());
        changeQueryId(query, ParamConstants.CLINICAL_PRIORITY_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.PRIORITY_ID.key());
        changeQueryId(query, ParamConstants.CLINICAL_FLAGS_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.FLAGS_ID.key());
        changeQueryId(query, ParamConstants.CLINICAL_QUALITY_CONTROL_SUMMARY_PARAM,
                ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL_SUMMARY.key());
        changeQueryId(query, ParamConstants.CLINICAL_STATUS_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.STATUS_ID.key());
        changeQueryId(query, ParamConstants.CLINICAL_INTERNAL_STATUS_PARAM,
                ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key());

        if (query.containsKey(ParamConstants.CLINICAL_PANELS_PARAM)) {
            List<String> panelList = query.getAsStringList(ParamConstants.CLINICAL_PANELS_PARAM);
            query.remove(ParamConstants.CLINICAL_PANELS_PARAM);
            PanelDBAdaptor.QueryParams fieldFilter = catalogManager.getPanelManager().getFieldFilter(panelList);
            Query tmpQuery = new Query(fieldFilter.key(), panelList);

            OpenCGAResult<Panel> result = getPanelDBAdaptor(organizationId).get(study.getUid(), tmpQuery, PanelManager.INCLUDE_PANEL_IDS,
                    user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS_UID.key(),
                        result.getResults().stream().map(Panel::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS_UID.key(), -1);
            }
        }

        if (query.containsKey(ParamConstants.CLINICAL_FILES_PARAM)) {
            List<String> fileList = query.getAsStringList(ParamConstants.CLINICAL_FILES_PARAM);
            query.remove(ParamConstants.CLINICAL_FILES_PARAM);
            FileDBAdaptor.QueryParams fieldFilter = catalogManager.getFileManager().getFieldFilter(fileList);
            Query tmpQuery = new Query(fieldFilter.key(), fileList);

            OpenCGAResult<File> result = getFileDBAdaptor(organizationId).get(study.getUid(), tmpQuery, FileManager.INCLUDE_FILE_IDS, user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FILES_UID.key(),
                        result.getResults().stream().map(File::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FILES_UID.key(), -1);
            }
        }

        if (query.containsKey(ParamConstants.CLINICAL_PROBAND_PARAM)) {
            List<String> probandList = query.getAsStringList(ParamConstants.CLINICAL_PROBAND_PARAM);
            query.remove(ParamConstants.CLINICAL_PROBAND_PARAM);
            IndividualDBAdaptor.QueryParams fieldFilter = catalogManager.getIndividualManager().getFieldFilter(probandList);
            Query tmpQuery = new Query(fieldFilter.key(), probandList);

            OpenCGAResult<Individual> result = getIndividualDBAdaptor(organizationId).get(study.getUid(), tmpQuery,
                    IndividualManager.INCLUDE_INDIVIDUAL_IDS, user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_UID.key(),
                        result.getResults().stream().map(Individual::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_UID.key(), -1);
            }
        }
        if (query.containsKey(ParamConstants.CLINICAL_PROBAND_SAMPLES_PARAM)) {
            List<String> sampleList = query.getAsStringList(ParamConstants.CLINICAL_PROBAND_SAMPLES_PARAM);
            query.remove(ParamConstants.CLINICAL_PROBAND_SAMPLES_PARAM);
            SampleDBAdaptor.QueryParams fieldFilter = catalogManager.getSampleManager().getFieldFilter(sampleList);
            Query tmpQuery = new Query(fieldFilter.key(), sampleList);

            OpenCGAResult<Sample> result = getSampleDBAdaptor(organizationId).get(study.getUid(), tmpQuery,
                    SampleManager.INCLUDE_SAMPLE_IDS, user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_SAMPLES_UID.key(),
                        result.getResults().stream().map(Sample::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_SAMPLES_UID.key(), -1);
            }
        }
        if (query.containsKey(ParamConstants.CLINICAL_FAMILY_PARAM)) {
            List<String> familyList = query.getAsStringList(ParamConstants.CLINICAL_FAMILY_PARAM);
            query.remove(ParamConstants.CLINICAL_FAMILY_PARAM);
            FamilyDBAdaptor.QueryParams fieldFilter = catalogManager.getFamilyManager().getFieldFilter(familyList);
            Query tmpQuery = new Query(fieldFilter.key(), familyList);

            OpenCGAResult<Family> result = getFamilyDBAdaptor(organizationId).get(study.getUid(), tmpQuery,
                    FamilyManager.INCLUDE_FAMILY_IDS, user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_UID.key(),
                        result.getResults().stream().map(Family::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_UID.key(), -1);
            }
        }
        if (query.containsKey(ParamConstants.CLINICAL_FAMILY_MEMBERS_PARAM)) {
            List<String> probandList = query.getAsStringList(ParamConstants.CLINICAL_FAMILY_MEMBERS_PARAM);
            query.remove(ParamConstants.CLINICAL_FAMILY_MEMBERS_PARAM);
            IndividualDBAdaptor.QueryParams fieldFilter = catalogManager.getIndividualManager().getFieldFilter(probandList);
            Query tmpQuery = new Query(fieldFilter.key(), probandList);

            OpenCGAResult<Individual> result = getIndividualDBAdaptor(organizationId).get(study.getUid(), tmpQuery,
                    IndividualManager.INCLUDE_INDIVIDUAL_IDS,
                    user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_MEMBERS_UID.key(),
                        result.getResults().stream().map(Individual::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_MEMBERS_UID.key(), -1);
            }
        }
        if (query.containsKey(ParamConstants.CLINICAL_FAMILY_MEMBERS_SAMPLES_PARAM)) {
            List<String> sampleList = query.getAsStringList(ParamConstants.CLINICAL_FAMILY_MEMBERS_SAMPLES_PARAM);
            query.remove(ParamConstants.CLINICAL_FAMILY_MEMBERS_SAMPLES_PARAM);
            SampleDBAdaptor.QueryParams fieldFilter = catalogManager.getSampleManager().getFieldFilter(sampleList);
            Query tmpQuery = new Query(fieldFilter.key(), sampleList);

            OpenCGAResult<Sample> result = getSampleDBAdaptor(organizationId).get(study.getUid(), tmpQuery,
                    SampleManager.INCLUDE_SAMPLE_IDS, user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_MEMBERS_SAMPLES_UID.key(),
                        result.getResults().stream().map(Sample::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY_MEMBERS_SAMPLES_UID.key(), -1);
            }
        }

        if (query.containsKey(ParamConstants.CLINICAL_INDIVIDUAL_PARAM)) {
            List<String> individualList = query.getAsStringList(ParamConstants.CLINICAL_INDIVIDUAL_PARAM);
            query.remove(ParamConstants.CLINICAL_INDIVIDUAL_PARAM);

            IndividualDBAdaptor.QueryParams fieldFilter = catalogManager.getIndividualManager().getFieldFilter(individualList);
            Query tmpQuery = new Query(fieldFilter.key(), individualList);

            OpenCGAResult<Individual> result = getIndividualDBAdaptor(organizationId).get(study.getUid(), tmpQuery,
                    IndividualManager.INCLUDE_INDIVIDUAL_IDS,
                    user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(),
                        result.getResults().stream().map(Individual::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_UID.key(), -1);
            }
        }
        if (query.containsKey(ParamConstants.CLINICAL_SAMPLE_PARAM)) {
            List<String> sampleList = query.getAsStringList(ParamConstants.CLINICAL_SAMPLE_PARAM);
            query.remove(ParamConstants.CLINICAL_SAMPLE_PARAM);
            SampleDBAdaptor.QueryParams fieldFilter = catalogManager.getSampleManager().getFieldFilter(sampleList);
            Query tmpQuery = new Query(fieldFilter.key(), sampleList);

            OpenCGAResult<Sample> result = getSampleDBAdaptor(organizationId).get(study.getUid(), tmpQuery,
                    SampleManager.INCLUDE_SAMPLE_IDS, user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key(),
                        result.getResults().stream().map(Sample::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_SAMPLES_UID.key(), -1);
            }
        }
    }

    public OpenCGAResult<ClinicalAnalysis> count(String studyId, Query query, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            fixQueryObject(organizationId, study, query, userId, token);
            query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Long> queryResultAux = getClinicalAnalysisDBAdaptor(organizationId).count(query, userId);

            auditManager.auditCount(organizationId, userId, Enums.Resource.CLINICAL_ANALYSIS, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(organizationId, userId, Enums.Resource.CLINICAL_ANALYSIS, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> clinicalAnalysisIds, QueryOptions options, String token)
            throws CatalogException {
        return delete(studyStr, clinicalAnalysisIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> clinicalAnalysisIds, QueryOptions options, boolean ignoreException,
                                String token) throws CatalogException {
        if (CollectionUtils.isEmpty(clinicalAnalysisIds)) {
            throw new CatalogException("Missing list of Clinical Analysis ids");
        }

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysisIds", clinicalAnalysisIds)
                .append("options", options)
                .append("ignoreException", ignoreException)
                .append("token", token);

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult result = OpenCGAResult.empty();
        for (String id : clinicalAnalysisIds) {
            String clinicalId = id;
            String clinicalUuid = "";
            try {
                OpenCGAResult<ClinicalAnalysis> internalResult = internalGet(organizationId, study.getUid(), id,
                        keepFieldsInQueryOptions(INCLUDE_CLINICAL_INTERPRETATION_IDS, Arrays.asList(
                                ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION.key() + "."
                                        + InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS_ID.key(),
                                ClinicalAnalysisDBAdaptor.QueryParams.SECONDARY_INTERPRETATIONS.key() + "."
                                        + InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS_ID.key())),
                        userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Clinical Analysis '" + id + "' not found");
                }
                ClinicalAnalysis clinicalAnalysis = internalResult.first();

                // We set the proper values for the audit
                clinicalId = clinicalAnalysis.getId();
                clinicalUuid = clinicalAnalysis.getUuid();

                if (checkPermissions) {
                    authorizationManager.checkClinicalAnalysisPermission(organizationId, study.getUid(), clinicalAnalysis.getUid(), userId,
                            ClinicalAnalysisPermissions.DELETE);
                }

                // Check if the ClinicalAnalysis can be deleted
                checkClinicalAnalysisCanBeDeleted(clinicalAnalysis, options);

                ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.DELETE_CLINICAL_ANALYSIS,
                        "Delete Clinical Analysis '" + clinicalId + "'", TimeUtils.getTime());

                result.append(getClinicalAnalysisDBAdaptor(organizationId).delete(clinicalAnalysis,
                        Collections.singletonList(clinicalAudit)));

                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete Clinical Analysis " + clinicalId + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, clinicalId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalId, clinicalUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    private void checkClinicalAnalysisCanBeDeleted(ClinicalAnalysis clinicalAnalysis, QueryOptions options) throws CatalogException {
        if (options.getBoolean(Constants.FORCE)) {
            return;
        }
        if (clinicalAnalysis.getInterpretation() != null
                && CollectionUtils.isNotEmpty(clinicalAnalysis.getInterpretation().getPrimaryFindings())) {
            throw new CatalogException("Deleting a Clinical Analysis containing interpretations with findings is forbidden.");
        }
        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
            for (Interpretation interpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                if (interpretation != null && CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
                    throw new CatalogException("Deleting a Clinical Analysis containing interpretations with findings is forbidden.");
                }
            }
        }
        if (clinicalAnalysis.isLocked()) {
            throw new CatalogException("Deleting a locked Clinical Analysis is forbidden.");
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        OpenCGAResult result = OpenCGAResult.empty();

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("options", options)
                .append("ignoreException", ignoreException)
                .append("token", token);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the ClinicalAnalyses to be deleted
        DBIterator<ClinicalAnalysis> iterator;
        try {
            fixQueryObject(organizationId, study, finalQuery, userId, token);
            AnnotationUtils.fixQueryOptionAnnotation(options);
            finalQuery.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = getClinicalAnalysisDBAdaptor(organizationId).iterator(study.getUid(), finalQuery,
                    INCLUDE_CLINICAL_INTERPRETATION_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.CLINICAL_ANALYSIS, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkClinicalAnalysisPermission(organizationId, study.getUid(), clinicalAnalysis.getUid(), userId,
                            ClinicalAnalysisPermissions.DELETE);
                }

                // Check if the sample can be deleted
                checkClinicalAnalysisCanBeDeleted(clinicalAnalysis, options);

                ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.DELETE_CLINICAL_ANALYSIS,
                        "Delete Clinical Analysis '" + clinicalAnalysis.getId() + "'", TimeUtils.getTime());

                result.append(getClinicalAnalysisDBAdaptor(organizationId).delete(clinicalAnalysis,
                        Collections.singletonList(clinicalAudit)));

                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete Clinical Analysis " + clinicalAnalysis.getId() + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, clinicalAnalysis.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error(errorMsg);
                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.CLINICAL_ANALYSIS, clinicalAnalysis.getId(),
                        clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationUuid);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc,
                              String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options,
                                 String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(sessionId);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        fixQueryObject(organizationId, study, query, userId, sessionId);
        AnnotationUtils.fixQueryOptionAnnotation(options);

        // Add study id to the query
        query.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = getClinicalAnalysisDBAdaptor(organizationId).groupBy(query, fields, options, userId);
        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    public OpenCGAResult<ClinicalAnalysis> updateAnnotations(String studyStr, String clinicalStr, String annotationSetId,
                                                             Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                                             QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            throw new CatalogException("Missing array of annotations.");
        }
        ClinicalAnalysisUpdateParams clinicalUpdateParams = new ClinicalAnalysisUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, null, annotations)));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, clinicalStr, clinicalUpdateParams, options, token);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<ClinicalAnalysisPermissions>> getAcls(String studyStr, List<String> clinicalList, String member,
                                                                            boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyStr, clinicalList,
                StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(), ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<ClinicalAnalysisPermissions>> getAcls(String studyId, List<String> clinicalList, List<String> members,
                                                                            boolean ignoreException, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("clinicalList", clinicalList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);

        OpenCGAResult<AclEntryList<ClinicalAnalysisPermissions>> clinicalAcls = OpenCGAResult.empty();
        Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
        try {
            auditManager.initAuditBatch(operationId);
            InternalGetDataResult<ClinicalAnalysis> queryResult = internalGet(organizationId, study.getUid(), clinicalList,
                    INCLUDE_CLINICAL_IDS, userId, ignoreException);

            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> clinicalUids = queryResult.getResults().stream().map(ClinicalAnalysis::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                clinicalAcls = authorizationManager.getAcl(organizationId, study.getUid(), clinicalUids, members,
                        Enums.Resource.CLINICAL_ANALYSIS, ClinicalAnalysisPermissions.class, userId);
            } else {
                clinicalAcls = authorizationManager.getAcl(organizationId, study.getUid(), clinicalUids, Enums.Resource.CLINICAL_ANALYSIS,
                        ClinicalAnalysisPermissions.class, userId);
            }

            // Include non-existing samples to the result list
            List<AclEntryList<ClinicalAnalysisPermissions>> resultList = new ArrayList<>(clinicalList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String clinicalId : clinicalList) {
                if (!missingMap.containsKey(clinicalId)) {
                    ClinicalAnalysis clinical = queryResult.getResults().get(counter);
                    resultList.add(clinicalAcls.getResults().get(counter));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.CLINICAL_ANALYSIS,
                            clinical.getId(), clinical.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, clinicalId, missingMap.get(clinicalId).getErrorMsg()));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.CLINICAL_ANALYSIS,
                            clinicalId, "", study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "",
                                    missingMap.get(clinicalId).getErrorMsg())), new ObjectMap());
                }
            }
            for (int i = 0; i < queryResult.getResults().size(); i++) {
                clinicalAcls.getResults().get(i).setId(queryResult.getResults().get(i).getId());
            }
            clinicalAcls.setResults(resultList);
            clinicalAcls.setEvents(eventList);
        } catch (CatalogException e) {
            for (String caseId : clinicalList) {
                auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.CLINICAL_ANALYSIS, caseId,
                        "", study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
            }
            if (!ignoreException) {
                throw e;
            } else {
                for (String caseId : clinicalList) {
                    Event event = new Event(Event.Type.ERROR, caseId, e.getMessage());
                    clinicalAcls.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0, Collections.emptyList(), 0));
                }
            }
        } finally {
            auditManager.finishAuditBatch(organizationId, operationId);
        }

        return clinicalAcls;
    }

    public OpenCGAResult<AclEntryList<ClinicalAnalysisPermissions>> updateAcl(
            String studyStr, List<String> clinicalList, String memberIds, AclParams clinicalAclParams,
            ParamUtils.AclAction action, boolean propagate, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("clinicalList", clinicalList)
                .append("memberIds", memberIds)
                .append("clinicalAclParams", clinicalAclParams)
                .append("action", action)
                .append("token", token);

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.CLINICAL);

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
                checkPermissions(permissions, ClinicalAnalysisPermissions::valueOf);
            }

            OpenCGAResult<ClinicalAnalysis> queryResult = internalGet(organizationId, study.getUid(), clinicalList, INCLUDE_CATALOG_DATA,
                    userId, false);

            authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);

            // Validate that the members are actually valid members
            List<String> members;
            if (memberIds != null && !memberIds.isEmpty()) {
                members = Arrays.asList(memberIds.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(organizationId, study.getUid(), members);

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
                            .map(ClinicalAnalysisPermissions::valueOf)
                            .map(ClinicalAnalysisPermissions::getImplicitPermissions)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet())
                            .stream().map(Enum::name)
                            .collect(Collectors.toSet())
                    );
                }

                // Only propagate VIEW and WRITE permissions
                List<String> propagatedPermissions = new LinkedList<>();
                for (String permission : allPermissions) {
                    if (ClinicalAnalysisPermissions.VIEW.name().equals(permission)
                            || ClinicalAnalysisPermissions.WRITE.name().equals(permission)) {
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

                Set<Long> fileUids = queryResult.getResults().stream()
                        .flatMap(c -> c.getFiles() != null ? c.getFiles().stream() : Stream.empty())
                        .map(File::getUid)
                        .collect(Collectors.toSet());
                Set<Long> reportedFileUids = queryResult.getResults().stream()
                        .flatMap(c -> c.getReportedFiles() != null ? c.getReportedFiles().stream() : Stream.empty())
                        .map(File::getUid)
                        .collect(Collectors.toSet());
                // Unify fileUids and reportedFileUids in a single list without duplicates
                Set<Long> fileUidsList = new HashSet<>(fileUids.size() + reportedFileUids.size());
                fileUidsList.addAll(fileUids);
                fileUidsList.addAll(reportedFileUids);
                // Add the fileUidsList to the aclParamsList
                AuthorizationManager.CatalogAclParams.addToList(new ArrayList<>(fileUidsList), propagatedPermissions, Enums.Resource.FILE,
                        aclParamsList);
            }

            OpenCGAResult<AclEntryList<ClinicalAnalysisPermissions>> queryResults;
            switch (action) {
                case SET:
                    authorizationManager.setAcls(organizationId, study.getUid(), members, aclParamsList);
                    break;
                case ADD:
                    authorizationManager.addAcls(organizationId, study.getUid(), members, aclParamsList);
                    break;
                case REMOVE:
                    authorizationManager.removeAcls(organizationId, members, aclParamsList);
                    break;
                case RESET:
                    for (AuthorizationManager.CatalogAclParams aclParams : aclParamsList) {
                        aclParams.setPermissions(null);
                    }
                    authorizationManager.removeAcls(organizationId, members, aclParamsList);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            queryResults = authorizationManager.getAcls(organizationId, study.getUid(), clinicalUidList, members,
                    Enums.Resource.CLINICAL_ANALYSIS, ClinicalAnalysisPermissions.class);
            for (int i = 0; i < queryResults.getResults().size(); i++) {
                queryResults.getResults().get(i).setId(queryResult.getResults().get(i).getId());
            }
            for (ClinicalAnalysis clinicalAnalysis : queryResult.getResults()) {
                auditManager.audit(organizationId, operationUuid, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.CLINICAL_ANALYSIS,
                        clinicalAnalysis.getId(), clinicalAnalysis.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }

            return queryResults;
        } catch (CatalogException e) {
            if (clinicalList != null) {
                for (String clinicalId : clinicalList) {
                    auditManager.audit(organizationId, operationUuid, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.CLINICAL_ANALYSIS,
                            clinicalId, "", study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            throw e;
        } finally {
            auditManager.finishAuditBatch(organizationId, operationUuid);
        }
    }

    public OpenCGAResult configureStudy(String studyStr, ClinicalAnalysisStudyConfiguration clinicalConfiguration, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("clinicalConfiguration", clinicalConfiguration)
                .append("token", token);

        try {
            authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), userId);
            ParamUtils.checkObj(clinicalConfiguration, "ClinicalConfiguration");
            ParamUtils.checkObj(clinicalConfiguration.getFlags(), "flags");
            ParamUtils.checkObj(clinicalConfiguration.getPriorities(), "priorities");
            ParamUtils.checkNotEmptyArray(clinicalConfiguration.getConsents(), "consents");
            ParamUtils.checkObj(clinicalConfiguration.getInterpretation(), "interpretation");

            // Validate clinical configuration object
            validateClinicalStatus(clinicalConfiguration.getStatus(), "status");
            validateClinicalStatus(clinicalConfiguration.getInterpretation().getStatus(), "interpretation.status");

            ObjectMap update = new ObjectMap();
            try {
                update.putNested(StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION_CLINICAL.key(),
                        new ObjectMap(getUpdateObjectMapper().writeValueAsString(clinicalConfiguration)), true);
            } catch (JsonProcessingException e) {
                throw new CatalogException("Jackson casting error: " + e.getMessage(), e);
            }

            OpenCGAResult<Study> updateResult = getStudyDBAdaptor(organizationId).update(study.getUid(), update, QueryOptions.empty());
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.STUDY, study.getId(), study.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return updateResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.STUDY, study.getId(), study.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void validateClinicalStatus(List<ClinicalStatusValue> status, String field)
            throws CatalogException {
        if (CollectionUtils.isEmpty(status)) {
            throw CatalogParameterException.isNull(field);
        }
        // Ensure there's at least one status id per status type
        Map<ClinicalStatusValue.ClinicalStatusType, Boolean> presentMap = new HashMap<>();
        for (ClinicalStatusValue.ClinicalStatusType value : ClinicalStatusValue.ClinicalStatusType.values()) {
            // Init map
            presentMap.put(value, false);
        }
        for (ClinicalStatusValue clinicalStatusValue : status) {
            if (StringUtils.isEmpty(clinicalStatusValue.getId())) {
                throw CatalogParameterException.isNull(field + ".id");
            }
            if (clinicalStatusValue.getType() == null) {
                throw CatalogParameterException.isNull(field + ".type");
            }
            presentMap.put(clinicalStatusValue.getType(), true);
        }
        for (Map.Entry<ClinicalStatusValue.ClinicalStatusType, Boolean> entry : presentMap.entrySet()) {
            if (!entry.getValue()) {
                throw new CatalogException("Missing status values for ClinicalStatus type '" + entry.getKey() + "'");
            }
        }
    }
}
