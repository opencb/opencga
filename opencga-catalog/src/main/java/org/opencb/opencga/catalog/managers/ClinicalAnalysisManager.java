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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.common.Status;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.FlagAnnotation;
import org.opencb.opencga.core.models.common.FlagValue;
import org.opencb.opencga.core.models.common.StatusValue;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileReferenceParam;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelReferenceParam;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.configuration.ClinicalConsent;
import org.opencb.opencga.core.models.study.configuration.*;
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
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisManager extends ResourceManager<ClinicalAnalysis> {

    public static final QueryOptions INCLUDE_CLINICAL_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.TYPE.key(), ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key()));
    public static final QueryOptions INCLUDE_CATALOG_DATA = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            ClinicalAnalysisDBAdaptor.QueryParams.ID.key(), ClinicalAnalysisDBAdaptor.QueryParams.UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.UUID.key(), ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.LOCKED.key(), ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(),
            ClinicalAnalysisDBAdaptor.QueryParams.TYPE.key()));
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
//        OpenCGAResult<ClinicalAnalysis> analysisDataResult = clinicalDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
//        if (analysisDataResult.getNumResults() == 0) {
//            analysisDataResult = clinicalDBAdaptor.get(queryCopy, queryOptions);
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

        fixQueryObject(study, query, userId, sessionId);
        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return clinicalDBAdaptor.iterator(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis, QueryOptions options, String token)
            throws CatalogException {
        return create(studyStr, clinicalAnalysis, null, options, token);
    }

    public OpenCGAResult<ClinicalAnalysis> create(String studyStr, ClinicalAnalysis clinicalAnalysis,
                                                  Boolean skipCreateDefaultInterpretation, QueryOptions options, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, StudyManager.INCLUDE_CONFIGURATION);

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

            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_CLINICAL_ANALYSIS);

            options = ParamUtils.defaultObject(options, QueryOptions::new);
            ParamUtils.checkObj(clinicalAnalysis, "clinicalAnalysis");
            ParamUtils.checkIdentifier(clinicalAnalysis.getId(), "id");
            ParamUtils.checkObj(clinicalAnalysis.getType(), "type");
            ParamUtils.checkObj(clinicalAnalysis.getProband(), "proband");

            clinicalAnalysis.setStatus(ParamUtils.defaultObject(clinicalAnalysis.getStatus(), Status::new));
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
                        panelDBAdaptor.get(study.getUid(), query, PanelManager.INCLUDE_PANEL_IDS, userId);
                if (panelResult.getNumResults() < panelIds.size()) {
                    throw new CatalogException("Some panels were not found or user doesn't have permissions to see them");
                }

                clinicalAnalysis.setPanels(panelResult.getResults());
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
            validateCustomStatusParameters(clinicalAnalysis, clinicalConfiguration);

            sortMembersFromFamily(clinicalAnalysis);

            clinicalAnalysis.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.CLINICAL));
            if (clinicalAnalysis.getInterpretation() == null
                    && (skipCreateDefaultInterpretation == null || !skipCreateDefaultInterpretation)) {
                clinicalAnalysis.setInterpretation(ParamUtils.defaultObject(clinicalAnalysis.getInterpretation(), Interpretation::new));
            }

            if (clinicalAnalysis.getInterpretation() != null) {
                catalogManager.getInterpretationManager().validateNewInterpretation(study, clinicalAnalysis.getInterpretation(),
                        clinicalAnalysis, userId);
            }

            ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.CREATE_CLINICAL_ANALYSIS,
                    "Create ClinicalAnalysis '" + clinicalAnalysis.getId() + "'", TimeUtils.getTime());
            OpenCGAResult result = clinicalDBAdaptor.insert(study.getUid(), clinicalAnalysis, Collections.singletonList(clinicalAudit),
                    options);

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

    private void validateCustomStatusParameters(ClinicalAnalysis clinicalAnalysis, ClinicalAnalysisStudyConfiguration clinicalConfiguration)
            throws CatalogException {
        // Status
        if (clinicalConfiguration.getStatus() == null
                || CollectionUtils.isEmpty(clinicalConfiguration.getStatus().get(clinicalAnalysis.getType()))) {
            throw new CatalogException("Missing status configuration in study for type '" + clinicalAnalysis.getType()
                    + "'. Please add a proper set of valid statuses.");
        }
        if (StringUtils.isNotEmpty(clinicalAnalysis.getStatus().getId())) {
            Map<String, StatusValue> statusMap = new HashMap<>();
            for (StatusValue status : clinicalConfiguration.getStatus().get(clinicalAnalysis.getType())) {
                statusMap.put(status.getId(), status);
            }
            if (!statusMap.containsKey(clinicalAnalysis.getStatus().getId())) {
                throw new CatalogException("Unknown status '" + clinicalAnalysis.getStatus().getId() + "'. The list of valid statuses is: '"
                        + String.join(",", statusMap.keySet()) + "'");
            }
            StatusValue statusValue = statusMap.get(clinicalAnalysis.getStatus().getId());
            clinicalAnalysis.getStatus().setDescription(statusValue.getDescription());
            clinicalAnalysis.getStatus().setDate(TimeUtils.getTime());
        }
    }

    private void validateCustomConsentParameters(ClinicalAnalysis clinicalAnalysis,
                                                 ClinicalAnalysisStudyConfiguration clinicalConfiguration) throws CatalogException {
        // Consent definition
        if (clinicalConfiguration.getConsent() == null || CollectionUtils.isEmpty(clinicalConfiguration.getConsent().getConsents())) {
            throw new CatalogException("Missing consent configuration in study. Please add a valid set of consents to the study"
                    + " configuration.");
        }
        Map<String, ClinicalConsent> consentMap = new HashMap<>();
        for (ClinicalConsent consent : clinicalConfiguration.getConsent().getConsents()) {
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
                            + clinicalConfiguration.getConsent().getConsents()
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
            if (CollectionUtils.isEmpty(clinicalConfiguration.getFlags().get(clinicalAnalysis.getType()))) {
                throw new CatalogException("Missing flags configuration in study for type '" + clinicalAnalysis.getType()
                        + "'. Please add a proper set of valid priorities.");
            }
            Map<String, FlagValue> supportedFlags = new HashMap<>();
            for (FlagValue flagValue : clinicalConfiguration.getFlags().get(clinicalAnalysis.getType())) {
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
                    throw new CatalogException("Flag '" + flag.getId() + "' not supported. Supported flags for Clinical Analyses of "
                            + "type '" + clinicalAnalysis.getType() + "' are: '" + String.join(", ", supportedFlags.keySet()) + "'.");
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
                .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), Arrays.asList(File.Bioformat.ALIGNMENT, File.Bioformat.VARIANT));
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

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, Query query, ClinicalAnalysisUpdateParams updateParams,
                                                  QueryOptions options, String token) throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, Query query, ClinicalAnalysisUpdateParams updateParams,
                                                  boolean ignoreException, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_CONFIGURATION);

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
            fixQueryObject(study, query, userId, token);
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

    public OpenCGAResult<ClinicalAnalysis> update(String studyStr, String clinicalId, ClinicalAnalysisUpdateParams updateParams,
                                                  QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_CONFIGURATION);

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
     * @param studyStr     Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
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
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_CONFIGURATION);

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

    private OpenCGAResult<ClinicalAnalysis> update(Study study, ClinicalAnalysis clinicalAnalysis,
                                                   ClinicalAnalysisUpdateParams updateParams, String userId, QueryOptions options)
            throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (study.getInternal() == null || study.getInternal().getConfiguration() == null
                || study.getInternal().getConfiguration().getClinical() == null) {
            throw new CatalogException("Unexpected error: ClinicalConfiguration is null");
        }
        ClinicalAnalysisStudyConfiguration clinicalConfiguration = study.getInternal().getConfiguration().getClinical();

        authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(), userId,
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.WRITE);

        if (StringUtils.isNotEmpty(clinicalAnalysis.getCreationDate())) {
            ParamUtils.checkDateFormat(clinicalAnalysis.getCreationDate(), ClinicalAnalysisDBAdaptor.QueryParams.CREATION_DATE.key());
        }
        if (StringUtils.isNotEmpty(clinicalAnalysis.getModificationDate())) {
            ParamUtils.checkDateFormat(clinicalAnalysis.getModificationDate(),
                    ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE.key());
        }

        ObjectMap parameters;
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse ClinicalUpdateParams object: " + e.getMessage(), e);
            }
        } else {
            throw new CatalogException("Empty update parameters. Nothing to update.");
        }
        ParamUtils.checkUpdateParametersMap(parameters);

        Map<String, Object> actionMap = options.getMap(Constants.ACTIONS);

        if (StringUtils.isNotEmpty(updateParams.getId())) {
            ParamUtils.checkIdentifier(updateParams.getId(), "id");
        }
        if (StringUtils.isNotEmpty(updateParams.getDueDate()) && TimeUtils.toDate(updateParams.getDueDate()) == null) {
            throw new CatalogException("Unrecognised due date. Accepted format is: yyyyMMddHHmmss");
        }

        if (updateParams.getComments() != null && !updateParams.getComments().isEmpty()) {
            List<ClinicalComment> comments = new ArrayList<>(updateParams.getComments().size());

            ParamUtils.AddRemoveReplaceAction action = ParamUtils.AddRemoveReplaceAction.from(actionMap,
                    ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), ParamUtils.AddRemoveReplaceAction.ADD);

            switch (action) {
                case ADD:
                    // Ensure each comment has a different milisecond
                    Calendar calendar = Calendar.getInstance();
                    for (ClinicalCommentParam comment : updateParams.getComments()) {
                        comments.add(new ClinicalComment(userId, comment.getMessage(), comment.getTags(),
                                TimeUtils.getTimeMillis(calendar.getTime())));
                        calendar.add(Calendar.MILLISECOND, 1);
                    }
                    break;
                case REMOVE:
                case REPLACE:
                    for (ClinicalCommentParam comment : updateParams.getComments()) {
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
        if (parameters.get(ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL.key()) != null) {
            ClinicalAnalysisQualityControl qualityControl = updateParams.getQualityControl().toClinicalQualityControl();
            if (qualityControl.getComments() != null) {
                for (ClinicalComment comment : qualityControl.getComments()) {
                    comment.setDate(TimeUtils.getTime());
                    comment.setAuthor(userId);
                }
            }
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL.key(), qualityControl);
        }

        if (updateParams.getFiles() != null && !updateParams.getFiles().isEmpty()) {
            clinicalAnalysis.setFiles(updateParams.getFiles().stream().map(FileReferenceParam::toFile).collect(Collectors.toList()));

            // Validate files
            validateFiles(study, clinicalAnalysis, userId);
        }

        if (CollectionUtils.isNotEmpty(updateParams.getPanels()) && updateParams.getPanelLock() != null && updateParams.getPanelLock()
                && (clinicalAnalysis.getInterpretation() != null
                || CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations()))) {
            throw new CatalogException("Updating the list of panels and setting 'panelLock' to true at the same time is not allowed "
                    + " when the ClinicalAnalysis has Interpretations.");
        }

        if (CollectionUtils.isNotEmpty(updateParams.getPanels())) {
            if (clinicalAnalysis.isPanelLock()) {
                throw new CatalogException("Cannot update panels from ClinicalAnalysis '" + clinicalAnalysis.getId() + "'. "
                        + "'panelLocked' field from ClinicalAnalysis is set to true.");
            }

            // Validate and get panels
            List<String> panelIds = updateParams.getPanels().stream().map(PanelReferenceParam::getId).collect(Collectors.toList());
            Query query = new Query(PanelDBAdaptor.QueryParams.ID.key(), panelIds);
            OpenCGAResult<org.opencb.opencga.core.models.panel.Panel> panelResult =
                    panelDBAdaptor.get(study.getUid(), query, PanelManager.INCLUDE_PANEL_IDS, userId);
            if (panelResult.getNumResults() < panelIds.size()) {
                throw new CatalogException("Some panels were not found or user doesn't have permissions to see them.");
            }

            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), panelResult.getResults());
        }

        if (updateParams.getPanelLock() != null && updateParams.getPanelLock() && !clinicalAnalysis.isPanelLock()) {
            // if user wants to set panelLock to true
            // We need to check if the CA has interpretations. If so, the interpretations should contain exactly the same panels in order
            // to set panelLock to true. Otherwise, that action is not allowed.
            Set<String> panelIds = clinicalAnalysis.getPanels().stream().map(Panel::getId).collect(Collectors.toSet());
            CatalogException exception = new CatalogException("The panels of the ClinicalAnalysis are different from the ones of at "
                    + "least one Interpretation. 'panelLock' can only be set to true if the Interpretations use exactly the same panels");
            if (clinicalAnalysis.getInterpretation() != null) {
                if (clinicalAnalysis.getInterpretation().getPanels().size() != panelIds.size()) {
                    throw exception;
                }
                for (Panel panel : clinicalAnalysis.getInterpretation().getPanels()) {
                    if (!panelIds.contains(panel.getId())) {
                        throw exception;
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
                for (Interpretation interpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                    if (interpretation.getPanels().size() != panelIds.size()) {
                        throw exception;
                    }
                    for (Panel panel : interpretation.getPanels()) {
                        if (!panelIds.contains(panel.getId())) {
                            throw exception;
                        }
                    }
                }
            }
        }

        if (updateParams.getFiles() != null && !updateParams.getFiles().isEmpty()) {
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), clinicalAnalysis.getFiles());
        }

        if (CollectionUtils.isNotEmpty(updateParams.getPanels())) {
            // Get panels
            Query query = new Query(PanelDBAdaptor.QueryParams.ID.key(),
                    updateParams.getPanels().stream().map(PanelReferenceParam::getId).collect(Collectors.toList()));
            OpenCGAResult<org.opencb.opencga.core.models.panel.Panel> panelResult =
                    panelDBAdaptor.get(study.getUid(), query, PanelManager.INCLUDE_PANEL_IDS, userId);
            if (panelResult.getNumResults() < updateParams.getPanels().size()) {
                throw new CatalogException("Some panels were not found or user doesn't have permissions to see them");
            }

            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), panelResult.getResults());
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

        // Validate user-defined parameters
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.PRIORITY.key())) {
            clinicalAnalysis.setPriority(updateParams.getPriority().toClinicalPriorityAnnotation());
            validateCustomPriorityParameters(clinicalAnalysis, clinicalConfiguration);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.PRIORITY.key(), clinicalAnalysis.getPriority());
        }
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key())) {
            Set<String> currentFlags = null;
            if (clinicalAnalysis.getFlags() != null) {
                currentFlags = clinicalAnalysis.getFlags().stream().map(FlagAnnotation::getId).collect(Collectors.toSet());
            }

            clinicalAnalysis.setFlags(updateParams.getFlags().stream().map(FlagValueParam::toFlagAnnotation).collect(Collectors.toList()));
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
            clinicalAnalysis.setConsent(updateParams.getConsent().toClinicalConsentAnnotation());
            validateCustomConsentParameters(clinicalAnalysis, clinicalConfiguration);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.CONSENT.key(), clinicalAnalysis.getConsent());
        }
        if (parameters.containsKey(ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key())) {
            clinicalAnalysis.setStatus(updateParams.getStatus().toCustomStatus());
            validateCustomStatusParameters(clinicalAnalysis, clinicalConfiguration);
            parameters.put(ClinicalAnalysisDBAdaptor.QueryParams.STATUS.key(), clinicalAnalysis.getStatus());
        }

        ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.UPDATE_CLINICAL_ANALYSIS,
                "Update ClinicalAnalysis '" + clinicalAnalysis.getId() + "'", TimeUtils.getTime());
        return clinicalDBAdaptor.update(clinicalAnalysis.getUid(), parameters, Collections.singletonList(clinicalAudit), options);
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

        fixQueryObject(study, query, userId, token);
        query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return clinicalDBAdaptor.get(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("field", new Query(query))
                .append("query", new Query(query))
                .append("token", token);
        try {
            ClinicalAnalysisDBAdaptor.QueryParams param = ClinicalAnalysisDBAdaptor.QueryParams.getParam(field);
            if (param == null) {
                throw new CatalogException("Unknown '" + field + "' parameter.");
            }
            Class<?> clazz = getTypeClass(param.type());

            fixQueryObject(study, query, userId, token);

            query.append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = clinicalDBAdaptor.distinct(study.getUid(), field, query, userId, clazz);

            auditManager.auditDistinct(userId, Enums.Resource.CLINICAL_ANALYSIS, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(userId, Enums.Resource.CLINICAL_ANALYSIS, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    protected void fixQueryObject(Study study, Query query, String user, String token) throws CatalogException {
        changeQueryId(query, ParamConstants.CLINICAL_DISORDER_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key());
        changeQueryId(query, ParamConstants.CLINICAL_ANALYST_ID_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.ANALYST_ID.key());
        changeQueryId(query, ParamConstants.CLINICAL_PRIORITY_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.PRIORITY_ID.key());
        changeQueryId(query, ParamConstants.CLINICAL_FLAGS_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.FLAGS_ID.key());
        changeQueryId(query, ParamConstants.CLINICAL_QUALITY_CONTROL_SUMMARY_PARAM,
                ClinicalAnalysisDBAdaptor.QueryParams.QUALITY_CONTROL_SUMMARY.key());
        changeQueryId(query, ParamConstants.CLINICAL_STATUS_PARAM, ClinicalAnalysisDBAdaptor.QueryParams.STATUS_ID.key());
        changeQueryId(query, ParamConstants.CLINICAL_INTERNAL_STATUS_PARAM,
                ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key());

        if (query.containsKey(ParamConstants.CLINICAL_PANELS_PARAM)) {
            List<String> panelList = query.getAsStringList(ParamConstants.CLINICAL_PANELS_PARAM);
            query.remove(ParamConstants.CLINICAL_PANELS_PARAM);
            PanelDBAdaptor.QueryParams fieldFilter = catalogManager.getPanelManager().getFieldFilter(panelList);
            Query tmpQuery = new Query(fieldFilter.key(), panelList);

            OpenCGAResult<Panel> result = panelDBAdaptor.get(study.getUid(), tmpQuery, PanelManager.INCLUDE_PANEL_IDS, user);
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

            OpenCGAResult<File> result = fileDBAdaptor.get(study.getUid(), tmpQuery, FileManager.INCLUDE_FILE_IDS, user);
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

            OpenCGAResult<Individual> result = individualDBAdaptor.get(study.getUid(), tmpQuery, IndividualManager.INCLUDE_INDIVIDUAL_IDS,
                    user);
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

            OpenCGAResult<Sample> result = sampleDBAdaptor.get(study.getUid(), tmpQuery, SampleManager.INCLUDE_SAMPLE_IDS, user);
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

            OpenCGAResult<Family> result = familyDBAdaptor.get(study.getUid(), tmpQuery, FamilyManager.INCLUDE_FAMILY_IDS, user);
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

            OpenCGAResult<Individual> result = individualDBAdaptor.get(study.getUid(), tmpQuery, IndividualManager.INCLUDE_INDIVIDUAL_IDS,
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

            OpenCGAResult<Sample> result = sampleDBAdaptor.get(study.getUid(), tmpQuery, SampleManager.INCLUDE_SAMPLE_IDS, user);
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

            OpenCGAResult<Individual> result = individualDBAdaptor.get(study.getUid(), tmpQuery, IndividualManager.INCLUDE_INDIVIDUAL_IDS,
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

            OpenCGAResult<Sample> result = sampleDBAdaptor.get(study.getUid(), tmpQuery, SampleManager.INCLUDE_SAMPLE_IDS, user);
            if (result.getNumResults() > 0) {
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.SAMPLE.key(),
                        result.getResults().stream().map(Sample::getUid).collect(Collectors.toList()));
            } else {
                // We won't return any results
                query.put(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_SAMPLES_UID.key(), -1);
            }
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
            fixQueryObject(study, query, userId, token);
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
    public OpenCGAResult delete(String studyStr, List<String> clinicalAnalysisIds, QueryOptions options, String token)
            throws CatalogException {
        return delete(studyStr, clinicalAnalysisIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> clinicalAnalysisIds, QueryOptions options, boolean ignoreException,
                                String token) throws CatalogException {
        if (CollectionUtils.isEmpty(clinicalAnalysisIds)) {
            throw new CatalogException("Missing list of Clinical Analysis ids");
        }

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

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
                checkClinicalAnalysisCanBeDeleted(clinicalAnalysis, options);

                ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.DELETE_CLINICAL_ANALYSIS,
                        "Delete Clinical Analysis '" + clinicalId + "'", TimeUtils.getTime());

                result.append(clinicalDBAdaptor.delete(clinicalAnalysis, Collections.singletonList(clinicalAudit)));

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

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

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
            fixQueryObject(study, finalQuery, userId, token);
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
                checkClinicalAnalysisCanBeDeleted(clinicalAnalysis, options);

                ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.DELETE_CLINICAL_ANALYSIS,
                        "Delete Clinical Analysis '" + clinicalAnalysis.getId() + "'", TimeUtils.getTime());

                result.append(clinicalDBAdaptor.delete(clinicalAnalysis, Collections.singletonList(clinicalAudit)));

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

        fixQueryObject(study, query, userId, sessionId);

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
                            || ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.WRITE.name().equals(permission)) {
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

    public OpenCGAResult configureStudy(String studyStr, ClinicalAnalysisStudyConfiguration clinicalConfiguration, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("clinicalConfiguration", clinicalConfiguration)
                .append("token", token);

        try {
            authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
            ParamUtils.checkObj(clinicalConfiguration, "clinical configuration");

            // TODO: Check fields

            ObjectMap update = new ObjectMap();
            try {
                update.putNested(StudyDBAdaptor.QueryParams.INTERNAL_CONFIGURATION_CLINICAL.key(),
                        new ObjectMap(getUpdateObjectMapper().writeValueAsString(clinicalConfiguration)), true);
            } catch (JsonProcessingException e) {
                throw new CatalogException("Jackson casting error: " + e.getMessage(), e);
            }

            OpenCGAResult<Study> updateResult = studyDBAdaptor.update(study.getUid(), update, QueryOptions.empty());
            auditManager.auditUpdate(userId, Enums.Resource.STUDY, study.getId(), study.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return updateResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, Enums.Resource.STUDY, study.getId(), study.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }
}
