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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

public class InterpretationManager extends ResourceManager<Interpretation> {

    protected static Logger logger = LoggerFactory.getLogger(InterpretationManager.class);

    private UserManager userManager;
    private StudyManager studyManager;

    public static final QueryOptions INCLUDE_INTERPRETATION_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            InterpretationDBAdaptor.QueryParams.ID.key(), InterpretationDBAdaptor.QueryParams.UID.key(),
            InterpretationDBAdaptor.QueryParams.UUID.key(), InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS_ID.key(),
            InterpretationDBAdaptor.QueryParams.VERSION.key(), InterpretationDBAdaptor.QueryParams.STUDY_UID.key()));

    public InterpretationManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                                 DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.INTERPRETATION;
    }

    @Override
    OpenCGAResult<Interpretation> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        ParamUtils.checkIsSingleID(entry);
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UuidUtils.isOpenCgaUuid(entry)) {
            queryCopy.put(InterpretationDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(InterpretationDBAdaptor.QueryParams.ID.key(), entry);
        }

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));

//        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
//                InterpretationDBAdaptor.QueryParams.UUID.key(), InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS.key(),
//                InterpretationDBAdaptor.QueryParams.UID.key(), InterpretationDBAdaptor.QueryParams.STUDY_UID.key(),
//                InterpretationDBAdaptor.QueryParams.ID.key(), InterpretationDBAdaptor.QueryParams.STATUS.key()));
        OpenCGAResult<Interpretation> interpretationDataResult = interpretationDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
        if (interpretationDataResult.getNumResults() == 0) {
            interpretationDataResult = interpretationDBAdaptor.get(queryCopy, queryOptions);
            if (interpretationDataResult.getNumResults() == 0) {
                throw new CatalogException("Interpretation " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the interpretation "
                        + entry);
            }
        } else if (interpretationDataResult.getNumResults() > 1) {
            throw new CatalogException("More than one interpretation found based on " + entry);
        } else {
            // We perform this query to check permissions because interpretations doesn't have ACLs
            try {
                catalogManager.getClinicalAnalysisManager().internalGet(studyUid,
                        interpretationDataResult.first().getClinicalAnalysisId(),
                        ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, user);
            } catch (CatalogException e) {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the interpretation "
                        + entry);
            }

            return interpretationDataResult;
        }
    }

    @Override
    InternalGetDataResult<Interpretation> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                                      String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing interpretation entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        Function<Interpretation, String> interpretationStringFunction = Interpretation::getId;
        InterpretationDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            InterpretationDBAdaptor.QueryParams param = InterpretationDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = InterpretationDBAdaptor.QueryParams.UUID;
                interpretationStringFunction = Interpretation::getUuid;
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

        OpenCGAResult<Interpretation> interpretationDataResult = interpretationDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        if (interpretationDataResult.getNumResults() != uniqueList.size() && !ignoreException) {
            throw CatalogException.notFound("interpretations",
                    getMissingFields(uniqueList, interpretationDataResult.getResults(), interpretationStringFunction));
        }

        ArrayList<Interpretation> interpretationList = new ArrayList<>(interpretationDataResult.getResults());
        Iterator<Interpretation> iterator = interpretationList.iterator();
        while (iterator.hasNext()) {
            Interpretation interpretation = iterator.next();
            // Check if the user has access to the corresponding clinical analysis
            try {
                catalogManager.getClinicalAnalysisManager().internalGet(studyUid,
                        interpretation.getClinicalAnalysisId(), ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, user);
            } catch (CatalogAuthorizationException e) {
                if (ignoreException) {
                    // Remove interpretation. User will not have permissions
                    iterator.remove();
                } else {
                    throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the"
                            + " interpretations", e);
                }
            }
        }

        interpretationDataResult.setResults(interpretationList);
        interpretationDataResult.setNumResults(interpretationList.size());
        interpretationDataResult.setNumMatches(interpretationList.size());

        return keepOriginalOrder(uniqueList, interpretationStringFunction, interpretationDataResult, ignoreException, false);
    }

    @Override
    public OpenCGAResult<Interpretation> create(String studyStr, Interpretation entry, QueryOptions options, String sessionId)
            throws CatalogException {
        throw new CatalogException("Non-supported. Use other create method");
    }

    public OpenCGAResult<Interpretation> create(String studyStr, String clinicalAnalysisStr, Interpretation interpretation,
                                                ParamUtils.SaveInterpretationAs saveInterpretationAs, QueryOptions options, String token)
            throws CatalogException {
        // We check if the user can create interpretations in the clinical analysis
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysis", clinicalAnalysisStr)
                .append("interpretation", interpretation)
                .append("saveAs", saveInterpretationAs)
                .append("options", options)
                .append("token", token);

        try {
            ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(), clinicalAnalysisStr,
                    ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, userId).first();

            authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(),
                    userId, ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

            validateNewInterpretation(study, interpretation, clinicalAnalysis.getId(), userId);

            ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.CREATE_INTERPRETATION,
                    "Create interpretation '" + interpretation.getId() + "'", TimeUtils.getTime());
            OpenCGAResult result = interpretationDBAdaptor.insert(study.getUid(), interpretation, saveInterpretationAs,
                    Collections.singletonList(clinicalAudit));
            OpenCGAResult<Interpretation> queryResult = interpretationDBAdaptor.get(study.getUid(), interpretation.getId(),
                    QueryOptions.empty());
            queryResult.setTime(result.getTime() + queryResult.getTime());

            auditManager.auditCreate(userId, Enums.Resource.INTERPRETATION, interpretation.getId(), "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.INTERPRETATION, interpretation.getId(), "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    void validateNewInterpretation(Study study, Interpretation interpretation, String clinicalAnalysisId, String userId)
            throws CatalogException {
        ParamUtils.checkObj(interpretation, "Interpretation");
        ParamUtils.checkParameter(clinicalAnalysisId, "ClinicalAnalysisId");

        ParamUtils.checkAlias(interpretation.getId(), "id");

        interpretation.setClinicalAnalysisId(clinicalAnalysisId);

        interpretation.setCreationDate(TimeUtils.getTime());
        interpretation.setModificationDate(TimeUtils.getTime());
        interpretation.setDescription(ParamUtils.defaultString(interpretation.getDescription(), ""));
        interpretation.setInternal(ParamUtils.defaultObject(interpretation.getInternal(), InterpretationInternal::new));
        interpretation.getInternal().setStatus(ParamUtils.defaultObject(interpretation.getInternal().getStatus(),
                InterpretationStatus::new));
        interpretation.setMethods(ParamUtils.defaultObject(interpretation.getMethods(), Collections.emptyList()));
        interpretation.setPrimaryFindings(ParamUtils.defaultObject(interpretation.getPrimaryFindings(), Collections.emptyList()));
        interpretation.setSecondaryFindings(ParamUtils.defaultObject(interpretation.getSecondaryFindings(), Collections.emptyList()));
        interpretation.setComments(ParamUtils.defaultObject(interpretation.getComments(), Collections.emptyList()));
        interpretation.setStatus(ParamUtils.defaultString(interpretation.getStatus(), ""));
        interpretation.setRelease(studyManager.getCurrentRelease(study));
        interpretation.setVersion(1);
        interpretation.setAttributes(ParamUtils.defaultObject(interpretation.getAttributes(), Collections.emptyMap()));
        interpretation.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.INTERPRETATION));

        // Analyst
        QueryOptions userInclude = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(UserDBAdaptor.QueryParams.ID.key(),
                UserDBAdaptor.QueryParams.NAME.key(), UserDBAdaptor.QueryParams.EMAIL.key()));
        User user;
        if (interpretation.getAnalyst() == null || StringUtils.isEmpty(interpretation.getAnalyst().getId())) {
            user = userDBAdaptor.get(userId, userInclude).first();
        } else {
            // Validate user
            OpenCGAResult<User> result = userDBAdaptor.get(interpretation.getAnalyst().getId(), userInclude);
            if (result.getNumResults() == 0) {
                throw new CatalogException("User '" + interpretation.getAnalyst().getId() + "' not found");
            }
            user = result.first();
        }
        interpretation.setAnalyst(new ClinicalAnalyst(user.getId(), user.getName(), user.getEmail(), userId, TimeUtils.getTime()));
    }

    public OpenCGAResult<Interpretation> clear(String studyStr, String clinicalAnalysisId, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysisId", clinicalAnalysisId)
                .append("token", token);

        String interpretationId = "";
        String interpretationUuid = "";

        try {
            OpenCGAResult<ClinicalAnalysis> clinicalResult = catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(),
                    clinicalAnalysisId, ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATIONS, userId);
            if (clinicalResult.getNumResults() == 0) {
                throw new CatalogException("ClinicalAnalysis '" + clinicalAnalysisId + "' not found");
            }
            if (clinicalResult.first().getInterpretation() == null
                    || StringUtils.isEmpty(clinicalResult.first().getInterpretation().getId())) {
                throw new CatalogException("No primary Interpretation found for ClinicalAnalysis '" + clinicalAnalysisId + "'");
            }

            interpretationId = clinicalResult.first().getInterpretation().getId();
            interpretationUuid = clinicalResult.first().getInterpretation().getUuid();

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), ParamUtils.UpdateAction.SET);
            actionMap.put(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), ParamUtils.UpdateAction.SET);
            actionMap.put(InterpretationDBAdaptor.QueryParams.METHODS.key(), ParamUtils.UpdateAction.SET);
            QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

            InterpretationUpdateParams params = new InterpretationUpdateParams("", new ClinicalAnalystParam(),
                    Collections.emptyList(), TimeUtils.getTime(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                    new ObjectMap());

            ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.CLEAR_INTERPRETATION,
                    "Clear interpretation '" + clinicalResult.first().getInterpretation().getId() + "'", TimeUtils.getTime());
            OpenCGAResult result = update(study, clinicalResult.first().getInterpretation(), params,
                    Collections.singletonList(clinicalAudit), null, options, userId);

            auditManager.audit(userId, Enums.Action.CLEAR, Enums.Resource.INTERPRETATION, interpretationId, interpretationUuid,
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.CLEAR, Enums.Resource.INTERPRETATION, interpretationId, interpretationUuid,
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

    }

    public OpenCGAResult<Interpretation> merge(String studyStr, String clinicalAnalysisId, String interpretationId,
                                               String interpretationId2, List<String> clinicalVariantList, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysisId", clinicalAnalysisId)
                .append("interpretationId", interpretationId)
                .append("interpretationId2", interpretationId2)
                .append("clinicalVariantList", clinicalVariantList)
                .append("token", token);

        String interpretationUuid = "";

        try {
            OpenCGAResult<Interpretation> tmpResult = internalGet(study.getUid(), interpretationId, INCLUDE_INTERPRETATION_IDS, userId);
            if (tmpResult.getNumResults() == 0) {
                throw new CatalogException("Interpretation '" + interpretationId + "' not found.");
            }
            Interpretation interpretation = tmpResult.first();

            if (!interpretation.getClinicalAnalysisId().equals(clinicalAnalysisId)) {
                throw new CatalogException("Interpretation '" + interpretationId + "' does not belong to ClinicalAnalysis '"
                        + clinicalAnalysisId + "'. It belongs to '" + interpretation.getClinicalAnalysisId() + "'.");
            }

            OpenCGAResult<ClinicalAnalysis> clinicalAnalysisOpenCGAResult = catalogManager.getClinicalAnalysisManager().internalGet(
                    study.getUid(), clinicalAnalysisId, ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATIONS, userId);
            if (clinicalAnalysisOpenCGAResult.getNumResults() == 0) {
                throw new CatalogException("ClinicalAnalysis '" + clinicalAnalysisId + "' not found.");
            }
            if (clinicalAnalysisOpenCGAResult.first().getInterpretation() == null
                    || !clinicalAnalysisOpenCGAResult.first().getInterpretation().getId().equals(interpretationId)) {
                throw new CatalogException("Interpretation '" + interpretationId + "' is not the primary interpretation of the "
                        + "ClinicalAnalysis '" + clinicalAnalysisId + "'.");
            }

            tmpResult = internalGet(study.getUid(), interpretationId2, QueryOptions.empty(), userId);
            if (tmpResult.getNumResults() == 0) {
                throw new CatalogException("Interpretation '" + interpretationId2 + "' not found.");
            }
            Interpretation interpretation2 = tmpResult.first();

            if (!interpretation.getClinicalAnalysisId().equals(clinicalAnalysisId)) {
                throw new CatalogException("Interpretation '" + interpretationId + "' does not belong to ClinicalAnalysis '"
                        + clinicalAnalysisId + "'. It belongs to '" + interpretation.getClinicalAnalysisId() + "'.");
            }

            // We set the proper values for the audit
            interpretationId = interpretation.getId();
            interpretationUuid = interpretation.getUuid();

            interpretation2.setMethods(ParamUtils.defaultObject(interpretation2.getMethods(), Collections.emptyList()));
            interpretation2.setPrimaryFindings(ParamUtils.defaultObject(interpretation2.getPrimaryFindings(), Collections.emptyList()));
            interpretation2.setSecondaryFindings(ParamUtils.defaultObject(interpretation2.getSecondaryFindings(), Collections.emptyList()));

            ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.MERGE_INTERPRETATION,
                    "Merge interpretation '" + interpretation2.getId() + "' in interpretation '" + interpretation.getId() + "'",
                    TimeUtils.getTime());
            OpenCGAResult<Interpretation> mergeResult = interpretationDBAdaptor.merge(interpretation.getUid(), interpretation2,
                    Collections.singletonList(clinicalAudit), clinicalVariantList);
            auditManager.audit(userId, Enums.Action.MERGE, Enums.Resource.INTERPRETATION, interpretationId, interpretationUuid,
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return mergeResult;
        } catch (CatalogException e) {
            logger.error("Cannot merge interpretation {}: {}", interpretationId, e.getMessage(), e);
            auditManager.audit(userId, Enums.Action.MERGE, Enums.Resource.INTERPRETATION, interpretationId, interpretationUuid,
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Interpretation> merge(String studyStr, String clinicalAnalysisId, String interpretationId,
                                               Interpretation interpretation2, List<String> clinicalVariantList, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysisId", clinicalAnalysisId)
                .append("interpretationId", interpretationId)
                .append("interpretation2", interpretation2)
                .append("clinicalVariantList", clinicalVariantList)
                .append("token", token);

        String interpretationUuid = "";

        try {
            OpenCGAResult<Interpretation> tmpResult = internalGet(study.getUid(), interpretationId, INCLUDE_INTERPRETATION_IDS, userId);
            if (tmpResult.getNumResults() == 0) {
                throw new CatalogException("Interpretation '" + interpretationId + "' not found.");
            }
            Interpretation interpretation = tmpResult.first();

            if (!interpretation.getClinicalAnalysisId().equals(clinicalAnalysisId)) {
                throw new CatalogException("Interpretation '" + interpretationId + "' does not belong to ClinicalAnalysis '"
                        + clinicalAnalysisId + "'. It belongs to '" + interpretation.getClinicalAnalysisId() + "'.");
            }

            OpenCGAResult<ClinicalAnalysis> clinicalAnalysisOpenCGAResult = catalogManager.getClinicalAnalysisManager().internalGet(
                    study.getUid(), clinicalAnalysisId, ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATIONS, userId);
            if (clinicalAnalysisOpenCGAResult.getNumResults() == 0) {
                throw new CatalogException("ClinicalAnalysis '" + clinicalAnalysisId + "' not found.");
            }
            if (clinicalAnalysisOpenCGAResult.first().getInterpretation() == null
                    || !clinicalAnalysisOpenCGAResult.first().getInterpretation().getId().equals(interpretationId)) {
                throw new CatalogException("Interpretation '" + interpretationId + "' is not the primary interpretation of the "
                        + "ClinicalAnalysis '" + clinicalAnalysisId + "'.");
            }

            // We set the proper values for the audit
            interpretationId = interpretation.getId();
            interpretationUuid = interpretation.getUuid();

            interpretation2.setMethods(ParamUtils.defaultObject(interpretation2.getMethods(), Collections.emptyList()));
            interpretation2.setPrimaryFindings(ParamUtils.defaultObject(interpretation2.getPrimaryFindings(), Collections.emptyList()));
            interpretation2.setSecondaryFindings(ParamUtils.defaultObject(interpretation2.getSecondaryFindings(), Collections.emptyList()));

            ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.MERGE_INTERPRETATION,
                    "Merge external interpretation in interpretation '" + interpretation.getId() + "'",
                    TimeUtils.getTime());
            OpenCGAResult<Interpretation> mergeResult = interpretationDBAdaptor.merge(interpretation.getUid(), interpretation2,
                    Collections.singletonList(clinicalAudit), clinicalVariantList);
            auditManager.audit(userId, Enums.Action.MERGE, Enums.Resource.INTERPRETATION, interpretationId, interpretationUuid,
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return mergeResult;
        } catch (CatalogException e) {
            logger.error("Cannot merge interpretation {}: {}", interpretationId, e.getMessage(), e);
            auditManager.audit(userId, Enums.Action.MERGE, Enums.Resource.INTERPRETATION, interpretationId, interpretationUuid,
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<Interpretation> update(String studyStr, Query query, InterpretationUpdateParams updateParams,
                                                ParamUtils.SaveInterpretationAs as, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, query, updateParams, as, false, options, token);
    }

    public OpenCGAResult<Interpretation> update(String studyStr, Query query, InterpretationUpdateParams updateParams,
                                                ParamUtils.SaveInterpretationAs as, boolean ignoreException, QueryOptions options,
                                                String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse InterpretationUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateMap)
                .append("as", as)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        DBIterator<Interpretation> iterator;
        try {
            finalQuery.append(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            iterator = interpretationDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_INTERPRETATION_IDS, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.INTERPRETATION, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Interpretation> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Interpretation interpretation = iterator.next();
            try {
                List<ClinicalAudit> clinicalAuditList = new ArrayList<>();
                clinicalAuditList.add(new ClinicalAudit(userId, ClinicalAudit.Action.UPDATE_INTERPRETATION,
                        "Update interpretation '" + interpretation.getId() + "'", TimeUtils.getTime()));
                if (as != null) {
                    clinicalAuditList.add(new ClinicalAudit(userId, ClinicalAudit.Action.SWAP_INTERPRETATION,
                            "Swap interpretation '" + interpretation.getId() + "' to " + as, TimeUtils.getTime()));
                }
                OpenCGAResult writeResult = update(study, interpretation, updateParams, clinicalAuditList, as, options, userId);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.INTERPRETATION, interpretation.getId(),
                        interpretation.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

                result.append(writeResult);
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, interpretation.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot update interpretation {}: {}", interpretation.getId(), e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.INTERPRETATION, interpretation.getId(),
                        interpretation.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Interpretation> update(String studyStr, String clinicalAnalysisId, String intepretationId,
                                                InterpretationUpdateParams updateParams, ParamUtils.SaveInterpretationAs as,
                                                QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse InterpretationUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysisId", clinicalAnalysisId)
                .append("intepretationId", intepretationId)
                .append("updateParams", updateMap)
                .append("as", as)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Interpretation> result = OpenCGAResult.empty();
        String interpretationId = "";
        String interpretationUuid = "";
        try {
            ParamUtils.checkParameter(clinicalAnalysisId, "ClinicalAnalysisId");
            ParamUtils.checkParameter(intepretationId, "InterpretationId");

            OpenCGAResult<Interpretation> interpretationOpenCGAResult = internalGet(study.getUid(), intepretationId,
                    INCLUDE_INTERPRETATION_IDS, userId);
            if (interpretationOpenCGAResult.getNumResults() == 0) {
                throw new CatalogException("Interpretation '" + interpretationId + "' not found.");
            }
            Interpretation interpretation = interpretationOpenCGAResult.first();

            if (!interpretation.getClinicalAnalysisId().equals(clinicalAnalysisId)) {
                throw new CatalogException("Interpretation '" + intepretationId + "' does not belong to ClinicalAnalysis '"
                        + clinicalAnalysisId + "'. It belongs to '" + interpretation.getClinicalAnalysisId() + "'.");
            }

            // We set the proper values for the audit
            interpretationId = interpretation.getId();
            interpretationUuid = interpretation.getUuid();

            List<ClinicalAudit> clinicalAuditList = new ArrayList<>();
            clinicalAuditList.add(new ClinicalAudit(userId, ClinicalAudit.Action.UPDATE_INTERPRETATION,
                    "Update interpretation '" + interpretation.getId() + "'", TimeUtils.getTime()));
            if (as != null) {
                clinicalAuditList.add(new ClinicalAudit(userId, ClinicalAudit.Action.SWAP_INTERPRETATION,
                        "Swap interpretation '" + interpretation.getId() + "' to " + as, TimeUtils.getTime()));
            }
            OpenCGAResult writeResult = update(study, interpretation, updateParams, clinicalAuditList, as, options, userId);
            result.append(writeResult);

            auditManager.auditUpdate(operationId, userId, Enums.Resource.INTERPRETATION, interpretation.getId(),
                    interpretation.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            CatalogException e1 = new CatalogException("Cannot update interpretation '" + interpretationId + "' of clinical analysis '"
                    + clinicalAnalysisId + "': " + e.getMessage(), e);
            Event event = new Event(Event.Type.ERROR, interpretationId, e1.getMessage());
            result.getEvents().add(event);

            logger.error("{}", e1.getMessage(), e);
            auditManager.auditUpdate(operationId, userId, Enums.Resource.INTERPRETATION, interpretationId, interpretationUuid,
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e1.getError()));
            throw e1;
        }

        return result;
    }

    /**
     * Update interpretations from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param clinicalAnalysisId ClinicalAnalysis id.
     * @param interpretationIds List of interpretation ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param as Enum to move the importance of the interpretation within the clinical analysis context.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A OpenCGAResult.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Interpretation> update(String studyStr, String clinicalAnalysisId, List<String> interpretationIds,
                                                InterpretationUpdateParams updateParams, ParamUtils.SaveInterpretationAs as,
                                                QueryOptions options, String token) throws CatalogException {
        return update(studyStr, clinicalAnalysisId, interpretationIds, updateParams, as, false, options, token);
    }

    public OpenCGAResult<Interpretation> update(String studyStr, String clinicalAnalysisId, List<String> interpretationIds,
                                                InterpretationUpdateParams updateParams, ParamUtils.SaveInterpretationAs as,
                                                boolean ignoreException, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse InterpretationUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("interpretationIds", interpretationIds)
                .append("clinicalAnalysisId", clinicalAnalysisId)
                .append("updateParams", updateMap)
                .append("as", as)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Interpretation> result = OpenCGAResult.empty();
        for (String id : interpretationIds) {
            String interpretationId = id;
            String interpretationUuid = "";

            try {
                OpenCGAResult<Interpretation> tmpResult = internalGet(study.getUid(), interpretationId, INCLUDE_INTERPRETATION_IDS, userId);
                if (tmpResult.getNumResults() == 0) {
                    throw new CatalogException("Interpretation '" + interpretationId + "' not found.");
                }
                Interpretation interpretation = tmpResult.first();

                if (!interpretation.getClinicalAnalysisId().equals(clinicalAnalysisId)) {
                    throw new CatalogException("Interpretation '" + interpretationId + "' does not belong to ClinicalAnalysis '"
                            + clinicalAnalysisId + "'. It belongs to '" + interpretation.getClinicalAnalysisId() + "'.");
                }

                // We set the proper values for the audit
                interpretationId = interpretation.getId();
                interpretationUuid = interpretation.getUuid();

                List<ClinicalAudit> clinicalAuditList = new ArrayList<>();
                clinicalAuditList.add(new ClinicalAudit(userId, ClinicalAudit.Action.UPDATE_INTERPRETATION,
                        "Update interpretation '" + interpretation.getId() + "'", TimeUtils.getTime()));
                if (as != null) {
                    clinicalAuditList.add(new ClinicalAudit(userId, ClinicalAudit.Action.SWAP_INTERPRETATION,
                            "Swap interpretation '" + interpretation.getId() + "' to " + as, TimeUtils.getTime()));
                }
                OpenCGAResult writeResult = update(study, interpretation, updateParams, clinicalAuditList, as, options, userId);
                result.append(writeResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.INTERPRETATION, interpretation.getId(),
                        interpretation.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot update interpretation {}: {}", interpretationId, e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.INTERPRETATION, interpretationId, interpretationUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult update(Study study, Interpretation interpretation, InterpretationUpdateParams updateParams,
                                 List<ClinicalAudit> clinicalAuditList,  ParamUtils.SaveInterpretationAs as, QueryOptions options,
                                 String userId) throws CatalogException {
        // Check if user has permissions to write clinical analysis
        ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(),
                interpretation.getClinicalAnalysisId(), ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, userId).first();
        authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(), userId,
                ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);

        ObjectMap parameters = new ObjectMap();
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse InterpretationUpdateParams object: " + e.getMessage(), e);
            }
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

        if ((ListUtils.isNotEmpty(interpretation.getPrimaryFindings()) || ListUtils.isNotEmpty(interpretation.getSecondaryFindings()))
                && !parameters.containsKey(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key())
                && !parameters.containsKey(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key())) {
            throw new CatalogException("Interpretation already has primary or secondary findings. Only array of primary or secondary "
                    + "findings can be updated.");
        }

        if (parameters.containsKey(InterpretationDBAdaptor.QueryParams.ID.key())) {
            ParamUtils.checkAlias(parameters.getString(InterpretationDBAdaptor.QueryParams.ID.key()),
                    InterpretationDBAdaptor.QueryParams.ID.key());
        }

        return interpretationDBAdaptor.update(interpretation.getUid(), parameters, clinicalAuditList, as, options);
    }

    @Override
    public DBIterator<Interpretation> iterator(String studyStr, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult<Interpretation> search(String studyId, Query query, QueryOptions options, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        query.append(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult<Interpretation> queryResult = interpretationDBAdaptor.get(study.getUid(), query, options, userId);

        List<Interpretation> results = new ArrayList<>(queryResult.getResults().size());
        for (Interpretation interpretation : queryResult.getResults()) {
            if (StringUtils.isNotEmpty(interpretation.getClinicalAnalysisId())) {
                try {
                    catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(),
                            interpretation.getClinicalAnalysisId(), ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS,
                            userId);
                    results.add(interpretation);
                } catch (CatalogException e) {
                    logger.debug("Removing interpretation " + interpretation.getUuid() + " from results. User " + userId + " does not have "
                            + "proper permissions");
                }
            }
        }

        queryResult.setResults(results);
        queryResult.setNumMatches(results.size());
        queryResult.setNumResults(results.size());
        return queryResult;
    }

    @Override
    public OpenCGAResult<Interpretation> count(String studyId, Query query, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> ids, ObjectMap params, String token) throws CatalogException {
        throw new NotImplementedException("Use other implemented delete method");
    }

    public OpenCGAResult delete(String studyStr, String clinicalAnalysisId, List<String> interpretationIds, String token)
            throws CatalogException {
        return delete(studyStr, clinicalAnalysisId, interpretationIds, false, token);
    }

    public OpenCGAResult delete(String studyStr, String clinicalAnalysisId, List<String> interpretationIds, boolean ignoreException,
                                String token) throws CatalogException {
        if (interpretationIds == null || ListUtils.isEmpty(interpretationIds)) {
            throw new CatalogException("Missing list of interpretation ids");
        }

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("clinicalAnalysisId", clinicalAnalysisId)
                .append("interpretationIds", interpretationIds)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationId, userId, Enums.Resource.INTERPRETATION, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        ClinicalAnalysis clinicalAnalysis;
        try {
            clinicalAnalysis = catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(), clinicalAnalysisId,
                    ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS, userId).first();
            if (checkPermissions) {
                authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(),
                        userId, ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.UPDATE);
            }
        } catch (CatalogException e) {
            auditManager.auditDelete(operationId, userId, Enums.Resource.INTERPRETATION, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult result = OpenCGAResult.empty();
        for (String id : interpretationIds) {
            String interpretationId = id;
            String interpretationUuid = "";
            try {
                OpenCGAResult<Interpretation> internalResult = internalGet(study.getUid(), id, INCLUDE_INTERPRETATION_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Interpretation '" + id + "' not found");
                }
                Interpretation interpretation = internalResult.first();

                // We set the proper values for the audit
                interpretationId = interpretation.getId();
                interpretationUuid = interpretation.getUuid();

                if (!interpretation.getClinicalAnalysisId().equals(clinicalAnalysis.getId())) {
                    throw new CatalogException("Cannot delete interpretation '" + interpretationId + "': Interpretation does not belong"
                            + " to ClinicalAnalysis '" + clinicalAnalysis.getId() + "'.");
                }

                // Check if the interpretation can be deleted
                // checkCanBeDeleted(study.getUid(), interpretation, params.getBoolean(Constants.FORCE, false));

                ClinicalAudit clinicalAudit = new ClinicalAudit(userId, ClinicalAudit.Action.DELETE_INTERPRETATION,
                        "Delete interpretation '" + interpretation.getId() + "'", TimeUtils.getTime());
                result.append(interpretationDBAdaptor.delete(interpretation, Collections.singletonList(clinicalAudit)));

                auditManager.auditDelete(operationId, userId, Enums.Resource.INTERPRETATION, interpretation.getId(),
                        interpretation.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg = "Cannot delete interpretation " + interpretationId + ": " + e.getMessage();

                Event event = new Event(Event.Type.ERROR, interpretationId, e.getMessage());
                result.getEvents().add(event);

                logger.error(errorMsg);
                auditManager.auditDelete(operationId, userId, Enums.Resource.INTERPRETATION, interpretationId, interpretationUuid,
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        throw new NotImplementedException("Use other delete implementation");
    }
//
//    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, boolean ignoreException, String token)
//            throws CatalogException {
//        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
//        params = ParamUtils.defaultObject(params, ObjectMap::new);
//
//        OpenCGAResult result = OpenCGAResult.empty();
//
//        String userId = catalogManager.getUserManager().getUserId(token);
//        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
//
//        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
//
//        ObjectMap auditParams = new ObjectMap()
//                .append("study", studyStr)
//                .append("query", new Query(query))
//                .append("params", params)
//                .append("ignoreException", ignoreException)
//                .append("token", token);
//
//        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
//        boolean checkPermissions;
//
//        // We try to get an iterator containing all the samples to be deleted
//        DBIterator<Interpretation> iterator;
//        try {
//            fixQueryObject(finalQuery);
//            finalQuery.append(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
//
//            iterator = interpretationDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_INTERPRETATION_IDS, userId);
//
//            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
//            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
//        } catch (CatalogException e) {
//            auditManager.auditDelete(operationUuid, userId, Enums.Resource.INTERPRETATION, "", "", study.getId(), study.getUuid(),
//                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
//            throw e;
//        }
//
//        auditManager.initAuditBatch(operationUuid);
//        while (iterator.hasNext()) {
//            Interpretation interpretation = iterator.next();
//
//            try {
//                ClinicalAnalysis clinicalAnalysis = catalogManager.getClinicalAnalysisManager().internalGet(study.getUid(),
//                        interpretation.getClinicalAnalysisId(), ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATIONS, userId).first();
//
//                if (checkPermissions) {
//                    authorizationManager.checkClinicalAnalysisPermission(study.getUid(), clinicalAnalysis.getUid(),
//                            userId, ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.DELETE);
//                }
//
//                // Check if the interpretation can be deleted
//                // checkCanBeDeleted(study.getUid(), interpretation, params.getBoolean(Constants.FORCE, false));
//
//                result.append(interpretationDBAdaptor.delete(interpretation));
//
//                auditManager.auditDelete(operationUuid, userId, Enums.Resource.INTERPRETATION, interpretation.getId(),
//                        interpretation.getUuid(), study.getId(), study.getUuid(), auditParams,
//                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
//            } catch (CatalogException e) {
//                String errorMsg = "Cannot delete interpretation " + interpretation.getId() + ": " + e.getMessage();
//
//                Event event = new Event(Event.Type.ERROR, interpretation.getId(), e.getMessage());
//                result.getEvents().add(event);
//
//                logger.error(errorMsg);
//                auditManager.auditDelete(operationUuid, userId, Enums.Resource.INTERPRETATION, interpretation.getId(),
//                        interpretation.getUuid(), study.getId(), study.getUuid(), auditParams,
//                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
//            }
//        }
//        auditManager.finishAuditBatch(operationUuid);
//
//        return endResult(result, ignoreException);
//    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        return null;
    }
}
