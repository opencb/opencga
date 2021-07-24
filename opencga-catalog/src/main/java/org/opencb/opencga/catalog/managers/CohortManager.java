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
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.cohort.*;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * Created by pfurio on 06/07/16.
 */
public class CohortManager extends AnnotationSetManager<Cohort> {

    protected static Logger logger = LoggerFactory.getLogger(CohortManager.class);

    private UserManager userManager;
    private StudyManager studyManager;

    private final String defaultFacet = "creationYear>>creationMonth;status;numSamples[0..10]:1";

    public static final QueryOptions INCLUDE_COHORT_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            CohortDBAdaptor.QueryParams.STUDY_UID.key(), CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.UID.key(),
            CohortDBAdaptor.QueryParams.UUID.key()));
    public static final QueryOptions INCLUDE_COHORT_STATUS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            CohortDBAdaptor.QueryParams.STUDY_UID.key(), CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.UID.key(),
            CohortDBAdaptor.QueryParams.UUID.key(), CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key()));


    CohortManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.COHORT;
    }

    @Override
    InternalGetDataResult<Cohort> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                              String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing cohort entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        Function<Cohort, String> cohortStringFunction = Cohort::getId;
        CohortDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            CohortDBAdaptor.QueryParams param = CohortDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = CohortDBAdaptor.QueryParams.UUID;
                cohortStringFunction = Cohort::getUuid;
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

        OpenCGAResult<Cohort> cohortDataResult = cohortDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        if (ignoreException || cohortDataResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, cohortStringFunction, cohortDataResult, ignoreException, false);
        }
        // Query without adding the user check
        OpenCGAResult<Cohort> resultsNoCheck = cohortDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == cohortDataResult.getNumResults()) {
            throw CatalogException.notFound("cohorts",
                    getMissingFields(uniqueList, cohortDataResult.getResults(), cohortStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the cohorts.");
        }
    }

    private OpenCGAResult<Cohort> getCohort(long studyUid, String cohortUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(CohortDBAdaptor.QueryParams.UUID.key(), cohortUuid);
        return cohortDBAdaptor.get(query, options);
    }

    public OpenCGAResult<Cohort> create(String studyStr, CohortCreateParams cohortParams, String variableSetId, String variableId,
                                        QueryOptions options, String token) throws CatalogException {
        ParamUtils.checkObj(cohortParams, "CohortCreateParams");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_COHORTS);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohortParams", cohortParams)
                .append("variableSetId", variableSetId)
                .append("variableId", variableId)
                .append("options", options)
                .append("token", token);

        List<Cohort> cohorts = new LinkedList<>();
        try {
            if (StringUtils.isNotEmpty(variableId) && CollectionUtils.isNotEmpty(cohortParams.getSamples())) {
                throw new CatalogParameterException("Can only create a cohort given list of sampleIds or a categorical variable name");
            }

            ParamUtils.checkIdentifier(cohortParams.getId(), "id");

            if (CollectionUtils.isNotEmpty(cohortParams.getSamples())) {
                List<String> sampleIds = new ArrayList<>(cohortParams.getSamples().size());
                for (SampleReferenceParam sample : cohortParams.getSamples()) {
                    if (StringUtils.isNotEmpty(sample.getId())) {
                        sampleIds.add(sample.getId());
                    } else if (StringUtils.isNotEmpty(sample.getUuid())) {
                        sampleIds.add(sample.getUuid());
                    } else {
                        throw new CatalogParameterException("Found samples with missing id and uuid.");
                    }
                }
                List<Sample> sampleList = catalogManager.getSampleManager().internalGet(study.getUid(), sampleIds,
                        SampleManager.INCLUDE_SAMPLE_IDS, userId, false).getResults();
                cohorts.add(new Cohort(cohortParams.getId(), cohortParams.getType(), "", cohortParams.getDescription(), sampleList, 0,
                        cohortParams.getAnnotationSets(), 1,
                        cohortParams.getStatus() != null ? cohortParams.getStatus().toCustomStatus() : new CustomStatus(), null,
                        cohortParams.getAttributes()));

            } else if (StringUtils.isNotEmpty(variableSetId)) {
                // Look for variable set
                VariableSet variableSet = null;
                for (VariableSet auxVariableSet : study.getVariableSets()) {
                    if (auxVariableSet.equals(variableSetId)) {
                        variableSet = auxVariableSet;
                        break;
                    }
                }
                if (variableSet == null) {
                    throw new CatalogException("VariableSet '" + variableSetId + "' not found.");
                }

                // Look for variable
                // TODO: We should also look in nested variables
                Variable variable = null;
                for (Variable v : variableSet.getVariables()) {
                    if (v.getId().equals(variableId)) {
                        variable = v;
                        break;
                    }
                }
                if (variable == null) {
                    throw new CatalogException("Variable '" + variableId + "' does not exist in VariableSet " + variableSet.getId());
                }
                if (variable.getType() != Variable.VariableType.CATEGORICAL) {
                    throw new CatalogException("Variable '" + variableId + "' is not a categorical variable. Please, choose a categorical "
                            + "variable");
                }
                for (String value : variable.getAllowedValues()) {
                    OpenCGAResult<Sample> sampleResults = catalogManager.getSampleManager().search(study.getFqn(),
                            new Query(Constants.ANNOTATION, variableSetId + ":" + variableId + "=" + value),
                            SampleManager.INCLUDE_SAMPLE_IDS, token);

                    cohorts.add(new Cohort(cohortParams.getId(), cohortParams.getType(), "", cohortParams.getDescription(),
                            sampleResults.getResults(), 0, cohortParams.getAnnotationSets(), 1,
                            cohortParams.getStatus() != null ? cohortParams.getStatus().toCustomStatus() : new CustomStatus(), null,
                            cohortParams.getAttributes()));
                }
            } else {
                //Create empty cohort
                cohorts.add(new Cohort(cohortParams.getId(), cohortParams.getType(), "", cohortParams.getDescription(),
                        Collections.emptyList(), cohortParams.getAnnotationSets(), -1, null));
            }
        } catch (CatalogException e) {
            auditManager.audit(operationId, userId, Enums.Action.CREATE, Enums.Resource.COHORT, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        List<Event> eventList = new LinkedList<>();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (Cohort cohort : cohorts) {
            try {
                validateNewCohort(study, cohort);
                cohortDBAdaptor.insert(study.getUid(), cohort, study.getVariableSets(), options);
                auditManager.audit(operationId, userId, Enums.Action.CREATE, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                eventList.add(event);

                logger.error("Could not create cohort {}: {}", cohort.getId(), e.getMessage(), e);
                auditManager.audit(operationId, userId, Enums.Action.CREATE, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);
        stopWatch.stop();

        Query query = new Query(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(CohortDBAdaptor.QueryParams.UID.key(), cohorts.stream()
                        .map(Cohort::getUid)
                        .filter(uid -> uid > 0)
                        .collect(Collectors.toList()));
        OpenCGAResult<Cohort> result = cohortDBAdaptor.get(study.getUid(), query, options, userId);
        result.setTime((int) stopWatch.getTime(TimeUnit.MILLISECONDS));
        result.setEvents(eventList);

        return result;
    }

    public OpenCGAResult<Cohort> generate(String studyStr, Query sampleQuery, Cohort cohort, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_COHORTS);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("query", new Query(sampleQuery))
                .append("cohort", cohort)
                .append("options", options)
                .append("token", token);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        try {
            // Fix sample query object and search for samples
            catalogManager.getSampleManager().fixQueryObject(study, sampleQuery, userId);
            AnnotationUtils.fixQueryOptionAnnotation(options);
            sampleQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Sample> result = sampleDBAdaptor.get(study.getUid(), sampleQuery, options, userId);

            // Add samples to provided cohort object
            cohort.setSamples(result.getResults());

            OpenCGAResult<Cohort> cohortResult = privateCreate(study, cohort, options, userId);
            auditManager.audit(userId, Enums.Action.GENERATE, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return cohortResult;
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.GENERATE, Enums.Resource.COHORT, cohort.getId(), "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Cohort> create(String studyStr, Cohort cohort, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_COHORTS);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohort", cohort)
                .append("options", options)
                .append("token", token);
        try {
            if (CollectionUtils.isNotEmpty(cohort.getSamples())) {
                // Look for the samples
                InternalGetDataResult<Sample> sampleResult = catalogManager.getSampleManager().internalGet(study.getUid(),
                        cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()), SampleManager.INCLUDE_SAMPLE_IDS,
                        userId, false);
                cohort.setSamples(sampleResult.getResults());
            }

            OpenCGAResult<Cohort> cohortResult = privateCreate(study, cohort, options, userId);
            auditManager.auditCreate(userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return cohortResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.COHORT, cohort.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    OpenCGAResult<Cohort> privateCreate(Study study, Cohort cohort, QueryOptions options, String userId) throws CatalogException {
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_COHORTS);
        validateNewCohort(study, cohort);

        cohortDBAdaptor.insert(study.getUid(), cohort, study.getVariableSets(), options);
        return getCohort(study.getUid(), cohort.getUuid(), options);
    }

    void validateNewCohort(Study study, Cohort cohort) throws CatalogException {
        ParamUtils.checkObj(cohort, "Cohort");
        ParamUtils.checkParameter(cohort.getId(), "id");
        ParamUtils.checkObj(cohort.getSamples(), "Sample list");
        cohort.setType(ParamUtils.defaultObject(cohort.getType(), Enums.CohortType.COLLECTION));
        cohort.setCreationDate(TimeUtils.getTime());
        cohort.setModificationDate(TimeUtils.getTime());
        cohort.setDescription(ParamUtils.defaultString(cohort.getDescription(), ""));
        cohort.setAnnotationSets(ParamUtils.defaultObject(cohort.getAnnotationSets(), Collections::emptyList));
        cohort.setAttributes(ParamUtils.defaultObject(cohort.getAttributes(), HashMap::new));
        cohort.setRelease(studyManager.getCurrentRelease(study));
        cohort.setInternal(ParamUtils.defaultObject(cohort.getInternal(), CohortInternal::new));
        cohort.getInternal().setStatus(ParamUtils.defaultObject(cohort.getInternal().getStatus(), CohortStatus::new));
        cohort.setSamples(ParamUtils.defaultObject(cohort.getSamples(), Collections::emptyList));
        cohort.setStatus(ParamUtils.defaultObject(cohort.getStatus(), CustomStatus::new));
        cohort.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.COHORT));

        validateNewAnnotationSets(study.getVariableSets(), cohort.getAnnotationSets());

        if (!cohort.getSamples().isEmpty()) {
            // Remove possible duplicates and ensure we have all the internal uids
            Map<Long, Sample> sampleMap = new HashMap<>();
            for (Sample sample : cohort.getSamples()) {
                if (sample.getUid() <= 0) {
                    throw new CatalogException("Internal error. Missing sample uid.");
                }
                sampleMap.put(sample.getUid(), sample);
            }
            cohort.setSamples(new ArrayList<>(sampleMap.values()));
        }

        cohort.setNumSamples(cohort.getSamples().size());
    }

    public Long getStudyId(long cohortId) throws CatalogException {
        return cohortDBAdaptor.getStudyId(cohortId);
    }

    /**
     * Fetch all the samples from a cohort.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param cohortStr Cohort id or name.
     * @param sessionId Token of the user logged in.
     * @return a OpenCGAResult containing all the samples belonging to the cohort.
     * @throws CatalogException if there is any kind of error (permissions or invalid ids).
     */
    public OpenCGAResult<Sample> getSamples(String studyStr, String cohortStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        OpenCGAResult<Cohort> cohortDataResult = internalGet(study.getUid(), cohortStr,
                new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key()), userId);

        if (cohortDataResult == null || cohortDataResult.getNumResults() == 0) {
            throw new CatalogException("No cohort " + cohortStr + " found in study " + studyStr);
        }
        if (cohortDataResult.first().getSamples().size() == 0) {
            return OpenCGAResult.empty();
        }

        return new OpenCGAResult<>(cohortDataResult.getTime(), cohortDataResult.getEvents(), cohortDataResult.first().getSamples().size(),
                cohortDataResult.first().getSamples(), cohortDataResult.first().getSamples().size());
    }

    @Override
    public DBIterator<Cohort> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        // Fix query if it contains any annotation
        Query finalQuery = new Query(query);
        AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        fixQueryObject(study, finalQuery, userId);
        finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return cohortDBAdaptor.iterator(study.getUid(), finalQuery, options, userId);
    }

    @Override
    public OpenCGAResult<Cohort> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);
            AnnotationUtils.fixQueryOptionAnnotation(options);
            fixQueryObject(study, query, userId);
            query.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Cohort> queryResult = cohortDBAdaptor.get(study.getUid(), query, options, userId);

            auditManager.auditSearch(userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("field", new Query(query))
                .append("query", new Query(query))
                .append("token", token);
        try {
            CohortDBAdaptor.QueryParams param = CohortDBAdaptor.QueryParams.getParam(field);
            if (param == null) {
                throw new CatalogException("Unknown '" + field + "' parameter.");
            }
            Class<?> clazz = getTypeClass(param.type());

            fixQueryObject(study, query, userId);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);

            query.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = cohortDBAdaptor.distinct(study.getUid(), field, query, userId, clazz);

            auditManager.auditDistinct(userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        super.fixQueryObject(query);

        if (query.containsKey(CohortDBAdaptor.QueryParams.SAMPLES.key())) {
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.UID.key());

            // First look for the sample ids.
            List<Sample> sampleList = catalogManager.getSampleManager().internalGet(study.getUid(),
                    query.getAsStringList(CohortDBAdaptor.QueryParams.SAMPLES.key()), SampleManager.INCLUDE_SAMPLE_IDS, userId, true)
                    .getResults();
            if (ListUtils.isNotEmpty(sampleList)) {
                query.append(CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleList.stream().map(Sample::getUid)
                        .collect(Collectors.toList()));
            } else {
                // Add -1 so the query does not return any result
                query.append(CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), -1);
            }

            query.remove(CohortDBAdaptor.QueryParams.SAMPLES.key());
        }
    }

    @Override
    public OpenCGAResult<Cohort> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);
            fixQueryObject(study, query, userId);

            query.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = cohortDBAdaptor.count(query, userId);

            auditManager.auditCount(userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> cohortIds, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, cohortIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> cohortIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        if (cohortIds == null || ListUtils.isEmpty(cohortIds)) {
            throw new CatalogException("Missing list of cohort ids");
        }

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohortIds", cohortIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        boolean checkPermissions;
        try {
            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationId, userId, Enums.Resource.COHORT, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        OpenCGAResult result = OpenCGAResult.empty();
        auditManager.initAuditBatch(operationId);
        for (String id : cohortIds) {

            String cohortId = id;
            String cohortUuid = "";
            try {
                OpenCGAResult<Cohort> internalResult = internalGet(study.getUid(), id, INCLUDE_COHORT_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Cohort '" + id + "' not found");
                }

                // We set the proper values for the audit
                cohortId = internalResult.first().getId();
                cohortUuid = internalResult.first().getUuid();

                if (checkPermissions) {
                    authorizationManager.checkCohortPermission(study.getUid(), internalResult.first().getUid(), userId,
                            CohortAclEntry.CohortPermissions.DELETE);
                }
                OpenCGAResult deleteResult = cohortDBAdaptor.delete(internalResult.first());
                result.append(deleteResult);

                auditManager.auditDelete(operationId, userId, Enums.Resource.COHORT, internalResult.first().getId(),
                        internalResult.first().getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot delete cohort {}: {}", cohortId, e.getMessage());
                auditManager.auditDelete(operationId, userId, Enums.Resource.COHORT, cohortId, cohortUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        OpenCGAResult result = OpenCGAResult.empty();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the cohorts to be deleted
        DBIterator<Cohort> iterator;
        try {
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = cohortDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_COHORT_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.COHORT, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();
            try {
                if (checkPermissions) {
                    authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId,
                            CohortAclEntry.CohortPermissions.DELETE);
                }
                OpenCGAResult tmpResult = cohortDBAdaptor.delete(cohort);
                result.append(tmpResult);

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot delete cohort {}: {}", cohort.getId(), e.getMessage());
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        return endResult(result, ignoreException);
    }

    public OpenCGAResult<Cohort> updateAnnotationSet(String studyStr, String cohortStr, List<AnnotationSet> annotationSetList,
                                                     ParamUtils.BasicUpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        CohortUpdateParams updateParams = new CohortUpdateParams().setAnnotationSets(annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        // By default, allow update the annotationSet of the cohort ALL
        return update(studyStr, cohortStr, updateParams, true, options, token);
    }

    public OpenCGAResult<Cohort> addAnnotationSet(String studyStr, String cohortStr, AnnotationSet annotationSet, QueryOptions options,
                                                  String token) throws CatalogException {
        return addAnnotationSets(studyStr, cohortStr, Collections.singletonList(annotationSet), options, token);
    }

    public OpenCGAResult<Cohort> addAnnotationSets(String studyStr, String cohortStr, List<AnnotationSet> annotationSetList,
                                                   QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.BasicUpdateAction.ADD, options, token);
    }

    public OpenCGAResult<Cohort> removeAnnotationSet(String studyStr, String cohortStr, String annotationSetId, QueryOptions options,
                                                     String token) throws CatalogException {
        return removeAnnotationSets(studyStr, cohortStr, Collections.singletonList(annotationSetId), options, token);
    }

    public OpenCGAResult<Cohort> removeAnnotationSets(String studyStr, String cohortStr, List<String> annotationSetIdList,
                                                      QueryOptions options, String token) throws CatalogException {
        List<AnnotationSet> annotationSetList = annotationSetIdList
                .stream()
                .map(id -> new AnnotationSet().setId(id))
                .collect(Collectors.toList());
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.BasicUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Cohort> updateAnnotations(String studyStr, String cohortStr, String annotationSetId,
                                                   Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                                   QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            throw new CatalogException("Missing array of annotations.");
        }
        CohortUpdateParams updateParams = new CohortUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, "", annotations)));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, cohortStr, updateParams, options, token);
    }

    public OpenCGAResult<Cohort> removeAnnotations(String studyStr, String cohortStr, String annotationSetId,
                                                   List<String> annotations, QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, cohortStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Cohort> resetAnnotations(String studyStr, String cohortStr, String annotationSetId, List<String> annotations,
                                                  QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, cohortStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    public OpenCGAResult<Cohort> update(String studyStr, String cohortId, CohortUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, cohortId, updateParams, false, options, token);
    }

    public OpenCGAResult<Cohort> update(String studyStr, String cohortId, CohortUpdateParams updateParams, boolean allowModifyCohortAll,
                                        QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse CohortUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohortId", cohortId)
                .append("updateParams", updateMap)
                .append("allowModifyCohortAll", allowModifyCohortAll)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<Cohort> result = OpenCGAResult.empty();
        String cohortUuid = "";

        try {
            OpenCGAResult<Cohort> internalResult = internalGet(study.getUid(), cohortId, INCLUDE_COHORT_STATUS, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Cohort '" + cohortId + "' not found");
            }
            Cohort cohort = internalResult.first();

            // We set the proper values for the audit
            cohortId = cohort.getId();
            cohortUuid = cohort.getUuid();

            OpenCGAResult<Cohort> updateResult = update(study, cohort, updateParams, allowModifyCohortAll, options, userId);
            result.append(updateResult);

            auditManager.auditUpdate(operationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, cohortId, e.getMessage());
            result.getEvents().add(event);

            logger.error("Could not update cohort {}: {}", cohortId, e.getMessage(), e);
            auditManager.auditUpdate(operationId, userId, Enums.Resource.COHORT, cohortId, cohortUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    public OpenCGAResult<Cohort> update(String studyStr, List<String> cohortIds, CohortUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, cohortIds, updateParams, false, false, options, token);
    }

    /**
     * Update a Cohort from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param cohortIds  List of cohort ids. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param allowModifyCohortAll Boolean indicating whether we should not raise an exception if the cohort ALL is to be updated.
     * @param ignoreException Boolean indicating whether to ignore the exception in case of an error.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A list of DataResults with the object updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Cohort> update(String studyStr, List<String> cohortIds, CohortUpdateParams updateParams,
                                        boolean allowModifyCohortAll, boolean ignoreException, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse CohortUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohortIds", cohortIds)
                .append("updateParams", updateMap)
                .append("allowModifyCohortAll", allowModifyCohortAll)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Cohort> result = OpenCGAResult.empty();
        for (String id : cohortIds) {
            String cohortId = id;
            String cohortUuid = "";

            try {
                OpenCGAResult<Cohort> internalResult = internalGet(study.getUid(), id, INCLUDE_COHORT_STATUS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Cohort '" + id + "' not found");
                }
                Cohort cohort = internalResult.first();

                // We set the proper values for the audit
                cohortId = cohort.getId();
                cohortUuid = cohort.getUuid();

                OpenCGAResult<Cohort> updateResult = update(study, cohort, updateParams, allowModifyCohortAll, options, userId);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, cohortId, e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update cohort {}: {}", cohortId, e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.COHORT, cohortId, cohortUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    /**
     * Update a Cohort from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query  Query object.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A OpenCGAResult with the object updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Cohort> update(String studyStr, Query query, CohortUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, query, updateParams, false, false, options, token);
    }

    public OpenCGAResult<Cohort> update(String studyStr, Query query, CohortUpdateParams updateParams, boolean allowModifyCohortAll,
                                        boolean ignoreException, QueryOptions options, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse CohortUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateMap)
                .append("allowModifyCohortAll", allowModifyCohortAll)
                .append("options", options)
                .append("token", token);

        DBIterator<Cohort> iterator;
        try {
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = cohortDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_COHORT_STATUS, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.COHORT, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Cohort> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();
            try {
                OpenCGAResult<Cohort> queryResult = update(study, cohort, updateParams, allowModifyCohortAll, options, userId);
                result.append(queryResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not update cohort {}: {}", cohort.getId(), e.getMessage(), e);
                auditManager.auditUpdate(operationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult<Cohort> update(Study study, Cohort cohort, CohortUpdateParams updateParams, boolean allowModifyCohortAll,
                                         QueryOptions options, String userId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap parameters = new ObjectMap();
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse CohortUpdateParams object: " + e.getMessage(), e);
            }
        }
        ParamUtils.checkUpdateParametersMap(parameters);

        if (parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
            if (!actionMap.containsKey(AnnotationSetManager.ANNOTATION_SETS) && !actionMap.containsKey(AnnotationSetManager.ANNOTATIONS)) {
                logger.warn("Assuming the user wants to add the list of annotation sets provided");
                actionMap.put(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.BasicUpdateAction.ADD);
                options.put(Constants.ACTIONS, actionMap);
            }
        }

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (updateParams != null && updateParams.getAnnotationSets() != null) {
            authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId,
                    CohortAclEntry.CohortPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId,
                    CohortAclEntry.CohortPermissions.WRITE);
        }

        if (!allowModifyCohortAll) {
            if (StudyEntry.DEFAULT_COHORT.equals(cohort.getId())) {
                throw new CatalogException("Unable to modify cohort " + StudyEntry.DEFAULT_COHORT);
            }
        }

        if (updateParams != null && (ListUtils.isNotEmpty(updateParams.getSamples())
                || StringUtils.isNotEmpty(updateParams.getId()))) {
            switch (cohort.getInternal().getStatus().getName()) {
                case CohortStatus.CALCULATING:
                    throw new CatalogException("Unable to modify a cohort while it's in status \"" + CohortStatus.CALCULATING
                            + "\"");
                case CohortStatus.READY:
                    parameters.putIfAbsent(CohortDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), CohortStatus.INVALID);
                    break;
                case CohortStatus.NONE:
                case CohortStatus.INVALID:
                    break;
                default:
                    break;
            }

            if (CollectionUtils.isNotEmpty(updateParams.getSamples())) {
                // Remove possible duplications
                Set<String> sampleIds = new HashSet<>(updateParams.getSamples().size());
                for (SampleReferenceParam sample : updateParams.getSamples()) {
                    if (StringUtils.isNotEmpty(sample.getId())) {
                        sampleIds.add(sample.getId());
                    } else if (StringUtils.isNotEmpty(sample.getUuid())) {
                        sampleIds.add(sample.getUuid());
                    } else {
                        throw new CatalogParameterException("Found samples with missing id and uuid.");
                    }
                }

                InternalGetDataResult<Sample> sampleResult = catalogManager.getSampleManager().internalGet(study.getUid(),
                        new ArrayList<>(sampleIds), SampleManager.INCLUDE_SAMPLE_IDS, userId, false);

                if (sampleResult.getNumResults() != sampleIds.size()) {
                    throw new CatalogException("Could not find all the samples introduced. Update was not performed.");
                }

                // Override sample list of ids with sample list
                parameters.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleResult.getResults());
            }
        }

        checkUpdateAnnotations(study, cohort, parameters, options, VariableSet.AnnotableDataModels.COHORT, cohortDBAdaptor, userId);
        return cohortDBAdaptor.update(cohort.getUid(), parameters, study.getVariableSets(), options);
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);
        AnnotationUtils.fixQueryOptionAnnotation(options);

        // Add study id to the query
        query.put(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = cohortDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    public void setStatus(String studyStr, String cohortId, String status, String message, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohortId", cohortId)
                .append("status", status)
                .append("message", message)
                .append("token", token);
        Cohort cohort;
        try {
            cohort = internalGet(study.getUid(), cohortId, INCLUDE_COHORT_IDS, userId).first();
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, Enums.Resource.COHORT, cohortId, "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        try {
            authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId, CohortAclEntry.CohortPermissions.WRITE);

            if (status != null && !CohortStatus.isValid(status)) {
                throw new CatalogException("The status " + status + " is not valid cohort status.");
            }

            ObjectMap parameters = new ObjectMap();
            parameters.putIfNotNull(CohortDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), status);
            parameters.putIfNotNull(CohortDBAdaptor.QueryParams.INTERNAL_STATUS_DESCRIPTION.key(), message);

            cohortDBAdaptor.update(cohort.getUid(), parameters, new QueryOptions());

            auditManager.auditUpdate(userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.VIEW_COHORTS);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = cohortDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<Map<String, List<String>>> getAcls(String studyId, List<String> cohortList, String member, boolean ignoreException,
                                                            String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("cohortList", cohortList)
                .append("member", member)
                .append("ignoreException", ignoreException)
                .append("token", token);
        try {
            OpenCGAResult<Map<String, List<String>>> cohortAclList = OpenCGAResult.empty();

            InternalGetDataResult<Cohort> cohortDataResult = internalGet(study.getUid(), cohortList, INCLUDE_COHORT_IDS, user,
                    ignoreException);

            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            if (cohortDataResult.getMissing() != null) {
                missingMap = cohortDataResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }
            int counter = 0;
            for (String cohortId : cohortList) {
                if (!missingMap.containsKey(cohortId)) {
                    Cohort cohort = cohortDataResult.getResults().get(counter);
                    try {
                        OpenCGAResult<Map<String, List<String>>> allCohortAcls;
                        if (StringUtils.isNotEmpty(member)) {
                            allCohortAcls = authorizationManager.getCohortAcl(study.getUid(), cohort.getUid(), user, member);
                        } else {
                            allCohortAcls = authorizationManager.getAllCohortAcls(study.getUid(), cohort.getUid(), user);
                        }
                        cohortAclList.append(allCohortAcls);

                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.COHORT, cohort.getId(),
                                cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    } catch (CatalogException e) {
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.COHORT, cohort.getId(),
                                cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());

                        if (!ignoreException) {
                            throw e;
                        } else {
                            Event event = new Event(Event.Type.ERROR, cohortId, missingMap.get(cohortId).getErrorMsg());
                            cohortAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                                    Collections.singletonList(Collections.emptyMap()), 0));
                        }
                    }
                    counter += 1;
                } else {
                    Event event = new Event(Event.Type.ERROR, cohortId, missingMap.get(cohortId).getErrorMsg());
                    cohortAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                            Collections.singletonList(Collections.emptyMap()), 0));

                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.COHORT, cohortId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(cohortId).getErrorMsg())), new ObjectMap());
                }
            }
            return cohortAclList;
        } catch (CatalogException e) {
            for (String cohortId : cohortList) {
                auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.COHORT, cohortId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            throw e;
        }
    }

    public OpenCGAResult<Map<String, List<String>>> updateAcl(String studyId, List<String> cohortStrList, String memberList,
                                                              AclParams aclParams, ParamUtils.AclAction action, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, StudyManager.INCLUDE_STUDY_UID);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("cohortStrList", cohortStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("action", action)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            if (cohortStrList == null || cohortStrList.isEmpty()) {
                throw new CatalogException("Missing cohort parameter");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, CohortAclEntry.CohortPermissions::valueOf);
            }

            List<Cohort> cohortList = internalGet(study.getUid(), cohortStrList, INCLUDE_COHORT_IDS, userId, false).getResults();

            authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), userId);

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(study.getUid(), members);

            List<Long> cohortUids = cohortList.stream().map(Cohort::getUid).collect(Collectors.toList());

            AuthorizationManager.CatalogAclParams catalogAclParams = new AuthorizationManager.CatalogAclParams(cohortUids, permissions,
                    Enums.Resource.COHORT);

            OpenCGAResult<Map<String, List<String>>> queryResultList;
            switch (action) {
                case SET:
                    queryResultList = authorizationManager.setAcls(study.getUid(), members, catalogAclParams);
                    break;
                case ADD:
                    queryResultList = authorizationManager.addAcls(study.getUid(), members, catalogAclParams);
                    break;
                case REMOVE:
                    queryResultList = authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                case RESET:
                    catalogAclParams.setPermissions(null);
                    queryResultList = authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            for (Cohort cohort : cohortList) {
                auditManager.audit(operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.COHORT, cohort.getId(),
                        cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (CatalogException e) {
            if (cohortStrList != null) {
                for (String cohortId : cohortStrList) {
                    auditManager.audit(operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.COHORT, cohortId, "",
                            study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            throw e;
        }
    }

    public DataResult<FacetField> facet(String studyId, Query query, QueryOptions options, boolean defaultStats, String token)
            throws CatalogException, IOException {
        ParamUtils.defaultObject(query, Query::new);
        ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        // We need to add variableSets and groups to avoid additional queries as it will be used in the catalogSolrManager
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), StudyDBAdaptor.QueryParams.GROUPS.key())));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("defaultStats", defaultStats)
                .append("token", token);

        try {
            if (defaultStats || StringUtils.isEmpty(options.getString(QueryOptions.FACET))) {
                String facet = options.getString(QueryOptions.FACET);
                options.put(QueryOptions.FACET, StringUtils.isNotEmpty(facet) ? defaultFacet + ";" + facet : defaultFacet);
            }

            AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

            try (CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager)) {

                DataResult<FacetField> result = catalogSolrManager.facetedQuery(study, CatalogSolrManager.COHORT_SOLR_COLLECTION, query,
                        options, userId);
                auditManager.auditFacet(userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

                return result;
            }
        } catch (CatalogException e) {
            auditManager.auditFacet(userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage())));
            throw e;
        }
    }
}
