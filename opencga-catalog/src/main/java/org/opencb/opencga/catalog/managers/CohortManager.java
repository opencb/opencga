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
import org.opencb.biodata.models.common.Status;
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
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.cohort.*;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.core.models.common.Enums.Action.*;
import static org.opencb.opencga.core.models.common.Enums.Resource.COHORT;

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
        return COHORT;
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
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohortParams", cohortParams)
                .append("variableSetId", variableSetId)
                .append("variableId", variableId)
                .append("options", options)
                .append("token", token);
        return runBatch(auditParams, Enums.Action.CREATE, COHORT, studyStr, token, options, (study, user, myOptions, operationUuid) -> {
            ParamUtils.checkObj(cohortParams, "CohortCreateParams");
            authorizationManager.checkStudyPermission(study.getUid(), user, StudyPermissions.Permissions.WRITE_COHORTS);

            // Generate list of cohorts to create
            List<Cohort> cohorts = new LinkedList<>();
            if (StringUtils.isNotEmpty(variableId) && CollectionUtils.isNotEmpty(cohortParams.getSamples())) {
                throw new CatalogParameterException("Can only create a cohort given list of sampleIds or a categorical variable "
                        + "name");
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
                        SampleManager.INCLUDE_SAMPLE_IDS, user, false).getResults();
                cohorts.add(new Cohort(cohortParams.getId(), cohortParams.getName(), cohortParams.getType(),
                        cohortParams.getCreationDate(), cohortParams.getModificationDate(), cohortParams.getDescription(),
                        sampleList, 0, cohortParams.getAnnotationSets(), 1,
                        cohortParams.getStatus() != null ? cohortParams.getStatus().toStatus() : new Status(),
                        null, cohortParams.getAttributes()));

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
                    throw new CatalogException("Variable '" + variableId + "' does not exist in VariableSet "
                            + variableSet.getId());
                }
                if (variable.getType() != Variable.VariableType.CATEGORICAL) {
                    throw new CatalogException("Variable '" + variableId + "' is not a categorical variable. Please, choose a "
                            + "categorical variable");
                }
                for (String value : variable.getAllowedValues()) {
                    OpenCGAResult<Sample> sampleResults = catalogManager.getSampleManager().search(study.getFqn(),
                            new Query(Constants.ANNOTATION, variableSetId + ":" + variableId + "=" + value),
                            SampleManager.INCLUDE_SAMPLE_IDS, token);

                    cohorts.add(new Cohort(cohortParams.getId(), cohortParams.getName(), cohortParams.getType(),
                            cohortParams.getCreationDate(), cohortParams.getModificationDate(), cohortParams.getDescription(),
                            sampleResults.getResults(), 0, cohortParams.getAnnotationSets(), 1,
                            cohortParams.getStatus() != null ? cohortParams.getStatus().toStatus() : new Status(),
                            null, cohortParams.getAttributes()));
                }
            } else {
                //Create empty cohort
                cohorts.add(new Cohort(cohortParams.getId(), cohortParams.getName(), cohortParams.getType(),
                        cohortParams.getCreationDate(), cohortParams.getModificationDate(), cohortParams.getDescription(),
                        Collections.emptyList(), cohortParams.getAnnotationSets(), -1, null));
            }

            // Create cohorts
            OpenCGAResult<Cohort> insertResult = OpenCGAResult.empty(Cohort.class);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            for (Cohort cohort : cohorts) {
                try {
                    run(auditParams, Enums.Action.CREATE, COHORT, operationUuid, study, user, myOptions, (s, u, rp, qOptions) -> {
                        validateNewCohort(study, cohort);
                        rp.setId(cohort.getId());
                        rp.setUuid(cohort.getUuid());
                        OpenCGAResult<?> tmpResult = cohortDBAdaptor.insert(study.getUid(), cohort, study.getVariableSets(),
                                myOptions);
                        insertResult.append(tmpResult);
                        return null;
                    });
                } catch (CatalogException e) {
                    Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                    insertResult.getEvents().add(event);
                    logger.warn("Could not create cohort {}", cohort.getId(), e);
                }
            }

            stopWatch.stop();
            if (myOptions.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created cohort(s)
                Query query = new Query(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(CohortDBAdaptor.QueryParams.UID.key(), cohorts.stream()
                                .map(Cohort::getUid)
                                .filter(uid -> uid > 0)
                                .collect(Collectors.toList()));
                OpenCGAResult<Cohort> result = cohortDBAdaptor.get(study.getUid(), query, myOptions, user);
                insertResult.setResults(result.getResults());
            }
            return insertResult;
        });
    }

    public OpenCGAResult<Cohort> generate(String studyStr, Query sampleQuery, Cohort cohort, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("query", new Query(sampleQuery))
                .append("cohort", cohort)
                .append("options", options)
                .append("token", token);
        return run(auditParams, Enums.Action.GENERATE, COHORT, studyStr, token, options, (study, userId, rp, qOptions) -> {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyPermissions.Permissions.WRITE_COHORTS);

            // Fix sample query object and search for samples
            catalogManager.getSampleManager().fixQueryObject(study, sampleQuery, userId);
            AnnotationUtils.fixQueryOptionAnnotation(qOptions);
            sampleQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Sample> result = sampleDBAdaptor.get(study.getUid(), sampleQuery, qOptions, userId);

            // Add samples to provided cohort object
            cohort.setSamples(result.getResults());
            OpenCGAResult<Cohort> openCGAResult = privateCreate(study, cohort, qOptions, userId);
            rp.setId(cohort.getId());
            rp.setUuid(cohort.getUuid());

            return openCGAResult;
        });
    }

    @Override
    public OpenCGAResult<Cohort> create(String studyStr, Cohort cohort, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohort", cohort)
                .append("options", options)
                .append("token", token);
        return run(auditParams, Enums.Action.CREATE, COHORT, studyStr, token, options, (study, userId, rp, queryOptions) -> {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyPermissions.Permissions.WRITE_COHORTS);

            if (CollectionUtils.isNotEmpty(cohort.getSamples())) {
                // Look for the samples
                InternalGetDataResult<Sample> sampleResult = catalogManager.getSampleManager().internalGet(study.getUid(),
                        cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()), SampleManager.INCLUDE_SAMPLE_IDS,
                        userId, false);
                cohort.setSamples(sampleResult.getResults());
            }

            OpenCGAResult<Cohort> cohortResult = privateCreate(study, cohort, queryOptions, userId);
            rp.setId(cohort.getId());
            rp.setUuid(cohort.getUuid());
            return cohortResult;
        });
    }

    OpenCGAResult<Cohort> privateCreate(Study study, Cohort cohort, QueryOptions options, String userId) throws CatalogException {
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyPermissions.Permissions.WRITE_COHORTS);
        validateNewCohort(study, cohort);

        OpenCGAResult<Cohort> insert = cohortDBAdaptor.insert(study.getUid(), cohort, study.getVariableSets(), options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch created cohort
            OpenCGAResult<Cohort> result = getCohort(study.getUid(), cohort.getUuid(), options);
            insert.setResults(result.getResults());
        }
        return insert;
    }

    void validateNewCohort(Study study, Cohort cohort) throws CatalogException {
        ParamUtils.checkObj(cohort, "Cohort");
        ParamUtils.checkParameter(cohort.getId(), "id");
        ParamUtils.checkObj(cohort.getSamples(), "Sample list");
        cohort.setName(ParamUtils.defaultString(cohort.getName(), ""));
        cohort.setType(ParamUtils.defaultObject(cohort.getType(), Enums.CohortType.COLLECTION));
        cohort.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(cohort.getCreationDate(),
                CohortDBAdaptor.QueryParams.CREATION_DATE.key()));
        cohort.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(cohort.getModificationDate(),
                CohortDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        cohort.setDescription(ParamUtils.defaultString(cohort.getDescription(), ""));
        cohort.setAnnotationSets(ParamUtils.defaultObject(cohort.getAnnotationSets(), Collections::emptyList));
        cohort.setAttributes(ParamUtils.defaultObject(cohort.getAttributes(), HashMap::new));
        cohort.setRelease(studyManager.getCurrentRelease(study));
        cohort.setInternal(CohortInternal.init());
        cohort.setSamples(ParamUtils.defaultObject(cohort.getSamples(), Collections::emptyList));
        cohort.setStatus(ParamUtils.defaultObject(cohort.getStatus(), Status::new));
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
    @Deprecated
    public OpenCGAResult<Sample> getSamples(String studyStr, String cohortStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        OpenCGAResult<Cohort> cohortDataResult = internalGet(study.getUid(), cohortStr,
                new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key()), userId);

        if (cohortDataResult == null || cohortDataResult.getNumResults() == 0) {
            throw new CatalogException("No cohort " + cohortStr + " found in study " + studyStr);
        }
        if (cohortDataResult.first().getSamples().size() == 0) {
            return OpenCGAResult.empty(Sample.class);
        }

        return new OpenCGAResult<>(cohortDataResult.getTime(), cohortDataResult.getEvents(), cohortDataResult.first().getSamples().size(),
                cohortDataResult.first().getSamples(), cohortDataResult.first().getSamples().size());
    }

    @Override
    public DBIterator<Cohort> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        Query myQuery = query != null ? new Query(query) : new Query();
        return run(auditParams, Enums.Action.ITERATE, COHORT, studyStr, token, options, (study, userId, rp, qOptions) -> {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, myQuery);
            AnnotationUtils.fixQueryOptionAnnotation(qOptions);
            fixQueryObject(study, myQuery, userId);
            myQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            return cohortDBAdaptor.iterator(study.getUid(), myQuery, qOptions, userId);
        });
    }

    @Override
    public OpenCGAResult<Cohort> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        return run(auditParams, Enums.Action.SEARCH, COHORT, studyId, token, options, (study, userId, rp, queryOptions) -> {
            Query myQuery = query != null ? new Query(query) : new Query();

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, myQuery);
            AnnotationUtils.fixQueryOptionAnnotation(options);
            fixQueryObject(study, myQuery, userId);
            myQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return cohortDBAdaptor.get(study.getUid(), myQuery, options, userId);
        });
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("field", new Query(query))
                .append("query", new Query(query))
                .append("token", token);
        return run(auditParams, Enums.Action.DISTINCT, COHORT, studyId, token, null, (study, userId, rp, queryOptions) -> {
            Query myQuery = query != null ? new Query(query) : new Query();

            CohortDBAdaptor.QueryParams param = CohortDBAdaptor.QueryParams.getParam(field);
            if (param == null) {
                throw new CatalogException("Unknown '" + field + "' parameter.");
            }
            Class<?> clazz = getTypeClass(param.type());

            fixQueryObject(study, myQuery, userId);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, myQuery);
            myQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return cohortDBAdaptor.distinct(study.getUid(), field, myQuery, userId, clazz);
        });
    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.COHORT_INTERNAL_STATUS_PARAM, CohortDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key());

        if (query.containsKey(ParamConstants.COHORT_SAMPLES_PARAM)) {
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.UID.key());

            // First look for the sample ids.
            List<Sample> sampleList = catalogManager.getSampleManager().internalGet(study.getUid(),
                            query.getAsStringList(ParamConstants.COHORT_SAMPLES_PARAM), SampleManager.INCLUDE_SAMPLE_IDS, userId, true)
                    .getResults();
            if (CollectionUtils.isNotEmpty(sampleList)) {
                query.append(CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleList.stream().map(Sample::getUid)
                        .collect(Collectors.toList()));
            } else {
                // Add -1 so the query does not return any result
                query.append(CohortDBAdaptor.QueryParams.SAMPLE_UIDS.key(), -1);
            }

            query.remove(ParamConstants.COHORT_SAMPLES_PARAM);
        }
    }

    @Override
    public OpenCGAResult<Cohort> count(String studyId, Query query, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        return run(auditParams, Enums.Action.COUNT, COHORT, studyId, token, null, (study, userId, rp, queryOptions) -> {
            Query myQuery = query != null ? new Query(query) : new Query();

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, myQuery);
            fixQueryObject(study, myQuery, userId);
            myQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            return cohortDBAdaptor.count(myQuery, userId);
        });
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> cohortIds, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, cohortIds, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> cohortIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohortIds", cohortIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);
        return runBatch(auditParams, DELETE, COHORT, studyStr, token, null, (study, userId, queryOptions, auditOperationUuid) -> {
            if (cohortIds == null || ListUtils.isEmpty(cohortIds)) {
                throw new CatalogException("Missing list of cohort ids");
            }
            boolean checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);

            OpenCGAResult<Cohort> result = OpenCGAResult.empty(Cohort.class);
            for (String cohortId : cohortIds) {
                try {
                    run(auditParams, DELETE, COHORT, auditOperationUuid, study, userId, queryOptions, (s, u, rp, q) -> {
                        OpenCGAResult<Cohort> internalResult = internalGet(study.getUid(), cohortId, INCLUDE_COHORT_IDS,
                                userId);
                        if (internalResult.getNumResults() == 0) {
                            throw new CatalogException("Cohort '" + cohortId + "' not found");
                        }
                        rp.setId(internalResult.first().getId());
                        rp.setUuid(internalResult.first().getUuid());

                        if (checkPermissions) {
                            authorizationManager.checkCohortPermission(study.getUid(), internalResult.first().getUid(), userId,
                                    CohortPermissions.DELETE);
                        }
                        OpenCGAResult<?> deleteResult = cohortDBAdaptor.delete(internalResult.first());
                        result.append(deleteResult);
                        return null;
                    });
                } catch (CatalogException e) {
                    Event event = new Event(Event.Type.ERROR, cohortId, e.getMessage());
                    result.getEvents().add(event);
                    result.setNumErrors(result.getNumErrors() + 1);
                    logger.warn("Could not delete cohort {}", cohortId, e);
                }
            }

            return endResult(result, ignoreException);
        });
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return delete(studyStr, query, options, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        return runBatch(auditParams, DELETE, COHORT, studyStr, token, null, (study, userId, q, operationUuid) -> {
            Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
            OpenCGAResult<Cohort> result = OpenCGAResult.empty(Cohort.class);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            boolean checkPermissions;

            // We try to get an iterator containing all the cohorts to be deleted
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            try (DBIterator<Cohort> iterator = cohortDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_COHORT_IDS, userId)) {
                // If the user is the owner or the admin, we won't check if he has permissions for every single entry
                checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);

                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    try {
                        run(auditParams, DELETE, COHORT, operationUuid, study, userId, q, (study1, userId1, rp, qOptions) -> {
                            rp.setId(cohort.getId());
                            rp.setUuid(cohort.getUuid());

                            if (checkPermissions) {
                                authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId,
                                        CohortPermissions.DELETE);
                            }
                            OpenCGAResult<Cohort> tmpResult = cohortDBAdaptor.delete(cohort);
                            result.append(tmpResult);
                            return null;
                        });
                    } catch (CatalogException e) {
                        Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                        result.getEvents().add(event);
                        result.setNumErrors(result.getNumErrors() + 1);
                        logger.warn("Could not delete cohort {}", cohort.getId(), e);
                    }
                }
            }

            return endResult(result, ignoreException);
        });
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
        return run(auditParams, UPDATE, COHORT, studyStr, token, options, (study, userId, rp, queryOptions) -> {
            OpenCGAResult<Cohort> internalResult = internalGet(study.getUid(), cohortId, INCLUDE_COHORT_STATUS, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Cohort '" + cohortId + "' not found");
            }
            Cohort cohort = internalResult.first();
            rp.setId(cohort.getId());
            rp.setUuid(cohort.getUuid());

            return update(study, cohort, updateParams, allowModifyCohortAll, options, userId);
        });
    }

    public OpenCGAResult<Cohort> update(String studyStr, List<String> cohortIds, CohortUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, cohortIds, updateParams, false, false, options, token);
    }

    /**
     * Update a Cohort from catalog.
     *
     * @param studyStr             Study id in string format. Could be one of [studyId|projectId:studyId|user@projectId:studyId].
     * @param cohortIds            List of cohort ids. Could be either the id or uuid.
     * @param updateParams         Data model filled only with the parameters to be updated.
     * @param allowModifyCohortAll Boolean indicating whether we should not raise an exception if the cohort ALL is to be updated.
     * @param ignoreException      Boolean indicating whether to ignore the exception in case of an error.
     * @param options              QueryOptions object.
     * @param token                Session id of the user logged in.
     * @return A list of DataResults with the object updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<Cohort> update(String studyStr, List<String> cohortIds, CohortUpdateParams updateParams,
                                        boolean allowModifyCohortAll, boolean ignoreException, QueryOptions options, String token)
            throws CatalogException {
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
        return runBatch(auditParams, UPDATE, COHORT, studyStr, token, options, (study, userId, queryOptions, operationUuid) -> {
            OpenCGAResult<Cohort> result = OpenCGAResult.empty(Cohort.class);
            for (String id : cohortIds) {
                try {
                    run(auditParams, UPDATE, COHORT, operationUuid, study, userId, queryOptions, (study1, userId1, rp, queryOptions1) -> {
                        OpenCGAResult<Cohort> internalResult = internalGet(study.getUid(), id, INCLUDE_COHORT_STATUS, userId);
                        if (internalResult.getNumResults() == 0) {
                            throw new CatalogException("Cohort '" + id + "' not found");
                        }
                        Cohort cohort = internalResult.first();
                        rp.setId(cohort.getId());
                        rp.setUuid(cohort.getUuid());

                        OpenCGAResult<Cohort> updateResult = update(study, cohort, updateParams, allowModifyCohortAll, options, userId);
                        result.append(updateResult);
                        return null;
                    });
                } catch (CatalogException e) {
                    Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                    result.getEvents().add(event);
                    result.setNumErrors(result.getNumErrors() + 1);
                    logger.warn("Could not update cohort {}", id, e);
                }
            }

            return endResult(result, ignoreException);
        });
    }

    /**
     * Update a Cohort from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query        Query object.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token        Session id of the user logged in.
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
        return runBatch(auditParams, UPDATE, COHORT, studyStr, token, options, (study, userId, queryOptions, operationUuid) -> {
            Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Cohort> result = OpenCGAResult.empty(Cohort.class);
            try (DBIterator<Cohort> iterator = cohortDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_COHORT_STATUS, userId)) {
                while (iterator.hasNext()) {
                    Cohort cohort = iterator.next();
                    try {
                        run(auditParams, UPDATE, COHORT, operationUuid, study, userId, queryOptions, (study1, userId1, rp, qOptions) -> {
                            OpenCGAResult<Cohort> queryResult = update(study, cohort, updateParams, allowModifyCohortAll, qOptions, userId);
                            result.append(queryResult);
                            return null;
                        });
                    } catch (CatalogException e) {
                        Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                        result.getEvents().add(event);
                        result.setNumErrors(result.getNumErrors() + 1);
                        logger.warn("Could not update cohort {}", cohort.getId(), e);
                    }
                }

                return endResult(result, ignoreException);
            }
        });
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

        if (updateParams != null && StringUtils.isNotEmpty(updateParams.getCreationDate())) {
            ParamUtils.checkDateFormat(updateParams.getCreationDate(), CohortDBAdaptor.QueryParams.CREATION_DATE.key());
        }
        if (updateParams != null && StringUtils.isNotEmpty(updateParams.getModificationDate())) {
            ParamUtils.checkDateFormat(updateParams.getModificationDate(), CohortDBAdaptor.QueryParams.MODIFICATION_DATE.key());
        }

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
                    CohortPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId,
                    CohortPermissions.WRITE);
        }

        if (!allowModifyCohortAll) {
            if (StudyEntry.DEFAULT_COHORT.equals(cohort.getId())) {
                throw new CatalogException("Unable to modify cohort " + StudyEntry.DEFAULT_COHORT);
            }
        }

        if (updateParams != null && (ListUtils.isNotEmpty(updateParams.getSamples())
                || StringUtils.isNotEmpty(updateParams.getId()))) {
            switch (cohort.getInternal().getStatus().getId()) {
                case CohortStatus.CALCULATING:
                    throw new CatalogException("Unable to modify a cohort while it's in status \"" + CohortStatus.CALCULATING
                            + "\"");
                case CohortStatus.READY:
                    parameters.putIfAbsent(CohortDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), CohortStatus.INVALID);
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
        OpenCGAResult<Cohort> update = cohortDBAdaptor.update(cohort.getUid(), parameters, study.getVariableSets(), options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated cohort
            OpenCGAResult<Cohort> result = cohortDBAdaptor.get(study.getUid(),
                    new Query(CohortDBAdaptor.QueryParams.UID.key(), cohort.getUid()), options, userId);
            update.setResults(result.getResults());
        }
        return update;
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
            auditManager.auditUpdate(userId, COHORT, cohortId, "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        try {
            authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId, CohortPermissions.WRITE);

            if (status != null && !CohortStatus.isValid(status)) {
                throw new CatalogException("The status " + status + " is not valid cohort status.");
            }

            ObjectMap parameters = new ObjectMap();
            parameters.putIfNotNull(CohortDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), status);
            parameters.putIfNotNull(CohortDBAdaptor.QueryParams.INTERNAL_STATUS_DESCRIPTION.key(), message);

            cohortDBAdaptor.update(cohort.getUid(), parameters, new QueryOptions());

            auditManager.auditUpdate(userId, COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
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
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyPermissions.Permissions.VIEW_COHORTS);

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
    public OpenCGAResult<AclEntryList<CohortPermissions>> getAcls(String studyId, List<String> cohortList, String member,
                                                                  boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, cohortList, Collections.singletonList(member), ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<CohortPermissions>> getAcls(String studyId, List<String> cohortList, List<String> members,
                                                                  boolean ignoreException, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("cohortList", cohortList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);
        return runBatch(auditParams, FETCH_ACLS, COHORT, studyId, token, null, (study, userId, qOptions, operationUuid) -> {
            OpenCGAResult<AclEntryList<CohortPermissions>> cohortAcls = OpenCGAResult.empty();
            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            InternalGetDataResult<Cohort> queryResult = internalGet(study.getUid(), cohortList, INCLUDE_COHORT_IDS, userId,
                    ignoreException);
            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> cohortUids = queryResult.getResults().stream().map(Cohort::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                cohortAcls = authorizationManager.getAcl(userId, study.getUid(), cohortUids, members, COHORT, CohortPermissions.class);
            } else {
                cohortAcls = authorizationManager.getAcl(userId, study.getUid(), cohortUids, COHORT, CohortPermissions.class);
            }

            // Include non-existing cohorts to the result list
            List<AclEntryList<CohortPermissions>> resultList = new ArrayList<>(cohortList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String cohortId : cohortList) {
                if (!missingMap.containsKey(cohortId)) {
                    Cohort cohort = queryResult.getResults().get(counter);
                    run(auditParams, FETCH_ACLS, COHORT, operationUuid, study, userId, qOptions, (s, u, rp, qo) -> {
                        rp.setId(cohort.getId());
                        rp.setUuid(cohort.getUuid());
                        return null;
                    });
                    resultList.add(cohortAcls.getResults().get(counter));
                    counter++;
                } else {
                    if (!ignoreException) {
                        throw new CatalogException(missingMap.get(cohortId).getErrorMsg());
                    }
                    eventList.add(new Event(Event.Type.ERROR, cohortId, missingMap.get(cohortId).getErrorMsg()));
                    resultList.add(new AclEntryList<>());
                }
            }
            cohortAcls.setResults(resultList);
            cohortAcls.setEvents(eventList);

            return cohortAcls;
        });
    }

    public OpenCGAResult<AclEntryList<CohortPermissions>> updateAcl(String studyId, List<String> cohortStrList,
                                                                    String memberList, AclParams aclParams,
                                                                    ParamUtils.AclAction action, String token)
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
            auditManager.initAuditBatch(operationId);

            if (cohortStrList == null || cohortStrList.isEmpty()) {
                throw new CatalogException("Missing cohort parameter");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, CohortPermissions::valueOf);
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
                    COHORT);

            OpenCGAResult<AclEntryList<CohortPermissions>> queryResultList;
            switch (action) {
                case SET:
                    authorizationManager.setAcls(study.getUid(), members, catalogAclParams);
                    break;
                case ADD:
                    authorizationManager.addAcls(study.getUid(), members, catalogAclParams);
                    break;
                case REMOVE:
                    authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                case RESET:
                    catalogAclParams.setPermissions(null);
                    authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            queryResultList = authorizationManager.getAcls(study.getUid(), cohortUids, members, COHORT,
                    CohortPermissions.class);

            for (Cohort cohort : cohortList) {
                auditManager.audit(operationId, userId, UPDATE_ACLS, COHORT, cohort.getId(),
                        cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (CatalogException e) {
            if (cohortStrList != null) {
                for (String cohortId : cohortStrList) {
                    auditManager.audit(operationId, userId, UPDATE_ACLS, COHORT, cohortId, "",
                            study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            throw e;
        } finally {
            auditManager.finishAuditBatch(operationId);
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
                auditManager.auditFacet(userId, COHORT, study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

                return result;
            }
        } catch (CatalogException e) {
            auditManager.auditFacet(userId, COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage())));
            throw e;
        }
    }
}
