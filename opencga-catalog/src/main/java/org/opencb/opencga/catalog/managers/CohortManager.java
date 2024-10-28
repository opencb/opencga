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
import org.opencb.opencga.catalog.utils.*;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.JwtPayload;
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
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * Created by pfurio on 06/07/16.
 */
public class CohortManager extends AnnotationSetManager<Cohort> {

    public static final QueryOptions INCLUDE_COHORT_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            CohortDBAdaptor.QueryParams.STUDY_UID.key(), CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.UID.key(),
            CohortDBAdaptor.QueryParams.UUID.key()));
    public static final QueryOptions INCLUDE_COHORT_STATUS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
            CohortDBAdaptor.QueryParams.STUDY_UID.key(), CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.UID.key(),
            CohortDBAdaptor.QueryParams.UUID.key(), CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key()));
    protected static Logger logger = LoggerFactory.getLogger(CohortManager.class);
    private final String defaultFacet = "creationYear>>creationMonth;status;numSamples[0..10]:1";
    private UserManager userManager;
    private StudyManager studyManager;


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
    InternalGetDataResult<Cohort> internalGet(String organizationId, long studyUid, List<String> entryList, @Nullable Query query,
                                              QueryOptions options, String user, boolean ignoreException) throws CatalogException {
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

        OpenCGAResult<Cohort> cohortDataResult = getCohortDBAdaptor(organizationId).get(studyUid, queryCopy, queryOptions, user);

        if (ignoreException || cohortDataResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, cohortStringFunction, cohortDataResult, ignoreException, false);
        }
        // Query without adding the user check
        OpenCGAResult<Cohort> resultsNoCheck = getCohortDBAdaptor(organizationId).get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == cohortDataResult.getNumResults()) {
            throw CatalogException.notFound("cohorts",
                    getMissingFields(uniqueList, cohortDataResult.getResults(), cohortStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the cohorts.");
        }
    }

    private OpenCGAResult<Cohort> getCohort(String organizationId, long studyUid, String cohortUuid, QueryOptions options)
            throws CatalogException {
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(CohortDBAdaptor.QueryParams.UUID.key(), cohortUuid);
        return getCohortDBAdaptor(organizationId).get(query, options);
    }

    public OpenCGAResult<Cohort> create(String studyStr, CohortCreateParams cohortParams, String variableSetId,
                                        String variableId, QueryOptions options, String token) throws CatalogException {
        ParamUtils.checkObj(cohortParams, "CohortCreateParams");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, StudyManager.INCLUDE_VARIABLE_SET, userId, organizationId);
        authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_COHORTS);

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
                List<Sample> sampleList = catalogManager.getSampleManager().internalGet(organizationId, study.getUid(), sampleIds,
                        SampleManager.INCLUDE_SAMPLE_IDS, userId, false).getResults();
                cohorts.add(new Cohort(cohortParams.getId(), cohortParams.getName(), cohortParams.getType(),
                        cohortParams.getCreationDate(), cohortParams.getModificationDate(), cohortParams.getDescription(), sampleList, 0,
                        cohortParams.getAnnotationSets(), 1,
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
        } catch (CatalogException e) {
            auditManager.audit(organizationId, operationId, userId, Enums.Action.CREATE, Enums.Resource.COHORT, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Cohort> insertResult = OpenCGAResult.empty(Cohort.class);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (Cohort cohort : cohorts) {
            try {
                validateNewCohort(organizationId, study, cohort);
                OpenCGAResult tmpResult = getCohortDBAdaptor(organizationId).insert(study.getUid(), cohort, study.getVariableSets(),
                        options);
                insertResult.append(tmpResult);
                auditManager.audit(organizationId, operationId, userId, Enums.Action.CREATE, Enums.Resource.COHORT, cohort.getId(),
                        cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                insertResult.getEvents().add(event);

                logger.error("Could not create cohort {}: {}", cohort.getId(), e.getMessage(), e);
                auditManager.audit(organizationId, operationId, userId, Enums.Action.CREATE, Enums.Resource.COHORT, cohort.getId(),
                        cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }

        auditManager.finishAuditBatch(organizationId, operationId);
        stopWatch.stop();

        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch created cohort(s)
            Query query = new Query(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(CohortDBAdaptor.QueryParams.UID.key(), cohorts.stream()
                            .map(Cohort::getUid)
                            .filter(uid -> uid > 0)
                            .collect(Collectors.toList()));
            OpenCGAResult<Cohort> result = getCohortDBAdaptor(organizationId).get(study.getUid(), query, options, userId);
            insertResult.setResults(result.getResults());
        }

        return insertResult;
    }

    public OpenCGAResult<Cohort> generate(String studyStr, Query sampleQuery, Cohort cohort, QueryOptions options, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, StudyManager.INCLUDE_VARIABLE_SET, userId, organizationId);
        authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_COHORTS);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyStr)
                .append("query", new Query(sampleQuery))
                .append("cohort", cohort)
                .append("options", options)
                .append("token", token);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        try {
            // Fix sample query object and search for samples
            catalogManager.getSampleManager().fixQueryObject(organizationId, study, sampleQuery, userId);
            AnnotationUtils.fixQueryOptionAnnotation(options);
            sampleQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Sample> result = getSampleDBAdaptor(organizationId).get(study.getUid(), sampleQuery, options, userId);

            // Add samples to provided cohort object
            cohort.setSamples(result.getResults());

            OpenCGAResult<Cohort> cohortResult = privateCreate(organizationId, study, cohort, options, userId);
            auditManager.audit(organizationId, userId, Enums.Action.GENERATE, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return cohortResult;
        } catch (CatalogException e) {
            auditManager.audit(organizationId, userId, Enums.Action.GENERATE, Enums.Resource.COHORT, cohort.getId(), "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Cohort> create(String studyStr, Cohort cohort, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, StudyManager.INCLUDE_VARIABLE_SET, userId, organizationId);
        authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_COHORTS);

        ObjectMap auditParams = new ObjectMap()
                .append("organizationId", organizationId)
                .append("study", studyStr)
                .append("cohort", cohort)
                .append("options", options)
                .append("token", token);
        try {
            if (CollectionUtils.isNotEmpty(cohort.getSamples())) {
                // Look for the samples
                InternalGetDataResult<Sample> sampleResult = catalogManager.getSampleManager().internalGet(organizationId, study.getUid(),
                        cohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList()), SampleManager.INCLUDE_SAMPLE_IDS,
                        userId, false);
                cohort.setSamples(sampleResult.getResults());
            }

            OpenCGAResult<Cohort> cohortResult = privateCreate(organizationId, study, cohort, options, userId);
            auditManager.auditCreate(organizationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return cohortResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(organizationId, userId, Enums.Resource.COHORT, cohort.getId(), "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    OpenCGAResult<Cohort> privateCreate(String organizationId, Study study, Cohort cohort, QueryOptions options, String userId)
            throws CatalogException {
        authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.WRITE_COHORTS);
        validateNewCohort(organizationId, study, cohort);

        OpenCGAResult<Cohort> insert = getCohortDBAdaptor(organizationId).insert(study.getUid(), cohort, study.getVariableSets(), options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch created cohort
            OpenCGAResult<Cohort> result = getCohort(organizationId, study.getUid(), cohort.getUuid(), options);
            insert.setResults(result.getResults());
        }
        return insert;
    }

    void validateNewCohort(String organizationId, Study study, Cohort cohort) throws CatalogException {
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

    public Long getStudyId(String organizationId, long cohortId) throws CatalogException {
        return getCohortDBAdaptor(organizationId).getStudyId(cohortId);
    }

    /**
     * Fetch all the samples from a cohort.
     *
     * @param studyStr  Study id in string format. Could be one of
     *                  [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
     * @param cohortStr Cohort id or name.
     * @param token     Token of the user logged in.
     * @return a OpenCGAResult containing all the samples belonging to the cohort.
     * @throws CatalogException if there is any kind of error (permissions or invalid ids).
     */
    public OpenCGAResult<Sample> getSamples(String studyStr, String cohortStr, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);
        OpenCGAResult<Cohort> cohortDataResult = internalGet(organizationId, study.getUid(), cohortStr,
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

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(sessionId);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

        // Fix query if it contains any annotation
        Query finalQuery = new Query(query);
        AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, finalQuery);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        fixQueryObject(organizationId, study, finalQuery, userId);
        finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getCohortDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, options, userId);
    }

    @Override
    public OpenCGAResult<FacetField> facet(String studyStr, Query query, String facet, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);

        Study study = catalogManager.getStudyManager().resolveId(studyFqn, StudyManager.INCLUDE_VARIABLE_SET, tokenPayload);

        Query finalQuery = new Query(query);
        fixQueryObject(organizationId, study, finalQuery, userId);
        finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return getCohortDBAdaptor(organizationId).facet(study.getUid(), finalQuery, facet, userId);
    }

    @Override
    public OpenCGAResult<Cohort> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()), userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, query);
            AnnotationUtils.fixQueryOptionAnnotation(options);
            fixQueryObject(organizationId, study, query, userId);
            query.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<Cohort> queryResult = getCohortDBAdaptor(organizationId).get(study.getUid(), query, options, userId);

            auditManager.auditSearch(organizationId, userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(organizationId, userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, List<String> fields, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()), userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("fields", fields)
                .append("query", new Query(query))
                .append("token", token);
        try {
            fixQueryObject(organizationId, study, query, userId);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, query);

            query.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = getCohortDBAdaptor(organizationId).distinct(study.getUid(), fields, query, userId);

            auditManager.auditDistinct(organizationId, userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(organizationId, userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void fixQueryObject(String organizationId, Study study, Query query, String userId) throws CatalogException {
        super.fixQueryObject(query);
        changeQueryId(query, ParamConstants.COHORT_INTERNAL_STATUS_PARAM, CohortDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key());

        if (query.containsKey(ParamConstants.COHORT_SAMPLES_PARAM)) {
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.UID.key());

            // First look for the sample ids.
            List<Sample> sampleList = catalogManager.getSampleManager().internalGet(organizationId, study.getUid(),
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
        query = ParamUtils.defaultObject(query, Query::new);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()), userId, organizationId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, query);
            fixQueryObject(organizationId, study, query, userId);

            query.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = getCohortDBAdaptor(organizationId).count(query, userId);

            auditManager.auditCount(organizationId, userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(organizationId, userId, Enums.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
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

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()), userId, organizationId);

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
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.COHORT, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        OpenCGAResult result = OpenCGAResult.empty();
        auditManager.initAuditBatch(operationId);
        for (String id : cohortIds) {

            String cohortId = id;
            String cohortUuid = "";
            try {
                OpenCGAResult<Cohort> internalResult = internalGet(organizationId, study.getUid(), id, INCLUDE_COHORT_IDS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Cohort '" + id + "' not found");
                }

                // We set the proper values for the audit
                cohortId = internalResult.first().getId();
                cohortUuid = internalResult.first().getUuid();

                if (checkPermissions) {
                    authorizationManager.checkCohortPermission(organizationId, study.getUid(), internalResult.first().getUid(), userId,
                            CohortPermissions.DELETE);
                }
                OpenCGAResult deleteResult = getCohortDBAdaptor(organizationId).delete(internalResult.first());
                result.append(deleteResult);

                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.COHORT, internalResult.first().getId(),
                        internalResult.first().getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot delete cohort {}: {}", cohortId, e.getMessage());
                auditManager.auditDelete(organizationId, operationId, userId, Enums.Resource.COHORT, cohortId, cohortUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

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

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()), userId, organizationId);

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
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, finalQuery);
            fixQueryObject(organizationId, study, finalQuery, userId);
            finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = getCohortDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_COHORT_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            long studyId = study.getUid();
            checkPermissions = !authorizationManager.isAtLeastStudyAdministrator(organizationId, studyId, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.COHORT, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationUuid);
        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();
            try {
                if (checkPermissions) {
                    authorizationManager.checkCohortPermission(organizationId, study.getUid(), cohort.getUid(), userId,
                            CohortPermissions.DELETE);
                }
                OpenCGAResult tmpResult = getCohortDBAdaptor(organizationId).delete(cohort);
                result.append(tmpResult);

                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Cannot delete cohort {}: {}", cohort.getId(), e.getMessage());
                auditManager.auditDelete(organizationId, operationUuid, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationUuid);

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
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.BasicUpdateAction.ADD, options,
                token);
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
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.BasicUpdateAction.REMOVE, options,
                token);
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
        return updateAnnotations(studyStr, cohortStr, annotationSetId,
                new ObjectMap("remove", StringUtils.join(annotations, ",")), ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<Cohort> resetAnnotations(String studyStr, String cohortStr, String annotationSetId,
                                                  List<String> annotations, QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, cohortStr, annotationSetId,
                new ObjectMap("reset", StringUtils.join(annotations, ",")), ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    public OpenCGAResult<Cohort> update(String studyStr, String cohortId, CohortUpdateParams updateParams, QueryOptions options,
                                        String token) throws CatalogException {
        return update(studyStr, cohortId, updateParams, false, options, token);
    }

    public OpenCGAResult<Cohort> update(String studyStr, String cohortId, CohortUpdateParams updateParams,
                                        boolean allowModifyCohortAll, QueryOptions options, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, StudyManager.INCLUDE_VARIABLE_SET, userId, organizationId);

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
            OpenCGAResult<Cohort> internalResult = internalGet(organizationId, study.getUid(), cohortId, INCLUDE_COHORT_STATUS, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("Cohort '" + cohortId + "' not found");
            }
            Cohort cohort = internalResult.first();

            // We set the proper values for the audit
            cohortId = cohort.getId();
            cohortUuid = cohort.getUuid();

            OpenCGAResult<Cohort> updateResult = update(organizationId, study, cohort, updateParams, allowModifyCohortAll, options, userId);
            result.append(updateResult);

            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, cohortId, e.getMessage());
            result.getEvents().add(event);
            result.setNumErrors(result.getNumErrors() + 1);

            logger.error("Could not update cohort {}: {}", cohortId, e.getMessage(), e);
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.COHORT, cohortId, cohortUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        return result;
    }

    public OpenCGAResult<Cohort> update(String studyStr, List<String> cohortIds, CohortUpdateParams updateParams,
                                        QueryOptions options, String token) throws CatalogException {
        return update(studyStr, cohortIds, updateParams, false, false, options, token);
    }

    /**
     * Update a Cohort from catalog.
     *
     * @param studyStr             Study id in string format. Could be one of [studyId|projectId:studyId|organizationId@projectId:studyId].
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
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, StudyManager.INCLUDE_VARIABLE_SET, userId, organizationId);

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
                OpenCGAResult<Cohort> internalResult = internalGet(organizationId, study.getUid(), id, INCLUDE_COHORT_STATUS, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("Cohort '" + id + "' not found");
                }
                Cohort cohort = internalResult.first();

                // We set the proper values for the audit
                cohortId = cohort.getId();
                cohortUuid = cohort.getUuid();

                OpenCGAResult<Cohort> updateResult = update(organizationId, study, cohort, updateParams, allowModifyCohortAll, options,
                        userId);
                result.append(updateResult);

                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, cohortId, e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Could not update cohort {}: {}", cohortId, e.getMessage(), e);
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.COHORT, cohortId, cohortUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    /**
     * Update a Cohort from catalog.
     *
     * @param studyStr     Study id in string format. Could be one of
     *                     [id|organization@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy]
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
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, StudyManager.INCLUDE_VARIABLE_SET, userId, organizationId);

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
            AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, finalQuery);
            fixQueryObject(organizationId, study, finalQuery, userId);
            finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = getCohortDBAdaptor(organizationId).iterator(study.getUid(), finalQuery, INCLUDE_COHORT_STATUS, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.COHORT, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<Cohort> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();
            try {
                OpenCGAResult<Cohort> queryResult = update(organizationId, study, cohort, updateParams, allowModifyCohortAll, options,
                        userId);
                result.append(queryResult);

                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                result.getEvents().add(event);
                result.setNumErrors(result.getNumErrors() + 1);

                logger.error("Could not update cohort {}: {}", cohort.getId(), e.getMessage(), e);
                auditManager.auditUpdate(organizationId, operationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(organizationId, operationId);

        return endResult(result, ignoreException);
    }

    private OpenCGAResult<Cohort> update(String organizationId, Study study, Cohort cohort, CohortUpdateParams updateParams,
                                         boolean allowModifyCohortAll, QueryOptions options, String userId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        if (StringUtils.isNotEmpty(updateParams.getCreationDate())) {
            ParamUtils.checkDateFormat(updateParams.getCreationDate(), CohortDBAdaptor.QueryParams.CREATION_DATE.key());
        }
        if (StringUtils.isNotEmpty(updateParams.getModificationDate())) {
            ParamUtils.checkDateFormat(updateParams.getModificationDate(), CohortDBAdaptor.QueryParams.MODIFICATION_DATE.key());
        }

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
            authorizationManager.checkCohortPermission(organizationId, study.getUid(), cohort.getUid(), userId,
                    CohortPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkCohortPermission(organizationId, study.getUid(), cohort.getUid(), userId,
                    CohortPermissions.WRITE);
        }

        if (!allowModifyCohortAll) {
            if (StudyEntry.DEFAULT_COHORT.equals(cohort.getId())) {
                throw new CatalogException("Unable to modify cohort " + StudyEntry.DEFAULT_COHORT);
            }
        }

        if (updateParams != null && (ListUtils.isNotEmpty(updateParams.getSamples()) || updateParams.getId() != null)) {
            switch (cohort.getInternal().getStatus().getId()) {
                case CohortStatus.CALCULATING:
                    throw new CatalogException("Unable to modify a cohort while it's in status \"" + CohortStatus.CALCULATING
                            + "\"");
                case CohortStatus.READY:
                    parameters.putIfAbsent(CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key(), new CohortStatus(CohortStatus.INVALID));
                    break;
                case CohortStatus.NONE:
                case CohortStatus.INVALID:
                    break;
                default:
                    break;
            }

            if (updateParams.getId() != null) {
                ParamUtils.checkIdentifier(updateParams.getId(), CohortDBAdaptor.QueryParams.ID.key());
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

                InternalGetDataResult<Sample> sampleResult = catalogManager.getSampleManager().internalGet(organizationId, study.getUid(),
                        new ArrayList<>(sampleIds), SampleManager.INCLUDE_SAMPLE_IDS, userId, false);

                if (sampleResult.getNumResults() != sampleIds.size()) {
                    throw new CatalogException("Could not find all the samples introduced. Update was not performed.");
                }

                // Override sample list of ids with sample list
                parameters.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleResult.getResults());
            }
        }

        checkUpdateAnnotations(organizationId, study, cohort, parameters, options, VariableSet.AnnotableDataModels.COHORT,
                getCohortDBAdaptor(organizationId), userId);
        OpenCGAResult<Cohort> update = getCohortDBAdaptor(organizationId).update(cohort.getUid(), parameters, study.getVariableSets(),
                options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated cohort
            OpenCGAResult<Cohort> result = getCohortDBAdaptor(organizationId).get(study.getUid(),
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

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(sessionId);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, userId, query, authorizationManager);
        AnnotationUtils.fixQueryOptionAnnotation(options);

        // Add study id to the query
        query.put(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        OpenCGAResult queryResult = getCohortDBAdaptor(organizationId).groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    public void setStatus(String studyStr, String cohortId, String status, String message, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyStr, userId, organizationId);

        CohortStatus cohortStatus = new CohortStatus(status, message);
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohortId", cohortId)
                .append(CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key(), cohortStatus)
                .append("token", token);
        Cohort cohort;
        try {
            cohort = internalGet(organizationId, study.getUid(), cohortId, INCLUDE_COHORT_IDS, userId).first();
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.COHORT, cohortId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        try {
            authorizationManager.checkCohortPermission(organizationId, study.getUid(), cohort.getUid(), userId, CohortPermissions.WRITE);

            if (status != null && !CohortStatus.isValid(status)) {
                throw new CatalogException("The status " + status + " is not valid cohort status.");
            }

            ObjectMap parameters = new ObjectMap();
            parameters.put(CohortDBAdaptor.QueryParams.INTERNAL_STATUS.key(), cohortStatus);

            getCohortDBAdaptor(organizationId).update(cohort.getUid(), parameters, new QueryOptions());

            auditManager.auditUpdate(organizationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            auditManager.auditUpdate(organizationId, userId, Enums.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(sessionId);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, organizationId);
        authorizationManager.checkStudyPermission(organizationId, study.getUid(), userId, StudyPermissions.Permissions.VIEW_COHORTS);

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(organizationId, study, userId, query, authorizationManager);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = getCohortDBAdaptor(organizationId).rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<AclEntryList<CohortPermissions>> getAcls(String studyId, List<String> cohortList, String member,
                                                                  boolean ignoreException, String token) throws CatalogException {
        return getAcls(studyId, cohortList,
                StringUtils.isNotEmpty(member) ? Collections.singletonList(member) : Collections.emptyList(),
                ignoreException, token);
    }

    public OpenCGAResult<AclEntryList<CohortPermissions>> getAcls(String studyId, List<String> cohortList, List<String> members,
                                                                  boolean ignoreException, String token) throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, userId, organizationId);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("cohortList", cohortList)
                .append("members", members)
                .append("ignoreException", ignoreException)
                .append("token", token);

        OpenCGAResult<AclEntryList<CohortPermissions>> cohortAcls = OpenCGAResult.empty();
        Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
        try {
            auditManager.initAuditBatch(operationId);
            InternalGetDataResult<Cohort> queryResult = internalGet(organizationId, study.getUid(), cohortList, INCLUDE_COHORT_IDS, userId,
                    ignoreException);

            if (queryResult.getMissing() != null) {
                missingMap = queryResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }

            List<Long> cohortUids = queryResult.getResults().stream().map(Cohort::getUid).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(members)) {
                cohortAcls = authorizationManager.getAcl(organizationId, study.getUid(), cohortUids, members, Enums.Resource.COHORT,
                        CohortPermissions.class, userId);
            } else {
                cohortAcls = authorizationManager.getAcl(organizationId, study.getUid(), cohortUids, Enums.Resource.COHORT,
                        CohortPermissions.class, userId);
            }

            // Include non-existing cohorts to the result list
            List<AclEntryList<CohortPermissions>> resultList = new ArrayList<>(cohortList.size());
            List<Event> eventList = new ArrayList<>(missingMap.size());
            int counter = 0;
            for (String cohortId : cohortList) {
                if (!missingMap.containsKey(cohortId)) {
                    Cohort cohort = queryResult.getResults().get(counter);
                    resultList.add(cohortAcls.getResults().get(counter));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.COHORT, cohort.getId(),
                            cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    counter++;
                } else {
                    resultList.add(new AclEntryList<>());
                    eventList.add(new Event(Event.Type.ERROR, cohortId, missingMap.get(cohortId).getErrorMsg()));
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.COHORT, cohortId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(cohortId).getErrorMsg())), new ObjectMap());
                }
            }
            for (int i = 0; i < queryResult.getResults().size(); i++) {
                cohortAcls.getResults().get(i).setId(queryResult.getResults().get(i).getId());
            }
            cohortAcls.setResults(resultList);
            cohortAcls.setEvents(eventList);
        } catch (CatalogException e) {
            for (String cohortId : cohortList) {
                auditManager.audit(organizationId, operationId, userId, Enums.Action.FETCH_ACLS, Enums.Resource.COHORT, cohortId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            if (!ignoreException) {
                throw e;
            } else {
                for (String cohortId : cohortList) {
                    Event event = new Event(Event.Type.ERROR, cohortId, e.getMessage());
                    cohortAcls.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0, Collections.emptyList(), 0));
                }
            }
        } finally {
            auditManager.finishAuditBatch(organizationId, operationId);
        }

        return cohortAcls;
    }

    public OpenCGAResult<AclEntryList<CohortPermissions>> updateAcl(String studyId, List<String> cohortStrList, String memberList,
                                                                    AclParams aclParams, ParamUtils.AclAction action, String token)
            throws CatalogException {
        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, tokenPayload);
        String organizationId = studyFqn.getOrganizationId();
        String userId = tokenPayload.getUserId(organizationId);
        Study study = studyManager.resolveId(studyId, StudyManager.INCLUDE_STUDY_UID, userId, organizationId);

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

            List<Cohort> cohortList = internalGet(organizationId, study.getUid(), cohortStrList, INCLUDE_COHORT_IDS, userId, false)
                    .getResults();

            authorizationManager.checkCanAssignOrSeePermissions(organizationId, study.getUid(), userId);

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(organizationId, study.getUid(), members);

            List<Long> cohortUids = cohortList.stream().map(Cohort::getUid).collect(Collectors.toList());

            AuthorizationManager.CatalogAclParams catalogAclParams = new AuthorizationManager.CatalogAclParams(cohortUids, permissions,
                    Enums.Resource.COHORT);

            OpenCGAResult<AclEntryList<CohortPermissions>> queryResultList;
            switch (action) {
                case SET:
                    authorizationManager.setAcls(organizationId, study.getUid(), members, catalogAclParams);
                    break;
                case ADD:
                    authorizationManager.addAcls(organizationId, study.getUid(), members, catalogAclParams);
                    break;
                case REMOVE:
                    authorizationManager.removeAcls(organizationId, members, catalogAclParams);
                    break;
                case RESET:
                    catalogAclParams.setPermissions(null);
                    authorizationManager.removeAcls(organizationId, members, catalogAclParams);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            queryResultList = authorizationManager.getAcls(organizationId, study.getUid(), cohortUids, members, Enums.Resource.COHORT,
                    CohortPermissions.class);
            for (int i = 0; i < queryResultList.getResults().size(); i++) {
                queryResultList.getResults().get(i).setId(cohortList.get(i).getId());
            }
            for (Cohort cohort : cohortList) {
                auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.COHORT, cohort.getId(),
                        cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (CatalogException e) {
            if (cohortStrList != null) {
                for (String cohortId : cohortStrList) {
                    auditManager.audit(organizationId, operationId, userId, Enums.Action.UPDATE_ACLS, Enums.Resource.COHORT, cohortId, "",
                            study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            throw e;
        } finally {
            auditManager.finishAuditBatch(organizationId, operationId);
        }
    }

}
