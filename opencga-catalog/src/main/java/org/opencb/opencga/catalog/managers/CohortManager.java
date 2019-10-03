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
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.models.update.CohortUpdateParams;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.CohortAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
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

    CohortManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                  Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    AuditRecord.Resource getEntity() {
        return AuditRecord.Resource.COHORT;
    }

    @Override
    DataResult<Cohort> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        ParamUtils.checkIsSingleID(entry);

        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            queryCopy.put(CohortDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(CohortDBAdaptor.QueryParams.ID.key(), entry);
        }
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();

        DataResult<Cohort> cohortDataResult = cohortDBAdaptor.get(queryCopy, queryOptions, user);
        if (cohortDataResult.getNumResults() == 0) {
            cohortDataResult = cohortDBAdaptor.get(queryCopy, queryOptions);
            if (cohortDataResult.getNumResults() == 0) {
                throw new CatalogException("Cohort " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the cohort " + entry);
            }
        } else if (cohortDataResult.getNumResults() > 1) {
            throw new CatalogException("More than one cohort found based on " + entry);
        } else {
            return cohortDataResult;
        }
    }

    @Override
    InternalGetDataResult<Cohort> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                               String user, boolean silent) throws CatalogException {
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
            if (UUIDUtils.isOpenCGAUUID(entry)) {
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

        DataResult<Cohort> cohortDataResult = cohortDBAdaptor.get(queryCopy, queryOptions, user);

        if (silent || cohortDataResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, cohortStringFunction, cohortDataResult, silent, false);
        }
        // Query without adding the user check
        DataResult<Cohort> resultsNoCheck = cohortDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == cohortDataResult.getNumResults()) {
            throw CatalogException.notFound("cohorts",
                    getMissingFields(uniqueList, cohortDataResult.getResults(), cohortStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the cohorts.");
        }
    }

    private DataResult<Cohort> getCohort(long studyUid, String cohortUuid, QueryOptions options) throws CatalogDBException {
        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(CohortDBAdaptor.QueryParams.UUID.key(), cohortUuid);
        return cohortDBAdaptor.get(query, options);
    }

    @Deprecated
    public DataResult<Cohort> create(long studyId, String name, Study.Type type, String description, List<Sample> samples,
                                      List<AnnotationSet> annotationSetList, Map<String, Object> attributes, String sessionId)
            throws CatalogException {
        return create(String.valueOf(studyId), name, type, description, samples, annotationSetList, attributes, sessionId);
    }

    public DataResult<Cohort> create(String studyId, String name, Study.Type type, String description, List<Sample> samples,
                                      List<AnnotationSet> annotationSetList, Map<String, Object> attributes, String sessionId)
            throws CatalogException {
        Cohort cohort = new Cohort(name, type, "", description, samples, annotationSetList, -1, attributes);
        return create(studyId, cohort, QueryOptions.empty(), sessionId);
    }

    @Override
    public DataResult<Cohort> create(String studyStr, Cohort cohort, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohort", cohort)
                .append("options", options)
                .append("token", token);
        try {
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_COHORTS);
            validateNewCohort(study, cohort, userId);

            cohortDBAdaptor.insert(study.getUid(), cohort, study.getVariableSets(), options);
            DataResult<Cohort> queryResult = getCohort(study.getUid(), cohort.getUuid(), options);

            auditManager.auditCreate(userId, AuditRecord.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return  queryResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, AuditRecord.Resource.COHORT, cohort.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    void validateNewCohort(Study study, Cohort cohort, String userId) throws CatalogException {
        ParamUtils.checkObj(cohort, "Cohort");
        ParamUtils.checkParameter(cohort.getId(), "id");
        ParamUtils.checkObj(cohort.getSamples(), "Sample list");
        cohort.setType(ParamUtils.defaultObject(cohort.getType(), Study.Type.COLLECTION));
        cohort.setCreationDate(TimeUtils.getTime());
        cohort.setDescription(ParamUtils.defaultString(cohort.getDescription(), ""));
        cohort.setAnnotationSets(ParamUtils.defaultObject(cohort.getAnnotationSets(), Collections::emptyList));
        cohort.setAttributes(ParamUtils.defaultObject(cohort.getAttributes(), HashMap::new));
        cohort.setRelease(studyManager.getCurrentRelease(study));
        cohort.setStats(ParamUtils.defaultObject(cohort.getStats(), Collections::emptyMap));
        cohort.setStatus(ParamUtils.defaultObject(cohort.getStatus(), Cohort.CohortStatus::new));
        cohort.setSamples(ParamUtils.defaultObject(cohort.getSamples(), Collections::emptyList));
        cohort.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.COHORT));

        validateNewAnnotationSets(study.getVariableSets(), cohort.getAnnotationSets());

        if (!cohort.getSamples().isEmpty()) {
            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(SampleDBAdaptor.QueryParams.UID.key(), cohort.getSamples().stream()
                            .map(Sample::getUid)
                            .collect(Collectors.toList()));
            DataResult<Long> count = sampleDBAdaptor.count(query);
            if (count.first() != cohort.getSamples().size()) {
                throw new CatalogException("Error: Some samples do not exist in the study " + study.getFqn());
            }
        }
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
     * @return a DataResult containing all the samples belonging to the cohort.
     * @throws CatalogException if there is any kind of error (permissions or invalid ids).
     */
    public DataResult<Sample> getSamples(String studyStr, String cohortStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        DataResult<Cohort> cohortDataResult = internalGet(study.getUid(), cohortStr,
                new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key()), userId);

        if (cohortDataResult == null || cohortDataResult.getNumResults() == 0) {
            throw new CatalogException("No cohort " + cohortStr + " found in study " + studyStr);
        }
        if (cohortDataResult.first().getSamples().size() == 0) {
            return DataResult.empty();
        }

        return new DataResult<>(cohortDataResult.getTime(), cohortDataResult.getEvents(), cohortDataResult.first().getSamples().size(),
                cohortDataResult.first().getSamples(), cohortDataResult.first().getSamples().size());
    }

    @Override
    public DBIterator<Cohort> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        fixQueryObject(study, query, userId);

        Query myQuery = new Query(query).append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        return cohortDBAdaptor.iterator(myQuery, options, userId);
    }

    @Override
    public DataResult<Cohort> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
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
            DataResult<Cohort> queryResult = cohortDBAdaptor.get(query, options, userId);

            auditManager.auditSearch(userId, AuditRecord.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, AuditRecord.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
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
    public DataResult<Cohort> count(String studyId, Query query, String token) throws CatalogException {
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
            DataResult<Long> queryResultAux = cohortDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_COHORTS);

            auditManager.auditCount(userId, AuditRecord.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new DataResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.first());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, AuditRecord.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public DataResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        DataResult result = DataResult.empty();

        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("params", params)
                .append("token", token);

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the cohorts to be deleted
        DBIterator<Cohort> iterator;
        try {
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = cohortDBAdaptor.iterator(finalQuery, INCLUDE_COHORT_IDS, userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, AuditRecord.Resource.COHORT, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();

            try {
                if (checkPermissions) {
                    authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId,
                            CohortAclEntry.CohortPermissions.DELETE);
                }

                DataResult tmpDataResult = cohortDBAdaptor.delete(cohort);
                result.append(tmpDataResult);

                auditManager.auditDelete(operationUuid, userId, AuditRecord.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, cohort.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot delete cohort {}: {}", cohort.getId(), e.getMessage());
                auditManager.auditDelete(operationUuid, userId, AuditRecord.Resource.COHORT, cohort.getId(), cohort.getUuid(),
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }

        return result;
    }

    public DataResult<Cohort> updateAnnotationSet(String studyStr, String cohortStr, List<AnnotationSet> annotationSetList,
                                                   ParamUtils.UpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        CohortUpdateParams updateParams = new CohortUpdateParams().setAnnotationSets(annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, cohortStr, updateParams, options, token);
    }

    public DataResult<Cohort> addAnnotationSet(String studyStr, String cohortStr, AnnotationSet annotationSet, QueryOptions options,
                                                String token) throws CatalogException {
        return addAnnotationSets(studyStr, cohortStr, Collections.singletonList(annotationSet), options, token);
    }

    public DataResult<Cohort> addAnnotationSets(String studyStr, String cohortStr, List<AnnotationSet> annotationSetList,
                                                 QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.UpdateAction.ADD, options, token);
    }

    public DataResult<Cohort> setAnnotationSet(String studyStr, String cohortStr, AnnotationSet annotationSet, QueryOptions options,
                                                String token) throws CatalogException {
        return setAnnotationSets(studyStr, cohortStr, Collections.singletonList(annotationSet), options, token);
    }

    public DataResult<Cohort> setAnnotationSets(String studyStr, String cohortStr, List<AnnotationSet> annotationSetList,
                                                 QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.UpdateAction.SET, options, token);
    }

    public DataResult<Cohort> removeAnnotationSet(String studyStr, String cohortStr, String annotationSetId, QueryOptions options,
                                                   String token) throws CatalogException {
        return removeAnnotationSets(studyStr, cohortStr, Collections.singletonList(annotationSetId), options, token);
    }

    public DataResult<Cohort> removeAnnotationSets(String studyStr, String cohortStr, List<String> annotationSetIdList,
                                                    QueryOptions options, String token) throws CatalogException {
        List<AnnotationSet> annotationSetList = annotationSetIdList
                .stream()
                .map(id -> new AnnotationSet().setId(id))
                .collect(Collectors.toList());
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.UpdateAction.REMOVE, options, token);
    }

    public DataResult<Cohort> updateAnnotations(String studyStr, String cohortStr, String annotationSetId,
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

    public DataResult<Cohort> removeAnnotations(String studyStr, String cohortStr, String annotationSetId,
                                                 List<String> annotations, QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, cohortStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public DataResult<Cohort> resetAnnotations(String studyStr, String cohortStr, String annotationSetId, List<String> annotations,
                                                QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, cohortStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    public DataResult<Cohort> update(String studyStr, String cohortId, CohortUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, cohortId, updateParams, false, options, token);
    }

    /**
     * Update a Cohort from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param cohortId   Cohort id in string format. Could be either the id or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param allowModifyCohortAll Boolean indicating whether we should not raise an exception if the cohort ALL is to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A DataResult with the object updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public DataResult<Cohort> update(String studyStr, String cohortId, CohortUpdateParams updateParams, boolean allowModifyCohortAll,
                                      QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("cohortId", cohortId)
                .append("updateParams", updateParams != null ? updateParams.getUpdateMap() : null)
                .append("allowModifyCohortAll", allowModifyCohortAll)
                .append("options", options)
                .append("token", token);
        Cohort cohort;
        try {
            cohort = internalGet(study.getUid(), cohortId, QueryOptions.empty(), userId).first();
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, AuditRecord.Resource.COHORT, cohortId, "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);

            ObjectMap parameters = new ObjectMap();
            if (updateParams != null) {
                parameters = updateParams.getUpdateMap();
            }
            ParamUtils.checkUpdateParametersMap(parameters);

            if (parameters.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
                Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
                if (!actionMap.containsKey(AnnotationSetManager.ANNOTATION_SETS)
                        && !actionMap.containsKey(AnnotationSetManager.ANNOTATIONS)) {
                    logger.warn("Assuming the user wants to add the list of annotation sets provided");
                    actionMap.put(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.UpdateAction.ADD);
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
                        CohortAclEntry.CohortPermissions.UPDATE);
            }

            if (!allowModifyCohortAll) {
                if (StudyEntry.DEFAULT_COHORT.equals(cohort.getId())) {
                    throw new CatalogException("Unable to modify cohort " + StudyEntry.DEFAULT_COHORT);
                }
            }

            if (updateParams != null && (ListUtils.isNotEmpty(updateParams.getSamples()) || StringUtils.isNotEmpty(updateParams.getId()))) {
                switch (cohort.getStatus().getName()) {
                    case Cohort.CohortStatus.CALCULATING:
                        throw new CatalogException("Unable to modify a cohort while it's in status \"" + Cohort.CohortStatus.CALCULATING
                                + "\"");
                    case Cohort.CohortStatus.READY:
                        parameters.putIfAbsent(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), Cohort.CohortStatus.INVALID);
                        break;
                    case Cohort.CohortStatus.NONE:
                    case Cohort.CohortStatus.INVALID:
                        break;
                    default:
                        break;
                }

                if (ListUtils.isNotEmpty(updateParams.getSamples())) {
                    InternalGetDataResult<Sample> sampleResult = catalogManager.getSampleManager().internalGet(study.getUid(),
                            updateParams.getSamples(), SampleManager.INCLUDE_SAMPLE_IDS, userId, false);

                    if (sampleResult.getNumResults() != updateParams.getSamples().size()) {
                        throw new CatalogException("Could not find all the samples introduced. Update was not performed.");
                    }

                    // Override sample list of ids with sample list
                    parameters.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleResult.getResults());
                    options.put(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                            ParamUtils.UpdateAction.SET.name()));
                }
            }

            checkUpdateAnnotations(study, cohort, parameters, options, VariableSet.AnnotableDataModels.COHORT, cohortDBAdaptor, userId);

            DataResult result = cohortDBAdaptor.update(cohort.getUid(), parameters, study.getVariableSets(), options);

            auditManager.auditUpdate(userId, AuditRecord.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            DataResult<Cohort> queryResult = cohortDBAdaptor.get(cohort.getUid(),
                    new QueryOptions(QueryOptions.INCLUDE, parameters.keySet()));
            queryResult.setTime(result.getTime() + queryResult.getTime());

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, AuditRecord.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public DataResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
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

        DataResult queryResult = cohortDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, DataResult::new);
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
            auditManager.auditUpdate(userId, AuditRecord.Resource.COHORT, cohortId, "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        try {
            authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId, CohortAclEntry.CohortPermissions.UPDATE);

            if (status != null && !Cohort.CohortStatus.isValid(status)) {
                throw new CatalogException("The status " + status + " is not valid cohort status.");
            }

            ObjectMap parameters = new ObjectMap();
            parameters.putIfNotNull(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), status);
            parameters.putIfNotNull(CohortDBAdaptor.QueryParams.STATUS_MSG.key(), message);

            cohortDBAdaptor.update(cohort.getUid(), parameters, new QueryOptions());

            auditManager.auditUpdate(userId, AuditRecord.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, AuditRecord.Resource.COHORT, cohort.getId(), cohort.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public DataResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
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
        DataResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = cohortDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, DataResult::new);
    }

    // **************************   ACLs  ******************************** //
    public List<DataResult<CohortAclEntry>> getAcls(String studyId, List<String> cohortList, String member, boolean silent, String token)
            throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("cohortList", cohortList)
                .append("member", member)
                .append("silent", silent)
                .append("token", token);
        try {
            List<DataResult<CohortAclEntry>> cohortAclList = new ArrayList<>(cohortList.size());

            InternalGetDataResult<Cohort> cohortDataResult = internalGet(study.getUid(), cohortList, INCLUDE_COHORT_IDS, user, silent);

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
                        DataResult<CohortAclEntry> allCohortAcls;
                        if (StringUtils.isNotEmpty(member)) {
                            allCohortAcls = authorizationManager.getCohortAcl(study.getUid(), cohort.getUid(), user, member);
                        } else {
                            allCohortAcls = authorizationManager.getAllCohortAcls(study.getUid(), cohort.getUid(), user);
                        }
                        cohortAclList.add(allCohortAcls);

                        auditManager.audit(operationId, user, AuditRecord.Action.FETCH_ACLS, AuditRecord.Resource.COHORT, cohort.getId(),
                                cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    } catch (CatalogException e) {
                        auditManager.audit(operationId, user, AuditRecord.Action.FETCH_ACLS, AuditRecord.Resource.COHORT, cohort.getId(),
                                cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());

                        if (!silent) {
                            throw e;
                        } else {
                            Event event = new Event(Event.Type.ERROR, cohortId, missingMap.get(cohortId).getErrorMsg());
                            cohortAclList.add(new DataResult<>(cohortDataResult.getTime(), Collections.singletonList(event), 0,
                                    Collections.emptyList(), 0));
                        }
                    }
                    counter += 1;
                } else {
                    Event event = new Event(Event.Type.ERROR, cohortId, missingMap.get(cohortId).getErrorMsg());
                    cohortAclList.add(new DataResult<>(cohortDataResult.getTime(), Collections.singletonList(event), 0,
                            Collections.emptyList(), 0));

                    auditManager.audit(operationId, user, AuditRecord.Action.FETCH_ACLS, AuditRecord.Resource.COHORT, cohortId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(cohortId).getErrorMsg())), new ObjectMap());
                }
            }
            return cohortAclList;
        } catch (CatalogException e) {
            for (String cohortId : cohortList) {
                auditManager.audit(operationId, user, AuditRecord.Action.FETCH_ACLS, AuditRecord.Resource.COHORT, cohortId, "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            throw e;
        }
    }

    public List<DataResult<CohortAclEntry>> updateAcl(String studyId, List<String> cohortStrList, String memberList, AclParams aclParams,
                                                       String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, StudyManager.INCLUDE_STUDY_UID);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("cohortStrList", cohortStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("token", token);
        String operationId = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        try {
            if (cohortStrList == null || cohortStrList.isEmpty()) {
                throw new CatalogException("Missing cohort parameter");
            }

            if (aclParams.getAction() == null) {
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

            List<DataResult<CohortAclEntry>> queryResultList;
            switch (aclParams.getAction()) {
                case SET:
                    queryResultList = authorizationManager.setAcls(study.getUid(), cohortUids, members, permissions, Entity.COHORT);
                    break;
                case ADD:
                    queryResultList = authorizationManager.addAcls(study.getUid(), cohortUids, members, permissions, Entity.COHORT);
                    break;
                case REMOVE:
                    queryResultList = authorizationManager.removeAcls(cohortUids, members, permissions, Entity.COHORT);
                    break;
                case RESET:
                    queryResultList = authorizationManager.removeAcls(cohortUids, members, null, Entity.COHORT);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }

            for (Cohort cohort : cohortList) {
                auditManager.audit(operationId, userId, AuditRecord.Action.UPDATE_ACLS, AuditRecord.Resource.COHORT, cohort.getId(),
                        cohort.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (CatalogException e) {
            if (cohortStrList != null) {
                for (String cohortId : cohortStrList) {
                    auditManager.audit(operationId, userId, AuditRecord.Action.UPDATE_ACLS, AuditRecord.Resource.COHORT, cohortId, "",
                            study.getId(), study.getUuid(), auditParams,
                            new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());
                }
            }
            throw e;
        }
    }

    public FacetQueryResult facet(String studyId, Query query, QueryOptions options, boolean defaultStats, String token)
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

            CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager);
            FacetQueryResult result = catalogSolrManager.facetedQuery(study, CatalogSolrManager.COHORT_SOLR_COLLECTION, query, options,
                    userId);
            auditManager.auditFacet(userId, AuditRecord.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException | IOException e) {
            auditManager.auditFacet(userId, AuditRecord.Resource.COHORT, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage())));
            throw e;
        }
    }
}
