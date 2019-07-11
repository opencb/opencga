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
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetQueryResult;
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
            CohortDBAdaptor.QueryParams.ID.key(), CohortDBAdaptor.QueryParams.UID.key(), CohortDBAdaptor.QueryParams.UUID.key()));

    CohortManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                  DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                  Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    QueryResult<Cohort> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
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

        QueryResult<Cohort> cohortQueryResult = cohortDBAdaptor.get(queryCopy, queryOptions, user);
        if (cohortQueryResult.getNumResults() == 0) {
            cohortQueryResult = cohortDBAdaptor.get(queryCopy, queryOptions);
            if (cohortQueryResult.getNumResults() == 0) {
                throw new CatalogException("Cohort " + entry + " not found");
            } else {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the cohort " + entry);
            }
        } else if (cohortQueryResult.getNumResults() > 1) {
            throw new CatalogException("More than one cohort found based on " + entry);
        } else {
            return cohortQueryResult;
        }
    }

    @Override
    InternalGetQueryResult<Cohort> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
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

        QueryResult<Cohort> cohortQueryResult = cohortDBAdaptor.get(queryCopy, queryOptions, user);

        if (silent || cohortQueryResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, cohortStringFunction, cohortQueryResult, silent, false);
        }
        // Query without adding the user check
        QueryResult<Cohort> resultsNoCheck = cohortDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == cohortQueryResult.getNumResults()) {
            throw CatalogException.notFound("cohorts",
                    getMissingFields(uniqueList, cohortQueryResult.getResult(), cohortStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the cohorts.");
        }
    }

    @Deprecated
    public QueryResult<Cohort> create(long studyId, String name, Study.Type type, String description, List<Sample> samples,
                                      List<AnnotationSet> annotationSetList, Map<String, Object> attributes, String sessionId)
            throws CatalogException {
        return create(String.valueOf(studyId), name, type, description, samples, annotationSetList, attributes, sessionId);
    }

    public QueryResult<Cohort> create(String studyId, String name, Study.Type type, String description, List<Sample> samples,
                                      List<AnnotationSet> annotationSetList, Map<String, Object> attributes, String sessionId)
            throws CatalogException {
        Cohort cohort = new Cohort(name, type, "", description, samples, annotationSetList, -1, attributes);
        return create(studyId, cohort, QueryOptions.empty(), sessionId);
    }

    @Override
    public QueryResult<Cohort> create(String studyStr, Cohort cohort, QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_COHORTS);

        ParamUtils.checkObj(cohort, "Cohort");
        ParamUtils.checkParameter(cohort.getId(), "name");
        ParamUtils.checkObj(cohort.getSamples(), "Sample list");
        cohort.setType(ParamUtils.defaultObject(cohort.getType(), Study.Type.COLLECTION));
        cohort.setCreationDate(TimeUtils.getTime());
        cohort.setDescription(ParamUtils.defaultString(cohort.getDescription(), ""));
        cohort.setAnnotationSets(ParamUtils.defaultObject(cohort.getAnnotationSets(), Collections::emptyList));
        cohort.setAttributes(ParamUtils.defaultObject(cohort.getAttributes(), HashMap::new));
        cohort.setRelease(studyManager.getCurrentRelease(study, userId));
        cohort.setStats(ParamUtils.defaultObject(cohort.getStats(), Collections::emptyMap));
        cohort.setStatus(ParamUtils.defaultObject(cohort.getStatus(), Cohort.CohortStatus::new));
        cohort.setSamples(ParamUtils.defaultObject(cohort.getSamples(), Collections::emptyList));

        List<VariableSet> variableSetList = validateNewAnnotationSetsAndExtractVariableSets(study.getUid(), cohort.getAnnotationSets());

        if (!cohort.getSamples().isEmpty()) {
            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(SampleDBAdaptor.QueryParams.UID.key(), cohort.getSamples().stream()
                            .map(Sample::getUid)
                            .collect(Collectors.toList()));
            QueryResult<Long> count = sampleDBAdaptor.count(query);
            if (count.first() != cohort.getSamples().size()) {
                throw new CatalogException("Error: Some samples do not exist in the study " + study.getFqn());
            }
        }

        cohort.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.COHORT));
        QueryResult<Cohort> queryResult = cohortDBAdaptor.insert(study.getUid(), cohort, variableSetList, null);
        auditManager.recordCreation(AuditRecord.Resource.cohort, queryResult.first().getUid(), userId, queryResult.first(), null, null);
        return queryResult;
    }

    public Long getStudyId(long cohortId) throws CatalogException {
        return cohortDBAdaptor.getStudyId(cohortId);
    }

    @Override
    public QueryResult<Cohort> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        fixQueryObject(study, query, userId);

        query.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult<Cohort> cohortQueryResult = cohortDBAdaptor.get(query, options, userId);

        if (cohortQueryResult.getNumResults() == 0 && query.containsKey(CohortDBAdaptor.QueryParams.UID.key())) {
            List<Long> idList = query.getAsLongList(CohortDBAdaptor.QueryParams.UID.key());
            for (Long myId : idList) {
                authorizationManager.checkCohortPermission(study.getUid(), myId, userId, CohortAclEntry.CohortPermissions.VIEW);
            }
        }

        return cohortQueryResult;
    }

    /**
     * Fetch all the samples from a cohort.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param cohortStr Cohort id or name.
     * @param sessionId Token of the user logged in.
     * @return a QueryResult containing all the samples belonging to the cohort.
     * @throws CatalogException if there is any kind of error (permissions or invalid ids).
     */
    public QueryResult<Sample> getSamples(String studyStr, String cohortStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        QueryResult<Cohort> cohortQueryResult = internalGet(study.getUid(), cohortStr,
                new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key()), userId);

        if (cohortQueryResult == null || cohortQueryResult.getNumResults() == 0) {
            throw new CatalogException("No cohort " + cohortStr + " found in study " + studyStr);
        }
        if (cohortQueryResult.first().getSamples().size() == 0) {
            return new QueryResult<>("Samples from cohort " + cohortStr);
        }

        return new QueryResult<>("Samples from cohort " + cohortStr,
                cohortQueryResult.getDbTime(), cohortQueryResult.first().getSamples().size(),
                cohortQueryResult.first().getSamples().size(), cohortQueryResult.getWarningMsg(), cohortQueryResult.getErrorMsg(),
                cohortQueryResult.first().getSamples());
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
    public QueryResult<Cohort> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        fixQueryObject(study, query, userId);

        query.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Cohort> queryResult = cohortDBAdaptor.get(query, options, userId);
//        authorizationManager.filterCohorts(userId, studyId, queryResultAux.getResult());

        return queryResult;
    }

    private void fixQueryObject(Study study, Query query, String userId) throws CatalogException {
        if (query.containsKey(CohortDBAdaptor.QueryParams.SAMPLES.key())) {
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.UID.key());

            // First look for the sample ids.
            List<Sample> sampleList = catalogManager.getSampleManager().internalGet(study.getUid(),
                    query.getAsStringList(CohortDBAdaptor.QueryParams.SAMPLES.key()), SampleManager.INCLUDE_SAMPLE_IDS, userId, true)
                    .getResult();
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
    public QueryResult<Cohort> count(String studyStr, Query query, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));


        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        fixQueryObject(study, query, userId);

        query.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Long> queryResultAux = cohortDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_COHORTS);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        WriteResult writeResult = new WriteResult("delete");

        String userId;
        Study study;

        StopWatch watch = StopWatch.createStarted();

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the cohorts to be deleted
        DBIterator<Cohort> iterator;
        try {
            userId = catalogManager.getUserManager().getUserId(sessionId);
            study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                    StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = cohortDBAdaptor.iterator(finalQuery, QueryOptions.empty(), userId);

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error("Delete cohort: {}", e.getMessage(), e);
            writeResult.setError(new Error(-1, null, e.getMessage()));
            writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
            return writeResult;
        }

        long numMatches = 0;
        long numModified = 0;
        List<WriteResult.Fail> failedList = new ArrayList<>();

        String suffixName = INTERNAL_DELIMITER + "DELETED_" + TimeUtils.getTime();

        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();
            numMatches += 1;

            try {
                if (checkPermissions) {
                    authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId,
                            CohortAclEntry.CohortPermissions.DELETE);
                }

                // Check if the cohort can be deleted
                checkCohortCanBeDeleted(cohort);

                // Delete the cohort
                Query updateQuery = new Query()
                        .append(CohortDBAdaptor.QueryParams.UID.key(), cohort.getUid())
                        .append(CohortDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
                ObjectMap updateParams = new ObjectMap()
                        .append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED)
                        .append(CohortDBAdaptor.QueryParams.ID.key(), cohort.getId() + suffixName);
                WriteResult update = cohortDBAdaptor.update(updateQuery, updateParams, QueryOptions.empty());
                if (update.getNumModified() > 0) {
                    numModified += 1;
                    auditManager.recordDeletion(AuditRecord.Resource.cohort, cohort.getUid(), userId, null, updateParams, null, null);
                } else {
                    failedList.add(new WriteResult.Fail(cohort.getId(), "Unknown reason"));
                }
            } catch (Exception e) {
                failedList.add(new WriteResult.Fail(cohort.getId(), e.getMessage()));
                logger.debug("Cannot delete cohort {}: {}", cohort.getId(), e.getMessage(), e);
            }
        }

        writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
        writeResult.setNumMatches(numMatches);
        writeResult.setNumModified(numModified);
        writeResult.setFailed(failedList);

        if (!failedList.isEmpty()) {
            writeResult.setWarning(Collections.singletonList(new Error(-1, null, "There are cohorts that could not be deleted")));
        }

        return writeResult;
    }

    public void checkCohortCanBeDeleted(Cohort cohort) throws CatalogException {
        // Check if the cohort is different from DEFAULT_COHORT
        if (StudyEntry.DEFAULT_COHORT.equals(cohort.getId())) {
            throw new CatalogException("Cohort " + StudyEntry.DEFAULT_COHORT + " cannot be deleted.");
        }

        // Check if the cohort can be deleted
        if (cohort.getStatus() != null && cohort.getStatus().getName() != null
                && !cohort.getStatus().getName().equals(Cohort.CohortStatus.NONE)) {
            throw new CatalogException("The cohort is used in storage.");
        }
    }

    public QueryResult<Cohort> updateAnnotationSet(String studyStr, String cohortStr, List<AnnotationSet> annotationSetList,
                                                   ParamUtils.UpdateAction action, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap params = new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, annotationSetList);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, action));

        return update(studyStr, cohortStr, params, options, token);
    }

    public QueryResult<Cohort> addAnnotationSet(String studyStr, String cohortStr, AnnotationSet annotationSet, QueryOptions options,
                                                String token) throws CatalogException {
        return addAnnotationSets(studyStr, cohortStr, Collections.singletonList(annotationSet), options, token);
    }

    public QueryResult<Cohort> addAnnotationSets(String studyStr, String cohortStr, List<AnnotationSet> annotationSetList,
                                                 QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.UpdateAction.ADD, options, token);
    }

    public QueryResult<Cohort> setAnnotationSet(String studyStr, String cohortStr, AnnotationSet annotationSet, QueryOptions options,
                                                String token) throws CatalogException {
        return setAnnotationSets(studyStr, cohortStr, Collections.singletonList(annotationSet), options, token);
    }

    public QueryResult<Cohort> setAnnotationSets(String studyStr, String cohortStr, List<AnnotationSet> annotationSetList,
                                                 QueryOptions options, String token) throws CatalogException {
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.UpdateAction.SET, options, token);
    }

    public QueryResult<Cohort> removeAnnotationSet(String studyStr, String cohortStr, String annotationSetId, QueryOptions options,
                                                   String token) throws CatalogException {
        return removeAnnotationSets(studyStr, cohortStr, Collections.singletonList(annotationSetId), options, token);
    }

    public QueryResult<Cohort> removeAnnotationSets(String studyStr, String cohortStr, List<String> annotationSetIdList,
                                                    QueryOptions options, String token) throws CatalogException {
        List<AnnotationSet> annotationSetList = annotationSetIdList
                .stream()
                .map(id -> new AnnotationSet().setId(id))
                .collect(Collectors.toList());
        return updateAnnotationSet(studyStr, cohortStr, annotationSetList, ParamUtils.UpdateAction.REMOVE, options, token);
    }

    public QueryResult<Cohort> updateAnnotations(String studyStr, String cohortStr, String annotationSetId,
                                                     Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                                     QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            return new QueryResult<>(cohortStr, -1, -1, -1, "Nothing to do: The map of annotations is empty", "", Collections.emptyList());
        }
        ObjectMap params = new ObjectMap(AnnotationSetManager.ANNOTATIONS, new AnnotationSet(annotationSetId, "", annotations));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, cohortStr, params, options, token);
    }

    public QueryResult<Cohort> removeAnnotations(String studyStr, String cohortStr, String annotationSetId,
                                                 List<String> annotations, QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, cohortStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public QueryResult<Cohort> resetAnnotations(String studyStr, String cohortStr, String annotationSetId, List<String> annotations,
                                                QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, cohortStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    @Override
    public QueryResult<Cohort> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, entryStr, parameters, false, options, token);
    }

    public QueryResult<Cohort> update(String studyStr, String entryStr, ObjectMap parameters, boolean allowModifyCohortAll,
                                      QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(parameters, "Update parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        parameters = new ObjectMap(parameters);

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);
        Cohort cohort = internalGet(study.getUid(), entryStr, QueryOptions.empty(), userId).first();

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (parameters.containsKey(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId,
                    CohortAclEntry.CohortPermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId, CohortAclEntry.CohortPermissions.UPDATE);
        }

        return unsafeUpdate(study, cohort, parameters, allowModifyCohortAll, options, userId);
    }

    QueryResult<Cohort> unsafeUpdate(Study study, Cohort cohort, ObjectMap parameters, boolean allowModifyCohortAll, QueryOptions options,
                                     String user) throws CatalogException {
        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> CohortDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }

        if (!allowModifyCohortAll) {
            if (StudyEntry.DEFAULT_COHORT.equals(cohort.getId())) {
                throw new CatalogException("Cannot modify cohort " + StudyEntry.DEFAULT_COHORT);
            }
        }

        if (parameters.containsKey(CohortDBAdaptor.QueryParams.SAMPLES.key())
                || parameters.containsKey(CohortDBAdaptor.QueryParams.ID.key())) {
            switch (cohort.getStatus().getName()) {
                case Cohort.CohortStatus.CALCULATING:
                    throw new CatalogException("Unable to modify a cohort while it's in status \"" + Cohort.CohortStatus.CALCULATING
                            + "\"");
                case Cohort.CohortStatus.READY:
                    parameters.putIfAbsent("status.name", Cohort.CohortStatus.INVALID);
                    break;
                case Cohort.CohortStatus.NONE:
                case Cohort.CohortStatus.INVALID:
                    break;
                default:
                    break;
            }

            List<String> sampleStringList = parameters.getAsStringList(CohortDBAdaptor.QueryParams.SAMPLES.key());
            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(SampleDBAdaptor.QueryParams.ID.key(), sampleStringList);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.UID.key());
            QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.get(query, queryOptions);

            if (sampleQueryResult.getNumResults() != sampleStringList.size()) {
                throw new CatalogException("Could not find all the samples introduced. Update was not performed.");
            }

            // Override sample list of ids with sample uids
            parameters.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), sampleQueryResult.getResult().stream()
                    .map(Sample::getUid)
                    .collect(Collectors.toList()));
        }

        List<VariableSet> variableSetList = checkUpdateAnnotationsAndExtractVariableSets(study, cohort, parameters, options,
                VariableSet.AnnotableDataModels.COHORT, cohortDBAdaptor, user);

        QueryResult<Cohort> queryResult = cohortDBAdaptor.update(cohort.getUid(), parameters, variableSetList, options);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohort.getUid(), user, parameters, null, null);
        return queryResult;
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
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

        QueryResult queryResult = cohortDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    public void setStatus(String studyStr, String id, String status, String message, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);
        Cohort cohort = internalGet(study.getUid(), id, INCLUDE_COHORT_IDS, userId).first();

        authorizationManager.checkCohortPermission(study.getUid(), cohort.getUid(), userId, CohortAclEntry.CohortPermissions.UPDATE);

        if (status != null && !Cohort.CohortStatus.isValid(status)) {
            throw new CatalogException("The status " + status + " is not valid cohort status.");
        }

        ObjectMap parameters = new ObjectMap();
        parameters.putIfNotNull(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        parameters.putIfNotNull(CohortDBAdaptor.QueryParams.STATUS_MSG.key(), message);

        cohortDBAdaptor.update(cohort.getUid(), parameters, new QueryOptions());
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohort.getUid(), userId, parameters, null, null);
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
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
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = cohortDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    // **************************   ACLs  ******************************** //
    public List<QueryResult<CohortAclEntry>> getAcls(String studyStr, List<String> cohortList, String member, boolean silent,
                                                     String sessionId) throws CatalogException {
        List<QueryResult<CohortAclEntry>> cohortAclList = new ArrayList<>(cohortList.size());
        String user = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, user);

        InternalGetQueryResult<Cohort> cohortQueryResult = internalGet(study.getUid(), cohortList, INCLUDE_COHORT_IDS, user, silent);

        Map<String, InternalGetQueryResult.Missing> missingMap = new HashMap<>();
        if (cohortQueryResult.getMissing() != null) {
            missingMap = cohortQueryResult.getMissing().stream()
                    .collect(Collectors.toMap(InternalGetQueryResult.Missing::getId, Function.identity()));
        }
        int counter = 0;
        for (String cohortId : cohortList) {
            if (!missingMap.containsKey(cohortId)) {
                try {
                    QueryResult<CohortAclEntry> allCohortAcls;
                    if (StringUtils.isNotEmpty(member)) {
                        allCohortAcls = authorizationManager.getCohortAcl(study.getUid(),
                                cohortQueryResult.getResult().get(counter).getUid(), user, member);
                    } else {
                        allCohortAcls = authorizationManager.getAllCohortAcls(study.getUid(),
                                cohortQueryResult.getResult().get(counter).getUid(), user);
                    }
                    allCohortAcls.setId(cohortId);
                    cohortAclList.add(allCohortAcls);
                } catch (CatalogException e) {
                    if (!silent) {
                        throw e;
                    } else {
                        cohortAclList.add(new QueryResult<>(cohortId, cohortQueryResult.getDbTime(), 0, 0, "",
                                missingMap.get(cohortId).getErrorMsg(), Collections.emptyList()));
                    }
                }
                counter += 1;
            } else {
                cohortAclList.add(new QueryResult<>(cohortId, cohortQueryResult.getDbTime(), 0, 0, "",
                        missingMap.get(cohortId).getErrorMsg(), Collections.emptyList()));
            }
        }
        return cohortAclList;
    }

    public List<QueryResult<CohortAclEntry>> updateAcl(String studyStr, List<String> cohortList, String memberIds, AclParams aclParams,
                                                       String sessionId) throws CatalogException {
        if (cohortList == null || cohortList.isEmpty()) {
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

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_STUDY_UID);

        QueryResult<Cohort> cohortQueryResult = internalGet(study.getUid(), cohortList, INCLUDE_COHORT_IDS, userId, false);

        authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), userId);

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        checkMembers(study.getUid(), members);
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
//        studyManager.membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        switch (aclParams.getAction()) {
            case SET:
                return authorizationManager.setAcls(study.getUid(), cohortQueryResult.getResult().stream().map(Cohort::getUid)
                                .collect(Collectors.toList()), members, permissions, Entity.COHORT);
            case ADD:
                return authorizationManager.addAcls(study.getUid(), cohortQueryResult.getResult().stream().map(Cohort::getUid)
                                .collect(Collectors.toList()), members, permissions, Entity.COHORT);
            case REMOVE:
                return authorizationManager.removeAcls(cohortQueryResult.getResult().stream().map(Cohort::getUid)
                                .collect(Collectors.toList()), members, permissions, Entity.COHORT);
            case RESET:
                return authorizationManager.removeAcls(cohortQueryResult.getResult().stream().map(Cohort::getUid)
                                .collect(Collectors.toList()), members, null, Entity.COHORT);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

    public FacetQueryResult facet(String studyStr, Query query, QueryOptions queryOptions, boolean defaultStats, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.defaultObject(query, Query::new);
        ParamUtils.defaultObject(queryOptions, QueryOptions::new);

        if (defaultStats || StringUtils.isEmpty(queryOptions.getString(QueryOptions.FACET))) {
            String facet = queryOptions.getString(QueryOptions.FACET);
            queryOptions.put(QueryOptions.FACET, StringUtils.isNotEmpty(facet) ? defaultFacet + ";" + facet : defaultFacet);
        }

        CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager);

        String userId = userManager.getUserId(sessionId);
        // We need to add variableSets and groups to avoid additional queries as it will be used in the catalogSolrManager
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), StudyDBAdaptor.QueryParams.GROUPS.key())));

        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

        return catalogSolrManager.facetedQuery(study, CatalogSolrManager.COHORT_SOLR_COLLECTION, query, queryOptions, userId);
    }
}
