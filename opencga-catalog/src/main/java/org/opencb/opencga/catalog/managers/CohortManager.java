/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.ICohortManager;
import org.opencb.opencga.catalog.managers.api.IUserManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.acls.permissions.CohortAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.AnnotationManager;
import org.opencb.opencga.catalog.utils.CatalogMemberValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;

/**
 * Created by pfurio on 06/07/16.
 */
public class CohortManager extends AbstractManager implements ICohortManager {

    protected static Logger logger = LoggerFactory.getLogger(CohortManager.class);
    private IUserManager userManager;

    public CohortManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                         DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
        this.userManager = catalogManager.getUserManager();
    }

    @Override
    public QueryResult<Cohort> create(long studyId, String name, Study.Type type, String description, List<Long> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(sampleIds, "Samples list");
        type = ParamUtils.defaultObject(type, Study.Type.COLLECTION);
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        if (!sampleIds.isEmpty()) {
            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);
            QueryResult<Long> count = sampleDBAdaptor.count(query);
            if (count.first() != sampleIds.size()) {
                throw new CatalogException("Error: Some sampleId does not exist in the study " + studyId);
            }
        }
        String userId = userManager.getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_COHORTS);
        Cohort cohort = new Cohort(name, type, TimeUtils.getTime(), description, sampleIds, attributes);
        QueryResult<Cohort> queryResult = cohortDBAdaptor.insert(cohort, studyId, null);
//        auditManager.recordCreation(AuditRecord.Resource.cohort, queryResult.first().getId(), userId, queryResult.first(), null, new
//                ObjectMap());
        auditManager.recordAction(AuditRecord.Resource.cohort, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public Long getStudyId(long cohortId) throws CatalogException {
        return cohortDBAdaptor.getStudyId(cohortId);
    }

    @Override
    public Long getId(String userId, String cohortStr) throws CatalogException {
        if (StringUtils.isNumeric(cohortStr) && Long.parseLong(cohortStr) > configuration.getCatalog().getOffset()) {
            return Long.parseLong(cohortStr);
        }

        // Resolve the studyIds and filter the cohortName
        ObjectMap parsedSampleStr = parseFeatureId(userId, cohortStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String cohortName = parsedSampleStr.getString("featureName");
        if (StringUtils.isNumeric(cohortName) && Long.parseLong(cohortName) > configuration.getCatalog().getOffset()) {
            return Long.parseLong(cohortName);
        }

        Query query = new Query(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(CohortDBAdaptor.QueryParams.NAME.key(), cohortName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.cohorts.id");
        QueryResult<Cohort> queryResult = cohortDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one cohort id found based on " + cohortName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Override
    public QueryResult<Cohort> get(Long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        //options = ParamUtils.defaultObject(options, QueryOptions::new);

        long studyId = cohortDBAdaptor.getStudyId(cohortId);
        String userId = userManager.getId(sessionId);

        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.VIEW);

        QueryResult<Cohort> queryResult = cohortDBAdaptor.get(cohortId, options);
        authorizationManager.filterCohorts(userId, studyId, queryResult.getResult());

        return queryResult;
    }

    @Override
    public QueryResult<Cohort> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getId(sessionId);
        if (!authorizationManager.memberHasPermissionsInStudy(studyId, userId)) {
            throw CatalogAuthorizationException.deny(userId, "view", "cohorts", studyId, null);
        }

        QueryResult<Cohort> queryResult = cohortDBAdaptor.get(new Query(query)
                .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId), options);
        authorizationManager.filterCohorts(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    public MyResourceId getId(String cohortStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(cohortStr)) {
            throw new CatalogException("Missing cohort parameter");
        }

        String userId;
        long studyId;
        long cohortId;

        if (StringUtils.isNumeric(cohortStr) && Long.parseLong(cohortStr) > configuration.getCatalog().getOffset()) {
            cohortId = Long.parseLong(cohortStr);
            cohortDBAdaptor.exists(cohortId);
            studyId = cohortDBAdaptor.getStudyId(cohortId);
            userId = userManager.getId(sessionId);
        } else {
            if (cohortStr.contains(",")) {
                throw new CatalogException("More than one cohort found");
            }

            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            Query query = new Query()
                    .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(CohortDBAdaptor.QueryParams.NAME.key(), cohortStr);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ID.key());
            QueryResult<Cohort> cohortQueryResult = cohortDBAdaptor.get(query, queryOptions);
            if (cohortQueryResult.getNumResults() == 1) {
                cohortId = cohortQueryResult.first().getId();
            } else {
                if (cohortQueryResult.getNumResults() == 0) {
                    throw new CatalogException("Cohort " + cohortStr + " not found in study " + studyStr);
                } else {
                    throw new CatalogException("More than one cohort found under " + cohortStr + " in study " + studyStr);
                }
            }
        }

        return new MyResourceId(userId, studyId, cohortId);
    }

    @Override
    public MyResourceIds getIds(String cohortStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(cohortStr)) {
            throw new CatalogException("Missing cohort parameter");
        }

        String userId;
        long studyId;
        List<Long> cohortIds;

        if (StringUtils.isNumeric(cohortStr) && Long.parseLong(cohortStr) > configuration.getCatalog().getOffset()) {
            cohortIds = Arrays.asList(Long.parseLong(cohortStr));
            cohortDBAdaptor.exists(cohortIds.get(0));
            studyId = cohortDBAdaptor.getStudyId(cohortIds.get(0));
            userId = userManager.getId(sessionId);
        } else {
            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            List<String> cohortSplit = Arrays.asList(cohortStr.split(","));
            Query query = new Query()
                    .append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(CohortDBAdaptor.QueryParams.NAME.key(), cohortSplit);
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ID.key());
            QueryResult<Cohort> cohortQueryResult = cohortDBAdaptor.get(query, queryOptions);
            if (cohortQueryResult.getNumResults() == cohortSplit.size()) {
                cohortIds = cohortQueryResult.getResult().stream().map(Cohort::getId).collect(Collectors.toList());
            } else {
                throw new CatalogException("Found only " + cohortQueryResult.getNumResults() + " out of the " + cohortSplit.size()
                        + " cohorts looked for in study " + studyStr);
            }
        }

        return new MyResourceIds(userId, studyId, cohortIds);
    }

    @Override
    public QueryResult<Cohort> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        List<Long> studyIds = catalogManager.getStudyManager().getIds(userId, studyStr);

        // Check any permission in studies
        for (Long studyId : studyIds) {
            authorizationManager.memberHasPermissionsInStudy(studyId, userId);
        }

        QueryResult<Cohort> queryResult = null;
        for (Long studyId : studyIds) {
            query.append(CohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
            QueryResult<Cohort> queryResultAux = cohortDBAdaptor.get(query, options);
            authorizationManager.filterCohorts(userId, studyId, queryResultAux.getResult());

            if (queryResult == null) {
                queryResult = queryResultAux;
            } else {
                queryResult.getResult().addAll(queryResultAux.getResult());
                queryResult.setNumTotalResults(queryResult.getNumTotalResults() + queryResultAux.getNumTotalResults());
                queryResult.setDbTime(queryResult.getDbTime() + queryResultAux.getDbTime());
            }
        }
        queryResult.setNumResults(queryResult.getResult().size());

        return queryResult;
    }

    @Override
    @Deprecated
    public QueryResult<Cohort> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Cohort> update(Long cohortId, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(parameters, "Update parameters");
        String userId = userManager.getId(sessionId);

        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.UPDATE);

        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            CohortDBAdaptor.QueryParams queryParam = CohortDBAdaptor.QueryParams.getParam(param.getKey());
            if (queryParam == null) {
                throw new CatalogException("Cannot update " + param.getKey());
            }
            switch (queryParam) {
                case NAME:
                case CREATION_DATE:
                case DESCRIPTION:
                case SAMPLES:
                case ATTRIBUTES:
                    break;
                default:
                    throw new CatalogException("Cannot update " + queryParam);
            }
        }

        Cohort cohort = get(cohortId, new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.STATUS_NAME.key()), sessionId)
                .first();
        if (parameters.containsKey(CohortDBAdaptor.QueryParams.SAMPLES.key())
                || parameters.containsKey(CohortDBAdaptor.QueryParams.NAME.key())/* || params.containsKey("type")*/) {
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
        }

        QueryResult<Cohort> queryResult = cohortDBAdaptor.update(cohortId, parameters, options);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, parameters, null, null);
        return queryResult;
    }

    @Override
    public List<QueryResult<Cohort>> delete(String cohortIdStr, @Nullable String studyStr, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(cohortIdStr, "id");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceIds resource = getIds(cohortIdStr, studyStr, sessionId);
        List<Long> cohortIds = resource.getResourceIds();
        String userId = resource.getUser();

        List<QueryResult<Cohort>> queryResultList = new ArrayList<>(cohortIds.size());
        for (Long cohortId : cohortIds) {
            QueryResult<Cohort> queryResult = null;
            try {
                authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.DELETE);
                queryResult = cohortDBAdaptor.delete(cohortId, options);
                auditManager.recordDeletion(AuditRecord.Resource.cohort, cohortId, userId, queryResult.first(), null, null);
            } catch (CatalogAuthorizationException e) {
                auditManager.recordAction(AuditRecord.Resource.cohort, AuditRecord.Action.delete, AuditRecord.Magnitude.high, cohortId,
                        userId, null, null, e.getMessage(), null);
                queryResult = new QueryResult<>("Delete cohort " + cohortId);
                queryResult.setErrorMsg(e.getMessage());
            } catch (CatalogException e) {
                e.printStackTrace();
                queryResult = new QueryResult<>("Delete cohort " + cohortId);
                queryResult.setErrorMsg(e.getMessage());
            } finally {
                queryResultList.add(queryResult);
            }
        }

        return queryResultList;
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_COHORTS);

        // Add study id to the query
        query.put(IndividualDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = cohortDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public List<QueryResult<Cohort>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ID.key());
        QueryResult<Cohort> cohortQueryResult = cohortDBAdaptor.get(query, queryOptions);
        List<Long> cohortIds = cohortQueryResult.getResult().stream().map(Cohort::getId).collect(Collectors.toList());
        String cohortIdStr = StringUtils.join(cohortIds, ",");
        return delete(cohortIdStr, null, options, sessionId);
    }

    @Override
    public List<QueryResult<Cohort>> restore(String cohortIdStr, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(cohortIdStr, "id");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        List<Long> cohortIds = getIds(userId, cohortIdStr);

        List<QueryResult<Cohort>> queryResultList = new ArrayList<>(cohortIds.size());
        for (Long cohortId : cohortIds) {
            QueryResult<Cohort> queryResult = null;
            try {
            authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.DELETE);
            queryResult = cohortDBAdaptor.restore(cohortId, options);
            auditManager.recordAction(AuditRecord.Resource.cohort, AuditRecord.Action.restore, AuditRecord.Magnitude.medium, cohortId,
                    userId, Status.DELETED, Cohort.CohortStatus.INVALID, "Cohort restore", new ObjectMap());
            } catch (CatalogAuthorizationException e) {
                auditManager.recordAction(AuditRecord.Resource.cohort, AuditRecord.Action.restore, AuditRecord.Magnitude.high, cohortId,
                        userId, null, null, e.getMessage(), null);
                queryResult = new QueryResult<>("Restore cohort " + cohortId);
                queryResult.setErrorMsg(e.getMessage());
            } catch (CatalogException e) {
                e.printStackTrace();
                queryResult = new QueryResult<>("Restore cohort " + cohortId);
                queryResult.setErrorMsg(e.getMessage());
            } finally {
                queryResultList.add(queryResult);
            }
        }

        return queryResultList;
    }

    @Override
    public List<QueryResult<Cohort>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ID.key());
        QueryResult<Cohort> cohortQueryResult = cohortDBAdaptor.get(query, queryOptions);
        List<Long> cohortIds = cohortQueryResult.getResult().stream().map(Cohort::getId).collect(Collectors.toList());
        String cohortIdStr = StringUtils.join(cohortIds, ",");
        return restore(cohortIdStr, options, sessionId);
    }

    @Override
    public void setStatus(String id, String status, String message, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long cohortId = getId(userId, id);

        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.UPDATE);

        if (status != null && !Cohort.CohortStatus.isValid(status)) {
            throw new CatalogException("The status " + status + " is not valid cohort status.");
        }

        ObjectMap parameters = new ObjectMap();
        parameters.putIfNotNull(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        parameters.putIfNotNull(CohortDBAdaptor.QueryParams.STATUS_MSG.key(), message);

        cohortDBAdaptor.update(cohortId, parameters, new QueryOptions());
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, parameters, null, null);
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");

        String userId = userManager.getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_COHORTS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = cohortDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public List<QueryResult<CohortAclEntry>> updateAcl(String cohort, String studyStr, String memberIds, AclParams aclParams,
                                                       String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(cohort)) {
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

        // Obtain the resource ids
        MyResourceIds resourceIds = getIds(cohort, studyStr, sessionId);

        // Check the user has the permissions needed to change permissions
        for (Long cohortId : resourceIds.getResourceIds()) {
            authorizationManager.checkCohortPermission(cohortId, resourceIds.getUser(), CohortAclEntry.CohortPermissions.SHARE);
        }

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        CatalogMemberValidator.checkMembers(catalogDBAdaptorFactory, resourceIds.getStudyId(), members);
        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        String collectionName = MongoDBAdaptorFactory.COHORT_COLLECTION;

        switch (aclParams.getAction()) {
            case SET:
                return authorizationManager.setAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
            case ADD:
                return authorizationManager.addAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
            case REMOVE:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
            case RESET:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, collectionName);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }

    @Override
    public QueryResult<AnnotationSet> createAnnotationSet(String id, @Nullable String studyStr, long variableSetId,
                                                          String annotationSetName, Map<String, Object> annotations,
                                                          Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkCohortPermission(resource.getResourceId(), resource.getUser(),
                CohortAclEntry.CohortPermissions.WRITE_ANNOTATIONS);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        QueryResult<AnnotationSet> annotationSet = AnnotationManager.createAnnotationSet(resource.getResourceId(), variableSet,
                annotationSetName, annotations, attributes, cohortDBAdaptor);

        auditManager.recordUpdate(AuditRecord.Resource.cohort, resource.getResourceId(), resource.getUser(),
                new ObjectMap("annotationSets", annotationSet.first()), "annotate", null);

        return annotationSet;
    }

    @Override
    public QueryResult<AnnotationSet> getAllAnnotationSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        long cohortId = commonGetAllAnnotationSets(id, studyStr, sessionId);
        return cohortDBAdaptor.getAnnotationSet(cohortId, null);
    }

    @Override
    public QueryResult<ObjectMap> getAllAnnotationSetsAsMap(String id, @Nullable String studyStr, String sessionId) throws
            CatalogException {
        long cohortId = commonGetAllAnnotationSets(id, studyStr, sessionId);
        return cohortDBAdaptor.getAnnotationSetAsMap(cohortId, null);
    }

    private long commonGetAllAnnotationSets(String id, @Nullable String studyStr, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkCohortPermission(resource.getResourceId(), resource.getUser(),
                CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS);
        return resource.getResourceId();
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        long cohortId = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return cohortDBAdaptor.getAnnotationSet(cohortId, annotationSetName);
    }

    @Override
    public QueryResult<ObjectMap> getAnnotationSetAsMap(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        long cohortId = commonGetAnnotationSet(id, studyStr, annotationSetName, sessionId);
        return cohortDBAdaptor.getAnnotationSetAsMap(cohortId, annotationSetName);
    }

    private long commonGetAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkAlias(annotationSetName, "annotationSetName", configuration.getCatalog().getOffset());
        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkCohortPermission(resource.getResourceId(), resource.getUser(),
                CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS);
        return resource.getResourceId();
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, @Nullable String studyStr, String annotationSetName,
                                                          Map<String, Object> newAnnotations, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        MyResourceId resource = getId(id, studyStr, sessionId);
        long cohortId = resource.getResourceId();
        authorizationManager.checkCohortPermission(cohortId, resource.getUser(), CohortAclEntry.CohortPermissions.WRITE_ANNOTATIONS);

        // Update the annotation
        QueryResult<AnnotationSet> queryResult =
                AnnotationManager.updateAnnotationSet(cohortId, annotationSetName, newAnnotations, cohortDBAdaptor, studyDBAdaptor);

        if (queryResult == null || queryResult.getNumResults() == 0) {
            throw new CatalogException("There was an error with the update");
        }

        AnnotationSet annotationSet = queryResult.first();

        // Audit the changes
        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getName(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream()
                        .map(entry -> new Annotation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()), annotationSet.getCreationDate(), null);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, resource.getUser(), new ObjectMap("annotationSets",
                Collections.singletonList(annotationSetUpdate)), "update annotation", null);

        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotationSet(String id, @Nullable String studyStr, String annotationSetName, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");

        MyResourceId resource = getId(id, studyStr, sessionId);
        long cohortId = resource.getResourceId();
        authorizationManager.checkCohortPermission(cohortId, resource.getUser(), CohortAclEntry.CohortPermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> annotationSet = cohortDBAdaptor.getAnnotationSet(cohortId, annotationSetName);
        if (annotationSet == null || annotationSet.getNumResults() == 0) {
            throw new CatalogException("Could not delete annotation set. The annotation set with name " + annotationSetName + " could not "
                    + "be found in the database.");
        }

        cohortDBAdaptor.deleteAnnotationSet(cohortId, annotationSetName);

        auditManager.recordDeletion(AuditRecord.Resource.cohort, cohortId, resource.getUser(), new ObjectMap("annotationSets",
                Collections.singletonList(annotationSet.first())), "delete annotation", null);

        return annotationSet;
    }

    @Override
    public QueryResult<ObjectMap> searchAnnotationSetAsMap(String id, @Nullable String studyStr, long variableSetId,
                                                           @Nullable String annotation, String sessionId) throws CatalogException {
        QueryResult<Cohort> cohortQueryResult = commonSearchAnnotationSet(id, studyStr, variableSetId, annotation, sessionId);
        List<ObjectMap> annotationSets;

        if (cohortQueryResult == null || cohortQueryResult.getNumResults() == 0) {
            annotationSets = Collections.emptyList();
        } else {
            annotationSets = cohortQueryResult.first().getAnnotationSetAsMap();
        }

        return new QueryResult<>("Search annotation sets", cohortQueryResult.getDbTime(), annotationSets.size(), annotationSets.size(),
                cohortQueryResult.getWarningMsg(), cohortQueryResult.getErrorMsg(), annotationSets);
    }

    @Override
    public QueryResult<AnnotationSet> searchAnnotationSet(String id, @Nullable String studyStr, long variableSetId,
                                                          @Nullable String annotation, String sessionId) throws CatalogException {
        QueryResult<Cohort> cohortQueryResult = commonSearchAnnotationSet(id, studyStr, variableSetId, annotation, sessionId);
        List<AnnotationSet> annotationSets;

        if (cohortQueryResult == null || cohortQueryResult.getNumResults() == 0) {
            annotationSets = Collections.emptyList();
        } else {
            annotationSets = cohortQueryResult.first().getAnnotationSets();
        }

        return new QueryResult<>("Search annotation sets", cohortQueryResult.getDbTime(), annotationSets.size(), annotationSets.size(),
                cohortQueryResult.getWarningMsg(), cohortQueryResult.getErrorMsg(), annotationSets);
    }

    private QueryResult<Cohort> commonSearchAnnotationSet(String id, @Nullable String studyStr, long variableSetId,
                                                          @Nullable String annotation, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");

        MyResourceId resource = getId(id, studyStr, sessionId);
        authorizationManager.checkCohortPermission(resource.getResourceId(), resource.getUser(),
                CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS);

        Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), id);

        if (variableSetId > 0) {
            query.append(CohortDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        }
        if (annotation != null) {
            query.append(CohortDBAdaptor.QueryParams.ANNOTATION.key(), annotation);
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key());
        logger.debug("Query: {}, \t QueryOptions: {}", query.safeToString(), queryOptions.safeToString());

        QueryResult<Cohort> cohortQueryResult = cohortDBAdaptor.get(query, queryOptions);

        // Filter out annotation sets only from the variable set id specified
        for (Cohort cohort : cohortQueryResult.getResult()) {
            List<AnnotationSet> finalAnnotationSets = new ArrayList<>();
            for (AnnotationSet annotationSet : cohort.getAnnotationSets()) {
                if (annotationSet.getVariableSetId() == variableSetId) {
                    finalAnnotationSets.add(annotationSet);
                }
            }

            if (finalAnnotationSets.size() > 0) {
                cohort.setAnnotationSets(finalAnnotationSets);
            }
        }

        return cohortQueryResult;
    }
}
