package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.ICohortManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.CohortAclEntry;
import org.opencb.opencga.catalog.models.acls.StudyAclEntry;
import org.opencb.opencga.catalog.utils.AnnotationManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 06/07/16.
 */
public class CohortManager extends AbstractManager implements ICohortManager {

    protected static Logger logger = LoggerFactory.getLogger(CohortManager.class);

    public CohortManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                         CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogConfiguration);
    }

    @Override
    @Deprecated
    public QueryResult<Cohort> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        return null;
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
                    .append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(CatalogSampleDBAdaptor.QueryParams.ID.key(), sampleIds);
            QueryResult<Long> count = sampleDBAdaptor.count(query);
            if (count.first() != sampleIds.size()) {
                throw new CatalogException("Error: Some sampleId does not exist in the study " + studyId);
            }
        }
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.CREATE_COHORTS);
        Cohort cohort = new Cohort(name, type, TimeUtils.getTime(), description, sampleIds, attributes);
        QueryResult<Cohort> queryResult = cohortDBAdaptor.createCohort(studyId, cohort, null);
//        auditManager.recordCreation(AuditRecord.Resource.cohort, queryResult.first().getId(), userId, queryResult.first(), null, new
//                ObjectMap());
        auditManager.recordAction(AuditRecord.Resource.cohort, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public Long getStudyId(long cohortId) throws CatalogException {
        return cohortDBAdaptor.getStudyIdByCohortId(cohortId);
    }

    @Override
    public Long getCohortId(String userId, String cohortStr) throws CatalogException {
        if (StringUtils.isNumeric(cohortStr)) {
            return Long.parseLong(cohortStr);
        }

        // Resolve the studyIds and filter the cohortName
        ObjectMap parsedSampleStr = parseFeatureId(userId, cohortStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String cohortName = parsedSampleStr.getString("featureName");

        Query query = new Query(CatalogCohortDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(CatalogCohortDBAdaptor.QueryParams.NAME.key(), cohortName);
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
    public QueryResult<Cohort> read(Long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        //options = ParamUtils.defaultObject(options, QueryOptions::new);

        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.VIEW);

        QueryResult<Cohort> queryResult = cohortDBAdaptor.getCohort(cohortId, options);
        authorizationManager.filterCohorts(userId, studyId, queryResult.getResult());

        return queryResult;
    }

    @Override
    public QueryResult<Cohort> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.memberHasPermissionsInStudy(studyId, userId)) {
            throw CatalogAuthorizationException.deny(userId, "view", "cohorts", studyId, null);
        }

        QueryResult<Cohort> queryResult = cohortDBAdaptor.get(new Query(query)
                .append(CatalogCohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId), options);
        authorizationManager.filterCohorts(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    @Deprecated
    public QueryResult<Cohort> readAll(Query query, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Cohort> update(Long cohortId, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(parameters, "Update parameters");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.UPDATE);

        Cohort cohort = read(cohortId, new QueryOptions(QueryOptions.INCLUDE, "projects.studies.cohorts."
                + CatalogCohortDBAdaptor.QueryParams.STATUS_NAME.key()), sessionId).first();
        if (parameters.containsKey(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key())
                || parameters.containsKey(CatalogCohortDBAdaptor.QueryParams.NAME.key())/* || params.containsKey("type")*/) {
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

        QueryResult<Cohort> queryResult = cohortDBAdaptor.modifyCohort(cohortId, parameters, options);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, parameters, null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> delete(Long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.DELETE);

        QueryResult<Cohort> queryResult = cohortDBAdaptor.deleteCohort(cohortId, options);
        auditManager.recordDeletion(AuditRecord.Resource.cohort, cohortId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult groupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_COHORTS);

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
    public QueryResult groupBy(long studyId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_COHORTS);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = cohortDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
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
    public QueryResult<AnnotationSet> createAnnotationSet(String id, long variableSetId, String annotationSetName,
                                                          Map<String, Object> annotations, Map<String, Object> attributes,
                                                          String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long cohortId = getCohortId(userId, id);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.CREATE_ANNOTATIONS);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        QueryResult<AnnotationSet> annotationSet = AnnotationManager.createAnnotationSet(cohortId, variableSet, annotationSetName,
                annotations, attributes, cohortDBAdaptor);

        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId,
                new ObjectMap("annotationSets", annotationSet.first()), "annotate", null);

        return annotationSet;
    }

    @Override
    public QueryResult<AnnotationSet> getAllAnnotationSets(String id, String sessionId) throws CatalogException {
        long cohortId = commonGetAllAnnotationSets(id, sessionId);
        return cohortDBAdaptor.getAnnotationSet(cohortId, null);
    }

    @Override
    public QueryResult<ObjectMap> getAllAnnotationSetsAsMap(String id, String sessionId) throws CatalogException {
        long cohortId = commonGetAllAnnotationSets(id, sessionId);
        return cohortDBAdaptor.getAnnotationSetAsMap(cohortId, null);
    }

    private long commonGetAllAnnotationSets(String id, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(id, "id");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long cohortId = getCohortId(userId, id);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS);
        return cohortId;
    }

    @Override
    public QueryResult<AnnotationSet> getAnnotationSet(String id, String annotationSetName, String sessionId) throws CatalogException {
        long cohortId = commonGetAnnotationSet(id, annotationSetName, sessionId);
        return cohortDBAdaptor.getAnnotationSet(cohortId, annotationSetName);
    }

    @Override
    public QueryResult<ObjectMap> getAnnotationSetAsMap(String id, String annotationSetName, String sessionId) throws CatalogException {
        long cohortId = commonGetAnnotationSet(id, annotationSetName, sessionId);
        return cohortDBAdaptor.getAnnotationSetAsMap(cohortId, annotationSetName);
    }

    private long commonGetAnnotationSet(String id, String annotationSetName, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkAlias(annotationSetName, "annotationSetName");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long cohortId = getCohortId(userId, id);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS);
        return cohortId;
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotationSet(String id, String annotationSetName, Map<String, Object> newAnnotations,
                                                          String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long cohortId = getCohortId(userId, id);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.UPDATE_ANNOTATIONS);

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
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSetUpdate)), "update annotation", null);

        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotationSet(String id, String annotationSetName, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetName, "annotationSetName");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long cohortId = getCohortId(userId, id);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.DELETE_ANNOTATIONS);

        QueryResult<AnnotationSet> annotationSet = cohortDBAdaptor.getAnnotationSet(cohortId, annotationSetName);
        if (annotationSet == null || annotationSet.getNumResults() == 0) {
            throw new CatalogException("Could not delete annotation set. The annotation set with name " + annotationSetName + " could not "
                    + "be found in the database.");
        }

        cohortDBAdaptor.deleteAnnotationSet(cohortId, annotationSetName);

        auditManager.recordDeletion(AuditRecord.Resource.cohort, cohortId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSet.first())), "delete annotation", null);

        return annotationSet;
    }

    @Override
    public QueryResult<ObjectMap> searchAnnotationSetAsMap(String id, long variableSetId, @Nullable String annotation, String sessionId)
            throws CatalogException {
        QueryResult<Cohort> cohortQueryResult = commonSearchAnnotationSet(id, variableSetId, annotation, sessionId);
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
    public QueryResult<AnnotationSet> searchAnnotationSet(String id, long variableSetId, @Nullable String annotation,
                                                          String sessionId) throws CatalogException {
        QueryResult<Cohort> cohortQueryResult = commonSearchAnnotationSet(id, variableSetId, annotation, sessionId);
        List<AnnotationSet> annotationSets;

        if (cohortQueryResult == null || cohortQueryResult.getNumResults() == 0) {
            annotationSets = Collections.emptyList();
        } else {
            annotationSets = cohortQueryResult.first().getAnnotationSets();
        }

        return new QueryResult<>("Search annotation sets", cohortQueryResult.getDbTime(), annotationSets.size(), annotationSets.size(),
                cohortQueryResult.getWarningMsg(), cohortQueryResult.getErrorMsg(), annotationSets);
    }

    private QueryResult<Cohort> commonSearchAnnotationSet(String id, long variableSetId, @Nullable String annotation, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long cohortId = getCohortId(userId, id);
        authorizationManager.checkCohortPermission(cohortId, userId, CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS);

        Query query = new Query(CatalogCohortDBAdaptor.QueryParams.ID.key(), id);

        if (variableSetId > 0) {
            query.append(CatalogCohortDBAdaptor.QueryParams.VARIABLE_SET_ID.key(), variableSetId);
        }
        if (annotation != null) {
            query.append(CatalogCohortDBAdaptor.QueryParams.ANNOTATION.key(), annotation);
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, CatalogCohortDBAdaptor.QueryParams.ANNOTATION_SETS.key());
        return cohortDBAdaptor.get(query, queryOptions);
    }
}
