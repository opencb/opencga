package org.opencb.opencga.catalog.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.authorization.CatalogPermission;
import org.opencb.opencga.catalog.authorization.StudyPermission;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.ISampleManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleManager extends AbstractManager implements ISampleManager {


    protected static Logger logger = LoggerFactory.getLogger(SampleManager.class);

    @Deprecated
    public SampleManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                         CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public SampleManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                         CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogConfiguration);
    }

    @Override
    public Long getStudyId(long sampleId) throws CatalogException {
        return sampleDBAdaptor.getStudyIdBySampleId(sampleId);
    }

    @Override
    public QueryResult<AnnotationSet> annotate(long sampleId, String annotationSetId, long variableSetId, Map<String, Object> annotations,
                                               Map<String, Object> attributes, boolean checkAnnotationSet,
                                               String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkSamplePermission(sampleId, userId, CatalogPermission.WRITE);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        AnnotationSet annotationSet = new AnnotationSet(annotationSetId, variableSetId, new HashSet<>(), TimeUtils.getTime(), attributes);

        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            annotationSet.getAnnotations().add(new Annotation(entry.getKey(), entry.getValue()));
        }
        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.getSample(sampleId,
                new QueryOptions("include", Collections.singletonList("projects.studies.samples.annotationSets")));

        List<AnnotationSet> annotationSets = sampleQueryResult.first().getAnnotationSets();
        if (checkAnnotationSet) {
            CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);
        }

        QueryResult<AnnotationSet> queryResult = sampleDBAdaptor.annotateSample(sampleId, annotationSet, false);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId,
                new ObjectMap("annotationSets", queryResult.first()), "annotate", null);
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotation(long sampleId, String annotationSetId, Map<String, Object> newAnnotations, String
            sessionId)
            throws CatalogException {

        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(newAnnotations, "newAnnotations");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkSamplePermission(sampleId, userId, CatalogPermission.WRITE);

        // Get sample
        QueryOptions queryOptions = new QueryOptions("include", Collections.singletonList("projects.studies.samples.annotationSets"));
        Sample sample = sampleDBAdaptor.getSample(sampleId, queryOptions).first();

        List<AnnotationSet> annotationSets = sample.getAnnotationSets();

        // Get annotation set
        AnnotationSet annotationSet = null;
        for (AnnotationSet annotationSetAux : sample.getAnnotationSets()) {
            if (annotationSetAux.getId().equals(annotationSetId)) {
                annotationSet = annotationSetAux;
                sample.getAnnotationSets().remove(annotationSet);
                break;
            }
        }

        if (annotationSet == null) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationSetId);
        }

        // Get variable set
        VariableSet variableSet = studyDBAdaptor.getVariableSet(annotationSet.getVariableSetId(), null).first();

        // Update and validate annotations
        CatalogAnnotationsValidator.mergeNewAnnotations(annotationSet, newAnnotations);
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);

        // Commit changes
        QueryResult<AnnotationSet> queryResult = sampleDBAdaptor.annotateSample(sampleId, annotationSet, true);

        AnnotationSet annotationSetUpdate = new AnnotationSet(annotationSet.getId(), annotationSet.getVariableSetId(),
                newAnnotations.entrySet().stream()
                        .map(entry -> new Annotation(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toSet()), annotationSet.getDate(), null);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSetUpdate)), "update annotation", null);
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotation(long sampleId, String annotationId, String sessionId) throws CatalogException {

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long studyId = sampleDBAdaptor.getStudyIdBySampleId(sampleId);

        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);

        QueryResult<AnnotationSet> queryResult = sampleDBAdaptor.deleteAnnotation(sampleId, annotationId);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, new ObjectMap("annotationSets", queryResult.first()),
                "deleteAnnotation", null);
        return queryResult;
    }

    @Override
    public QueryResult<Annotation> load(File file) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public QueryResult<Sample> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(objectMap, "objectMap");
        return create(
                objectMap.getInt("studyId"),
                objectMap.getString("name"),
                objectMap.getString("source"),
                objectMap.getString("description"),
                objectMap.getMap("attributes"),
                options,
                sessionId
        );
    }

    @Override
    public QueryResult<Sample> create(long studyId, String name, String source, String description,
                                      Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        source = ParamUtils.defaultString(source, "");
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, Collections.<String, Object>emptyMap());

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);

        Sample sample = new Sample(-1, name, source, -1, description, Collections.<AclEntry>emptyList(), Collections
                .<AnnotationSet>emptyList(),
                attributes);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        QueryResult<Sample> queryResult = sampleDBAdaptor.createSample(studyId, sample, options);
        auditManager.recordCreation(AuditRecord.Resource.sample, queryResult.first().getId(), userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Sample> read(Long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkSamplePermission(sampleId, userId, CatalogPermission.READ);

        return sampleDBAdaptor.getSample(sampleId, options);
    }

    @Override
    public QueryResult<Sample> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Sample> queryResult = sampleDBAdaptor.get(query, options);

        authorizationManager.filterSamples(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());

        return queryResult;
    }

    @Override
    public QueryResult<Sample> readAll(Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(query, "query");
        QueryResult<Sample> result =
                readAll(query.getInt(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), -1), query, options, sessionId);
//        auditManager.recordRead(AuditRecord.Resource.sample, , userId, parameters, null, null);
        return result;
    }

    @Override
    public QueryResult<Sample> update(Long sampleId, ObjectMap parameters, QueryOptions options, String sessionId) throws
            CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkSamplePermission(sampleId, userId, CatalogPermission.WRITE);

        options.putAll(parameters);
        QueryResult<Sample> queryResult = sampleDBAdaptor.modifySample(sampleId, options);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, parameters, null, null);
        return queryResult;

    }

    @Override
    public QueryResult<Sample> delete(Long sampleId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(sampleId, "sampleId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkSamplePermission(sampleId, userId, CatalogPermission.DELETE);

        QueryResult<Sample> queryResult = sampleDBAdaptor.delete(sampleId, new QueryOptions());
        auditManager.recordDeletion(AuditRecord.Resource.sample, sampleId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.rank(query, field, numResults, asc);
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
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
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
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        //query.append(CatalogSampleDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = sampleDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    /*
     * Cohort methods
     * ***************************
     */
    @Override
    public long getStudyIdByCohortId(long cohortId) throws CatalogException {
        return cohortDBAdaptor.getStudyIdByCohortId(cohortId);
    }

    @Override
    public QueryResult<Cohort> readCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        //options = ParamUtils.defaultObject(options, QueryOptions::new);

        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        QueryResult<Cohort> queryResult = cohortDBAdaptor.getCohort(cohortId, options);
        authorizationManager.checkReadCohort(userId, queryResult.first());

        return queryResult;

    }

    @Override
    public QueryResult<Cohort> readAllCohort(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        QueryResult<Cohort> queryResult = cohortDBAdaptor.get(new Query(query)
                .append(CatalogCohortDBAdaptor.QueryParams.STUDY_ID.key(), studyId), options);
        authorizationManager.filterCohorts(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> createCohort(long studyId, String name, Cohort.Type type, String description, List<Long> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(sampleIds, "Samples list");
        type = ParamUtils.defaultObject(type, Cohort.Type.COLLECTION);
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        if (!sampleIds.isEmpty() && readAll(studyId, new Query(CatalogSampleDBAdaptor.QueryParams.ID.key(), sampleIds)
                , null, sessionId).getResult().size() != sampleIds.size()) {
            throw new CatalogException("Error: Some sampleId does not exist in the study " + studyId);
        }
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);
        Cohort cohort = new Cohort(name, type, TimeUtils.getTime(), description, sampleIds, attributes);
        QueryResult<Cohort> queryResult = cohortDBAdaptor.createCohort(studyId, cohort, null);
        auditManager.recordCreation(AuditRecord.Resource.cohort, queryResult.first().getId(), userId, queryResult.first(), null, new
                ObjectMap());
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> updateCohort(long cohortId, ObjectMap params, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(params, "Update parameters");
        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        Cohort cohort = readCohort(cohortId, new QueryOptions(MongoDBCollection.INCLUDE, "projects.studies.cohorts."
                + CatalogCohortDBAdaptor.QueryParams.STATUS_STATUS.key()), sessionId).first();
        if (params.containsKey(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key())
                || params.containsKey(CatalogCohortDBAdaptor.QueryParams.NAME.key())/* || params.containsKey("type")*/) {
            switch (cohort.getStatus().getStatus()) {
                case Cohort.CohortStatus.CALCULATING:
                    throw new CatalogException("Unable to modify a cohort while it's in status \"" + Cohort.CohortStatus.CALCULATING
                            + "\"");
                case Cohort.CohortStatus.READY:
                    params.putIfAbsent("status.status", Cohort.CohortStatus.INVALID);
                    break;
                case Cohort.CohortStatus.NONE:
                case Cohort.CohortStatus.INVALID:
                    break;
                default:
                    break;
            }
        }
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);
        QueryResult<Cohort> queryResult = cohortDBAdaptor.modifyCohort(cohortId, params, options);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, params, null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> deleteCohort(long cohortId, QueryOptions options, String sessionId) throws CatalogException {
        long studyId = cohortDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);

        QueryResult<Cohort> queryResult = cohortDBAdaptor.deleteCohort(cohortId, options);
        auditManager.recordDeletion(AuditRecord.Resource.cohort, cohortId, userId, queryResult.first(), null, null);
        return queryResult;
    }
}
