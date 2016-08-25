package org.opencb.opencga.catalog.managers;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.authorization.CatalogPermission;
import org.opencb.opencga.catalog.authorization.StudyPermission;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
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

    public SampleManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                         AuditManager auditManager,
                         CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }


    @Override
    public Integer getStudyId(int sampleId) throws CatalogException {
        return sampleDBAdaptor.getStudyIdBySampleId(sampleId);
    }

    @Override
    public QueryResult<AnnotationSet> annotate(int sampleId, String annotationSetId, int variableSetId,
                                               Map<String, Object> annotations, Map<String, Object> attributes,
                                               boolean checkAnnotationSet, String sessionId)
            throws CatalogException{
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkSamplePermission(sampleId, userId, CatalogPermission.WRITE);

        VariableSet variableSet = studyDBAdaptor.getVariableSet(variableSetId, null).first();

        AnnotationSet annotationSet =
                new AnnotationSet(annotationSetId, variableSetId, new HashSet<>(), TimeUtils.getTime(), attributes);

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
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, new ObjectMap("annotationSets", queryResult.first()), "annotate", null);
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> updateAnnotation(int sampleId, String annotationSetId, Map<String, Object> newAnnotations, String sessionId)
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
                newAnnotations.entrySet().stream().map(entry -> new Annotation(entry.getKey(), entry.getValue())).collect(Collectors.toSet()),
                annotationSet.getDate(), null);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, new ObjectMap("annotationSets",
                Collections.singletonList(annotationSetUpdate)), "update annotation", null);
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotation(int sampleId, String annotationId, String sessionId) throws CatalogException {

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = sampleDBAdaptor.getStudyIdBySampleId(sampleId);

        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);

        QueryResult<AnnotationSet> queryResult = sampleDBAdaptor.deleteAnnotation(sampleId, annotationId);
        auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userId, new ObjectMap("annotationSets", queryResult.first()), "deleteAnnotation", null);
        return queryResult;
    }

    @Override
    public QueryResult<Annotation> load(File file) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult<Sample> create(QueryOptions params, String sessionId) throws CatalogException {
        ParamUtils.checkObj(params, "params");
        return create(
                params.getInt("studyId"),
                params.getString("name"),
                params.getString("source"),
                params.getString("description"),
                params.getMap("attributes"),
                params,
                sessionId
        );
    }

    @Override
    public QueryResult<Sample> create(int studyId, String name, String source, String description,
                                      Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        source = ParamUtils.defaultString(source, "");
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, Collections.<String, Object>emptyMap());

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);

        Sample sample = new Sample(-1, name, source, -1, description, Collections.<AclEntry>emptyList(), Collections.<AnnotationSet>emptyList(),
                attributes);

        QueryResult<Sample> queryResult = sampleDBAdaptor.createSample(studyId, sample, options);
        auditManager.recordCreation(AuditRecord.Resource.sample, queryResult.first().getId(), userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Sample> read(Integer sampleId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkSamplePermission(sampleId, userId, CatalogPermission.READ);

        return sampleDBAdaptor.getSample(sampleId, options);
    }

    @Override
    public QueryResult<Sample> readAll(int studyId, QueryOptions query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(query, "query");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        query.putAll(options);
        query.put(CatalogSampleDBAdaptor.SampleFilterOption.studyId.toString(), studyId);
        QueryResult<Sample> queryResult = sampleDBAdaptor.getAllSamples(query);

        authorizationManager.filterSamples(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());

        return queryResult;
    }

    @Override
    public QueryResult<Sample> readAll(QueryOptions query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(query, "query");
        return readAll(query.getInt("studyId", -1), query, options, sessionId);
    }

    @Override
    public QueryResult<Sample> update(Integer sampleId, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
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
    public QueryResult<Sample> delete(Integer sampleId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(sampleId, "sampleId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkSamplePermission(sampleId, userId, CatalogPermission.DELETE);

        QueryResult<Sample> queryResult = sampleDBAdaptor.deleteSample(sampleId);
        auditManager.recordDeletion(AuditRecord.Resource.sample, sampleId, userId, queryResult.first(), null, null);
        return queryResult;
    }

    /*
     * Cohort methods
     * ***************************
     */

    @Override
    public int getStudyIdByCohortId(int cohortId) throws CatalogException {
        return sampleDBAdaptor.getStudyIdByCohortId(cohortId);
    }

    @Override
    public QueryResult<Cohort> readCohort(int cohortId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        int studyId = sampleDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        QueryResult<Cohort> queryResult = sampleDBAdaptor.getCohort(cohortId);
        authorizationManager.checkReadCohort(userId, queryResult.first());

        return queryResult;

    }

    @Override
    public QueryResult<Cohort> readAllCohort(int studyId, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        QueryResult<Cohort> queryResult = sampleDBAdaptor.getAllCohorts(studyId, options);
        authorizationManager.filterCohorts(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> createCohort(int studyId, String name, Cohort.Type type, String description, List<Integer> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(sampleIds, "Samples list");
        type = ParamUtils.defaultObject(type, Cohort.Type.COLLECTION);
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        if (!sampleIds.isEmpty() && readAll(studyId, new QueryOptions(CatalogSampleDBAdaptor.SampleFilterOption.id.toString(), sampleIds), null, sessionId).getResult().size() != sampleIds.size()) {
            throw new CatalogException("Error: Some sampleId does not exist in the study " + studyId);
        }
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);
        Cohort cohort = new Cohort(name, type, TimeUtils.getTime(), description, sampleIds, attributes);
        QueryResult<Cohort> queryResult = sampleDBAdaptor.createCohort(studyId, cohort);
        auditManager.recordCreation(AuditRecord.Resource.cohort, queryResult.first().getId(), userId, queryResult.first(), null, new ObjectMap());
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> updateCohort(int cohortId, ObjectMap params, String sessionId) throws CatalogException {
        ParamUtils.checkObj(params, "Update parameters");
        int studyId = sampleDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        Cohort cohort = readCohort(cohortId, new QueryOptions("include", "projects.studies.cohorts.status"), sessionId).first();
        if (params.containsKey("samples") || params.containsKey("name")/* || params.containsKey("type")*/) {
            switch (cohort.getStatus()) {
                case CALCULATING:
                        throw new CatalogException("Unable to modify a cohort while it's in status \"" + Cohort.Status.CALCULATING + "\"");
                case READY:
                    params.put("status", Cohort.Status.INVALID);
                    break;
                case NONE:
                case INVALID:
                    break;
            }
        }
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);
        QueryResult<Cohort> queryResult = sampleDBAdaptor.modifyCohort(cohortId, params);
        auditManager.recordUpdate(AuditRecord.Resource.cohort, cohortId, userId, params, null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Cohort> deleteCohort(int cohortId, ObjectMap options, String sessionId) throws CatalogException {
        int studyId = sampleDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);

        QueryResult<Cohort> queryResult = sampleDBAdaptor.deleteCohort(cohortId, options);
        auditManager.recordDeletion(AuditRecord.Resource.cohort, cohortId, userId, queryResult.first(), null, null);
        return queryResult;
    }
}
