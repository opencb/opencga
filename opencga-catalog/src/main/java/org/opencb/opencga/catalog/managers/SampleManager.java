package org.opencb.opencga.catalog.managers;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.managers.api.ISampleManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleManager extends AbstractManager implements ISampleManager {


    protected static Logger logger = LoggerFactory.getLogger(SampleManager.class);

    public SampleManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                         CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                         Properties catalogProperties) {
        super(authorizationManager, authenticationManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }


    @Override
    public Integer getStudyId(int sampleId) throws CatalogException {
        return sampleDBAdaptor.getStudyIdBySampleId(sampleId);
    }

    @Override
    public QueryResult<AnnotationSet> annotate(int sampleId, String id, int variableSetId,
                                               Map<String, Object> annotations, Map<String, Object> attributes,
                                               boolean checkAnnotationSet, String sessionId)
            throws CatalogException{
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(id, "id");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = sampleDBAdaptor.getStudyIdBySampleId(sampleId);
        if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify study");
        }

        QueryResult<VariableSet> variableSetResult = sampleDBAdaptor.getVariableSet(variableSetId, null);
        if (variableSetResult.getResult().isEmpty()) {
            throw new CatalogException("VariableSet " + variableSetId + " does not exists");
        }
        VariableSet variableSet = variableSetResult.getResult().get(0);

        AnnotationSet annotationSet =
                new AnnotationSet(id, variableSetId, new HashSet<>(), TimeUtils.getTime(), attributes);

        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            annotationSet.getAnnotations().add(new Annotation(entry.getKey(), entry.getValue()));
        }
        QueryResult<Sample> sampleQueryResult = sampleDBAdaptor.getSample(sampleId,
                new QueryOptions("include", Collections.singletonList("annotationSets")));

        List<AnnotationSet> annotationSets = sampleQueryResult.getResult().get(0).getAnnotationSets();
        if (checkAnnotationSet) {
            CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);
        }

        return sampleDBAdaptor.annotateSample(sampleId, annotationSet);
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

        if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify study");
        }
        Sample sample = new Sample(-1, name, source, -1, description, Collections.<AnnotationSet>emptyList(),
                attributes);

        return sampleDBAdaptor.createSample(studyId, sample, options);
    }

    @Override
    public QueryResult<Sample> read(Integer sampleId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = sampleDBAdaptor.getStudyIdBySampleId(sampleId);

        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. User " + userId + " can't read study");
        }

        return sampleDBAdaptor.getSample(sampleId, options);
    }

    @Override
    public QueryResult<Sample> readAll(int studyId, QueryOptions query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(query, "query");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. User " + userId + " can't read study");
        }
        query.putAll(options);
        return sampleDBAdaptor.getAllSamples(studyId, query);
    }

    @Override
    public QueryResult<Sample> readAll(QueryOptions query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(query, "query");
        return readAll(query.getInt("studyId", -1), query, options, sessionId);
    }

    @Override
    public QueryResult<Sample> update(Integer id, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkObj(parameters, "parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkParameter(sessionId, "sessionId");

        int studyId = sampleDBAdaptor.getStudyIdBySampleId(id);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. User " + userId + " can't read study");
        }
        options.putAll(parameters);
        return sampleDBAdaptor.modifySample(id, options);

    }

    @Override
    public QueryResult<Sample> delete(Integer id, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /*
     * Variables Methods
     */

    @Override
    public QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                                      String description, Map<String, Object> attributes,
                                                      List<Variable> variables, String sessionId)
            throws CatalogException {

        ParamUtils.checkObj(variables, "Variables List");
        Set<Variable> variablesSet = new HashSet<>(variables);
        if (variables.size() != variablesSet.size()) {
            throw new CatalogException("Error. Repeated variables");
        }
        return createVariableSet(studyId, name, unique, description, attributes, variablesSet, sessionId);
    }

    @Override
    public QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                                      String description, Map<String, Object> attributes,
                                                      Set<Variable> variables, String sessionId)
            throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(variables, "Variables Set");
        unique = ParamUtils.defaultObject(unique, true);
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, new HashMap<String, Object>());

        for (Variable variable : variables) {
            ParamUtils.checkParameter(variable.getId(), "variable ID");
            ParamUtils.checkObj(variable.getType(), "variable Type");
            variable.setAllowedValues(ParamUtils.defaultObject(variable.getAllowedValues(), Collections.<String>emptyList()));
            variable.setAttributes(ParamUtils.defaultObject(variable.getAttributes(), Collections.<String, Object>emptyMap()));
            variable.setCategory(ParamUtils.defaultString(variable.getCategory(), ""));
            variable.setDependsOn(ParamUtils.defaultString(variable.getDependsOn(), ""));
            variable.setDescription(ParamUtils.defaultString(variable.getDescription(), ""));
//            variable.setRank(defaultString(variable.getDescription(), ""));
        }

        if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify study");
        }

        VariableSet variableSet = new VariableSet(-1, name, unique, description, variables, attributes);
        CatalogAnnotationsValidator.checkVariableSet(variableSet);

        return sampleDBAdaptor.createVariableSet(studyId, variableSet);
    }

    @Override
    public QueryResult<VariableSet> readVariableSet(int variableSet, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = sampleDBAdaptor.getStudyIdByVariableSetId(variableSet);
        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. User " + userId + " can't read study");
        }
        return sampleDBAdaptor.getVariableSet(variableSet, options);
    }

    @Override
    public QueryResult<VariableSet> readAllVariableSets(int studyId, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw new CatalogException("Permission denied. User " + userId + " can't read study");
        }
        return sampleDBAdaptor.getAllVariableSets(studyId, options);
    }

    @Override
    public QueryResult<VariableSet> deleteVariableSet(int variableSetId, QueryOptions queryOptions, String sessionId) throws CatalogException {
        return sampleDBAdaptor.deleteVariableSet(variableSetId, queryOptions);
    }


    /**
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

        int studyId = sampleDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (authorizationManager.getStudyACL(userId, studyId).isRead()) {
            return sampleDBAdaptor.getCohort(cohortId);
        } else {
            throw CatalogAuthorizationException.cantRead(userId, "Cohort", cohortId, null);
        }
    }

    @Override
    public QueryResult<Cohort> createCohort(int studyId, String name, Cohort.Type type, String description, List<Integer> sampleIds,
                                            Map<String, Object> attributes, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(sampleIds, "Samples list");
        type = ParamUtils.defaultObject(type, Cohort.Type.COLLECTION);
        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        if (readAll(studyId, new QueryOptions("id", sampleIds), null, sessionId).getResult().size() != sampleIds.size()) {
            throw new CatalogException("Error: Some sampleId does not exist in the study " + studyId);
        }
        Cohort cohort = new Cohort(name, type, TimeUtils.getTime(), description, sampleIds, attributes);
        return sampleDBAdaptor.createCohort(studyId, cohort);
    }

    @Override
    public QueryResult<Cohort> updateCohort(int cohortId, ObjectMap params, String sessionId) throws CatalogException {
        ParamUtils.checkObj(params, "Update parameters");
        int studyId = sampleDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            return sampleDBAdaptor.updateCohort(cohortId, params);
        } else {
            throw CatalogAuthorizationException.cantModify(userId, "Cohort", cohortId, null);
        }
    }

    @Override
    public QueryResult<Cohort> deleteCohort(int cohortId, ObjectMap options, String sessionId) throws CatalogException {
        int studyId = sampleDBAdaptor.getStudyIdByCohortId(cohortId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            return sampleDBAdaptor.deleteCohort(cohortId, options);
        } else {
            throw CatalogAuthorizationException.cantModify(userId, "Cohort", cohortId, null);
        }
    }


}
