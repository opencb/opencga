package org.opencb.opencga.catalog.managers;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.authorization.CatalogPermission;
import org.opencb.opencga.catalog.authorization.StudyPermission;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IIndividualManager;
import org.opencb.opencga.catalog.models.Annotation;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.VariableSet;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidator;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualManager extends AbstractManager implements IIndividualManager {

    protected static Logger logger = LoggerFactory.getLogger(IndividualManager.class);

    public IndividualManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                        CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                        Properties catalogProperties) {
        super(authorizationManager, authenticationManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }


    @Override
    public QueryResult<Individual> create(QueryOptions params, String sessionId)
            throws CatalogException {
        return create(params.getInt("studyId"), params.getString("name"), params.getString("family"),
                params.getInt("fatherId"), params.getInt("motherId"), params.get("gender", Individual.Gender.class), params, sessionId);
    }

    @Override
    public QueryResult<Individual> create(int studyId, String name, String family, int fatherId, int motherId,
                                          Individual.Gender gender, QueryOptions options, String sessionId)
            throws CatalogException {

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        gender = ParamUtils.defaultObject(gender, Individual.Gender.UNKNOWN);
        ParamUtils.checkAlias(name, "name");
        family = ParamUtils.defaultObject(family, "");

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_SAMPLES);

        return individualDBAdaptor.createIndividual(studyId, new Individual(0, name, fatherId, motherId, family, gender, null, null, null, Collections.emptyList(), null), options);
    }

    @Override
    public QueryResult<Individual> read(Integer individualId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(individualId, "individualId");
        ParamUtils.checkObj(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, CatalogPermission.READ);

        return individualDBAdaptor.getIndividual(individualId, options);
    }

    @Override
    public QueryResult<Individual> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        options = ParamUtils.defaultObject(query, QueryOptions::new);
        if (options != null) {
            query.putAll(options);
        }
        return readAll(query.getInt("studyId"), query, sessionId);
    }

    @Override
    public QueryResult<Individual> readAll(int studyId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);

        QueryResult<Individual> queryResult = individualDBAdaptor.getAllIndividuals(studyId, options);
        authorizationManager.filterIndividuals(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());
        return queryResult;
    }

    @Override
    public QueryResult<AnnotationSet> annotate(int individualId, String annotationSetId, int variableSetId, Map<String, Object> annotations, Map<String, Object> attributes, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(annotationSetId, "annotationSetId");
        ParamUtils.checkObj(annotations, "annotations");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, CatalogPermission.WRITE);

        VariableSet variableSet = sampleDBAdaptor.getVariableSet(variableSetId, null).first();

        AnnotationSet annotationSet =
                new AnnotationSet(annotationSetId, variableSetId, new HashSet<>(), TimeUtils.getTime(), attributes);

        for (Map.Entry<String, Object> entry : annotations.entrySet()) {
            annotationSet.getAnnotations().add(new Annotation(entry.getKey(), entry.getValue()));
        }
        QueryResult<Individual> individualQueryResult = individualDBAdaptor.getIndividual(individualId,
                new QueryOptions("include", Collections.singletonList("annotationSets")));

        List<AnnotationSet> annotationSets = individualQueryResult.getResult().get(0).getAnnotationSets();
//        if (checkAnnotationSet) {
        CatalogAnnotationsValidator.checkAnnotationSet(variableSet, annotationSet, annotationSets);
//        }

        return individualDBAdaptor.annotateIndividual(individualId, annotationSet);

    }

    @Override
    public QueryResult<Individual> update(Integer individualId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        ParamUtils.defaultObject(parameters, QueryOptions::new);
        ParamUtils.defaultObject(options, QueryOptions::new);

        int studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, CatalogPermission.WRITE);


        options.putAll(parameters);//FIXME: Use separated params and options, or merge
        return individualDBAdaptor.modifyIndividual(individualId, options);

    }

    @Override
    public QueryResult<Individual> delete(Integer individualId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        ParamUtils.defaultObject(options, QueryOptions::new);

        int studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkIndividualPermission(individualId, userId, CatalogPermission.DELETE);


        return individualDBAdaptor.deleteIndividual(individualId, options);
    }
}
