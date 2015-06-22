package org.opencb.opencga.catalog.managers;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IIndividualManager;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

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

        ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(gender, "gender");
        ParamUtils.checkAlias(name, "name");
        family = ParamUtils.defaultObject(family, "");

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            throw CatalogAuthorizationException.cantWrite(userId, "Study", studyId, null);
        }

        return individualDBAdaptor.createIndividual(studyId, new Individual(0, name, fatherId, motherId, family, gender, null, null, null, null), options);
    }

    @Override
    public QueryResult<Individual> read(Integer individualId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(individualId, "individualId");
        ParamUtils.checkObj(sessionId, "sessionId");
        ParamUtils.defaultObject(options, QueryOptions::new);

        int studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw CatalogAuthorizationException.cantRead(userId, "Study", studyId, null);
        }

        return individualDBAdaptor.getIndividual(individualId, options);
    }

    @Override
    public QueryResult<Individual> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.defaultObject(query, QueryOptions::new);
        if (options != null) {
            query.putAll(options);
        }
        return readAll(query.getInt("studyId"), query, sessionId);
    }

    @Override
    public QueryResult<Individual> readAll(int studyId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw CatalogAuthorizationException.cantRead(userId, "Study", studyId, null);
        }

        return individualDBAdaptor.getAllIndividuals(studyId, options);
    }

    @Override
    public QueryResult<Individual> update(Integer individualId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(sessionId, "sessionId");
        ParamUtils.defaultObject(parameters, QueryOptions::new);
        ParamUtils.defaultObject(options, QueryOptions::new);

        int studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        String userId = super.userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            throw CatalogAuthorizationException.cantModify(userId, "Study", studyId, null);
        }

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
        if (!authorizationManager.getStudyACL(userId, studyId).isDelete()) {
            throw CatalogAuthorizationException.cantModify(userId, "Study", studyId, null);
        }

        QueryResult<Individual> individual = read(individualId, options, sessionId);
        individualDBAdaptor.deleteIndividual(individualId);
        return individual;
    }
}
