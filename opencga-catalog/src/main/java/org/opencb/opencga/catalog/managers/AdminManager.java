package org.opencb.opencga.catalog.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminManager extends AbstractManager {

    private final CatalogIOManager catalogIOManager;
    protected static Logger logger = LoggerFactory.getLogger(AdminManager.class);

    AdminManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManager catalogIOManager, Configuration configuration)
            throws CatalogException {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.catalogIOManager = catalogIOManager;
    }

    public OpenCGAResult<User> userSearch(Query query, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("query", query)
                .append("options", options)
                .append("token", token);

        return run(auditParams, Enums.Action.SEARCH, Enums.Resource.USER, "", token, options, (study, userId, rp, queryOptions) -> {
            Query myQuery = query != null ? new Query(query) : new Query();
            authorizationManager.checkIsInstallationAdministrator(userId);

            changeQueryId(myQuery, ParamConstants.USER, UserDBAdaptor.QueryParams.ID.key());
            changeQueryId(myQuery, ParamConstants.USER_ACCOUNT_TYPE, UserDBAdaptor.QueryParams.ACCOUNT_TYPE.key());
            changeQueryId(myQuery, ParamConstants.USER_AUTHENTICATION_ORIGIN, UserDBAdaptor.QueryParams.ACCOUNT_AUTHENTICATION_ID.key());
            changeQueryId(myQuery, ParamConstants.USER_CREATION_DATE, UserDBAdaptor.QueryParams.ACCOUNT_CREATION_DATE.key());

            return userDBAdaptor.get(myQuery, queryOptions);
        });
    }

}
