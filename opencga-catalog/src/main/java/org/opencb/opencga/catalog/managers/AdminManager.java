package org.opencb.opencga.catalog.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
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

    public OpenCGAResult<User> userSearch(Query query, QueryOptions options, String token)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap auditParams = new ObjectMap()
                .append("query", query)
                .append("options", options)
                .append("token", token);
        String userId = catalogManager.getUserManager().getUserId(token);
        try {
            authorizationManager.checkIsInstallationAdministrator(userId);

            // Fix query object
            if (query.containsKey(ParamConstants.USER)) {
                query.put(UserDBAdaptor.QueryParams.ID.key(), query.get(ParamConstants.USER));
                query.remove(ParamConstants.USER);
            }
            if (query.containsKey(ParamConstants.USER_ACCOUNT_TYPE)) {
                query.put(UserDBAdaptor.QueryParams.ACCOUNT_TYPE.key(), query.get(ParamConstants.USER_ACCOUNT_TYPE));
                query.remove(ParamConstants.USER_ACCOUNT_TYPE);
            }
            if (query.containsKey(ParamConstants.USER_AUTHENTICATION_ORIGIN)) {
                query.put(UserDBAdaptor.QueryParams.ACCOUNT_AUTHENTICATION_ID.key(), query.get(ParamConstants.USER_AUTHENTICATION_ORIGIN));
                query.remove(ParamConstants.USER_AUTHENTICATION_ORIGIN);
            }
            if (query.containsKey(ParamConstants.USER_CREATION_DATE)) {
                query.put(UserDBAdaptor.QueryParams.ACCOUNT_CREATION_DATE.key(), query.get(ParamConstants.USER_CREATION_DATE));
                query.remove(ParamConstants.USER_CREATION_DATE);
            }

            OpenCGAResult<User> userDataResult = userDBAdaptor.get(query, options);
            auditManager.auditSearch(userId, Enums.Resource.USER, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return userDataResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.USER, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

}
