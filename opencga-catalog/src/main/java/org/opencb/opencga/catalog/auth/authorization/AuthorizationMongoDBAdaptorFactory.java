package org.opencb.opencga.catalog.auth.authorization;

import org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.core.config.Configuration;

import java.util.HashMap;
import java.util.Map;

public class AuthorizationMongoDBAdaptorFactory implements AuthorizationDBAdaptorFactory {

    private final MongoDBAdaptorFactory dbAdaptorFactory;
    private final Configuration catalogConfiguration;
    private final Map<String, AuthorizationMongoDBAdaptor> authorizationMongoDBAdaptorMap;

    public AuthorizationMongoDBAdaptorFactory(MongoDBAdaptorFactory dbAdaptorFactory, Configuration catalogConfiguration)
            throws CatalogDBException {
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.catalogConfiguration = catalogConfiguration;
        this.authorizationMongoDBAdaptorMap = new HashMap<>();
        for (String organizationId : dbAdaptorFactory.getOrganizationIds()) {
            OrganizationMongoDBAdaptorFactory orgDbAdaptorFactory = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organizationId);
            authorizationMongoDBAdaptorMap.put(organizationId, new AuthorizationMongoDBAdaptor(orgDbAdaptorFactory, catalogConfiguration));
        }
    }

    @Override
    public AuthorizationMongoDBAdaptor getAuthorizationDBAdaptor(String organization) throws CatalogDBException {
        if (authorizationMongoDBAdaptorMap.containsKey(organization)) {
            return authorizationMongoDBAdaptorMap.get(organization);
        }
        OrganizationMongoDBAdaptorFactory orgDBAdaptorFactory = dbAdaptorFactory.getOrganizationMongoDBAdaptorFactory(organization);
        authorizationMongoDBAdaptorMap.put(organization, new AuthorizationMongoDBAdaptor(orgDBAdaptorFactory, catalogConfiguration));
        return authorizationMongoDBAdaptorMap.get(organization);
    }

}
