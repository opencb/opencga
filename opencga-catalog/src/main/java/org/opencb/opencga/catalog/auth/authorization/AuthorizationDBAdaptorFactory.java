package org.opencb.opencga.catalog.auth.authorization;

import org.opencb.opencga.catalog.exceptions.CatalogDBException;

public interface AuthorizationDBAdaptorFactory {

    AuthorizationDBAdaptor getAuthorizationDBAdaptor(String organization) throws CatalogDBException;

}
