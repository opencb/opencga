package org.opencb.opencga.catalog.authentication;

import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;

/**
 * Created by hpccoll1 on 28/04/15.
 */
public class CatalogAuthenticationManager implements AuthenticationManager {

    protected final CatalogUserDBAdaptor userDBAdaptor;

    public CatalogAuthenticationManager(CatalogUserDBAdaptor userDBAdaptor) {
        this.userDBAdaptor = userDBAdaptor;
    }

    @Override
    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException {
        return false;
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        userDBAdaptor.changePassword(userId, oldPassword, newPassword);
    }
}
