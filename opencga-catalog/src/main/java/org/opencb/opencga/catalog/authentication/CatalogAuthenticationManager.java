package org.opencb.opencga.catalog.authentication;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
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
