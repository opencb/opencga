package org.opencb.opencga.catalog.authentication;

import org.opencb.opencga.catalog.CatalogException;

/**
 * Created by hpccoll1 on 28/04/15.
 */
public class LDAPAuthenticationManager implements AuthenticationManager {
    @Override
    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException {
        return false;
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException();
    }
}
