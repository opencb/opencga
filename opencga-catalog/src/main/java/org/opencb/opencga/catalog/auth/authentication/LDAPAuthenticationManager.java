package org.opencb.opencga.catalog.auth.authentication;

import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class LDAPAuthenticationManager implements AuthenticationManager {
    @Override
    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException {
        return false;
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException();
    }
}
