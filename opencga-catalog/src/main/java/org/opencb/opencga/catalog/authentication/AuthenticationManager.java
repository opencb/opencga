package org.opencb.opencga.catalog.authentication;

import org.opencb.opencga.catalog.CatalogException;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public interface AuthenticationManager {

    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException;

    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException;

}
