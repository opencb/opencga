package org.opencb.opencga.catalog.authentication;

import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface AuthenticationManager {

    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException;

    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException;

}
