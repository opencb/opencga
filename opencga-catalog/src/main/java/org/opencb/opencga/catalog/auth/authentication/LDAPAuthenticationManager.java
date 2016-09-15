package org.opencb.opencga.catalog.auth.authentication;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import java.util.Hashtable;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class LDAPAuthenticationManager implements AuthenticationManager {

    private String host;

    public LDAPAuthenticationManager(String host) {
        this.host = host;
        if (!this.host.startsWith("ldap://")) {
            this.host = "ldap://" + this.host;
        }
    }

    @Override
    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, host);

        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, userId);
        env.put(Context.SECURITY_CREDENTIALS, password);

        // Create the initial context
        try {
            new InitialDirContext(env);
        } catch (NamingException e) {
            if (throwException) {
                throw new CatalogException(e.getMessage());
            }
            return false;
        }
        return true;
    }

    @Override
    public String getUserId(String token) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult resetPassword(String userId) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void newPassword(String userId, String newPassword) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    public String getHost() {
        return host;
    }

    public LDAPAuthenticationManager setHost(String host) {
        this.host = host;
        return this;
    }
}
