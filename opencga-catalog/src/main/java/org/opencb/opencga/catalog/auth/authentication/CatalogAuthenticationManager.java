package org.opencb.opencga.catalog.auth.authentication;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogMetaDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.MailUtils;

import java.security.NoSuchAlgorithmException;
import java.util.Properties;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthenticationManager implements AuthenticationManager {

    protected final CatalogUserDBAdaptor userDBAdaptor;
    protected final CatalogMetaDBAdaptor metaDBAdaptor;
    protected final Properties catalogProperties;
    protected final CatalogConfiguration catalogConfiguration;

    public CatalogAuthenticationManager(CatalogDBAdaptorFactory dbAdaptorFactory, CatalogConfiguration catalogConfiguration) {
        this.userDBAdaptor = dbAdaptorFactory.getCatalogUserDBAdaptor();
        this.metaDBAdaptor = dbAdaptorFactory.getCatalogMetaDBAdaptor();
        catalogProperties = null;
        this.catalogConfiguration = catalogConfiguration;
    }

    public static String cypherPassword(String password) throws CatalogException {
        try {
            return StringUtils.sha1(password);
        } catch (NoSuchAlgorithmException e) {
            throw new CatalogDBException("Could not encode password", e);
        }
    }

    @Override
    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException {
        String cypherPassword = (password.length() != 40) ? cypherPassword(password) : password;
        String storedPassword;
        if (userId.equals("admin")) {
            storedPassword = metaDBAdaptor.getAdminPassword();
        } else {
            storedPassword = userDBAdaptor.getUser(userId, new QueryOptions(QueryOptions.INCLUDE, "password"), null).first().getPassword();
        }
        if (storedPassword.equals(cypherPassword)) {
            return true;
        } else {
            if (throwException) {
                throw new CatalogException("Bad user or password");
            } else {
                return false;
            }
        }
    }

    @Override
    public void changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
        String oldCryptPass = (oldPassword.length() != 40) ? cypherPassword(oldPassword) : oldPassword;
        String newCryptPass = (newPassword.length() != 40) ? cypherPassword(newPassword) : newPassword;
        userDBAdaptor.changePassword(userId, oldCryptPass, newCryptPass);
    }

    @Override
    public void newPassword(String userId, String newPassword) throws CatalogException {
        String newCryptPass = (newPassword.length() != 40) ? cypherPassword(newPassword) : newPassword;
        userDBAdaptor.changePassword(userId, "", newCryptPass);
    }

    @Override
    public QueryResult resetPassword(String userId) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        userDBAdaptor.updateUserLastModified(userId);

        String newPassword = StringUtils.randomString(6);

        String newCryptPass = cypherPassword(newPassword);

        QueryResult<User> user =
                userDBAdaptor.getUser(userId, new QueryOptions(QueryOptions.INCLUDE, CatalogUserDBAdaptor.QueryParams.EMAIL.key()), "");

        if (user == null && user.getNumResults() != 1) {
            throw new CatalogException("Could not retrieve the user e-mail.");
        }

        String email = user.first().getEmail();

        QueryResult qr = userDBAdaptor.resetPassword(userId, email, newCryptPass);

        /*
        String mailUser = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_USER);
        String mailPassword = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_PASSWORD);
        String mailHost = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_HOST);
        String mailPort = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_PORT);
*/
        String mailUser = catalogConfiguration.getEmailServer().getFrom();
        String mailPassword = catalogConfiguration.getEmailServer().getPassword();
        String mailHost = catalogConfiguration.getEmailServer().getHost();
        String mailPort = catalogConfiguration.getEmailServer().getPort();

        MailUtils.sendResetPasswordMail(email, newPassword, mailUser, mailPassword, mailHost, mailPort);

        return qr;
    }

}
