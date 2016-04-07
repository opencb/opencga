package org.opencb.opencga.catalog.authentication;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.MailUtils;

import java.security.NoSuchAlgorithmException;
import java.util.Properties;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthenticationManager implements AuthenticationManager {

    protected final CatalogUserDBAdaptor userDBAdaptor;
    protected final Properties catalogProperties;

    public CatalogAuthenticationManager(CatalogUserDBAdaptor userDBAdaptor, Properties properties) {
        this.userDBAdaptor = userDBAdaptor;
        catalogProperties = properties;
    }

    public static String cipherPassword(String password) throws CatalogException {
        try {
            return StringUtils.sha1(password);
        } catch (NoSuchAlgorithmException e) {
            throw new CatalogDBException("Could not encode password", e);
        }
    }

    @Override
    public boolean authenticate(String userId, String password, boolean throwException) throws CatalogException {
        String cypherPassword = (password.length() != 40) ? cipherPassword(password) : password;
        String storedPassword = userDBAdaptor.getUser(userId, new QueryOptions("include", "password"), null).first().getPassword();
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
        String oldCryptPass = (oldPassword.length() != 40) ? cipherPassword(oldPassword) : oldPassword;
        String newCryptPass = (newPassword.length() != 40) ? cipherPassword(newPassword) : newPassword;
        userDBAdaptor.changePassword(userId, oldCryptPass, newCryptPass);
    }

    @Override
    public void newPassword(String userId, String newPassword) throws CatalogException {
        String newCryptPass = (newPassword.length() != 40) ? cipherPassword(newPassword) : newPassword;
        userDBAdaptor.changePassword(userId, "", newCryptPass);
    }

    @Override
    public QueryResult resetPassword(String userId, String email) throws CatalogException {
        ParamUtils.checkParameter(userId, "userId");
        ParamUtils.checkParameter(email, "email");
        userDBAdaptor.updateUserLastActivity(userId);

        String newPassword = StringUtils.randomString(6);

        String newCryptPass = cipherPassword(newPassword);

        QueryResult qr = userDBAdaptor.resetPassword(userId, email, newCryptPass);

        String mailUser = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_USER);
        String mailPassword = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_PASSWORD);
        String mailHost = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_HOST);
        String mailPort = catalogProperties.getProperty(CatalogManager.CATALOG_MAIL_PORT);

        MailUtils.sendResetPasswordMail(email, newPassword, mailUser, mailPassword, mailHost, mailPort);

        return qr;
    }

}
