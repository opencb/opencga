package org.opencb.opencga.account.db;

import org.opencb.opencga.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class AccountFileManager /* implements AccountManager */ {

    protected static Logger logger = LoggerFactory.getLogger(AccountFileManager.class);
    private Properties accountProperties;

    private String home;
    private String accounts;
    private String tmp;

    public AccountFileManager() throws IOException {
        accountProperties = Config.getAccountProperties();
        home = System.getenv(accountProperties.getProperty("OPENCGA.ENV.HOME"));
        accounts = home + accountProperties.getProperty("OPENCGA.ACCOUNT.PATH");
        tmp = accountProperties.getProperty("OPENCGA.TMP.PATH");
    }

}
