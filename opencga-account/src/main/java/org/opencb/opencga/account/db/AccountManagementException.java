package org.opencb.opencga.account.db;


import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.ObjectMap;

public class AccountManagementException extends Exception {

    private static final long serialVersionUID = 1L;

    public AccountManagementException(String msg) {
        super(msg);
    }
}
