package org.opencb.opencga.catalog.models;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Calendar;

/**
 * Created by pfurio on 02/09/16.
 */
public class Account {

    public static final String GUEST = "guest";
    public static final String FULL = "full";

    private String type;
    private String creationDate;
    private String expirationDate;
    private String authOrigin;

    public Account() {
        String creationDate = TimeUtils.getTime();

        Calendar cal = Calendar.getInstance();
        cal.setTime(TimeUtils.toDate(creationDate));
        cal.add(Calendar.YEAR, +1);
        String expirationDate = TimeUtils.getTime(cal.getTime());

        this.type = FULL;
        this.creationDate = creationDate;
        this.expirationDate = expirationDate;
        this.authOrigin = "internal";
    }

    public Account(String type, String creationDate, String expirationDate, String authOrigin) {
        this.type = type;
        this.expirationDate = expirationDate;
        this.creationDate = creationDate;
        this.authOrigin = authOrigin;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Account{");
        sb.append("type='").append(type).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", expirationDate='").append(expirationDate).append('\'');
        sb.append(", authOrigin='").append(authOrigin).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getType() {
        return type;
    }

    public Account setType(String type) {
        this.type = type;
        return this;
    }

    public String getExpirationDate() {
        return expirationDate;
    }

    public Account setExpirationDate(String expirationDate) {
        this.expirationDate = expirationDate;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Account setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getAuthOrigin() {
        return authOrigin;
    }

    public Account setAuthOrigin(String authOrigin) {
        this.authOrigin = authOrigin;
        return this;
    }
}
