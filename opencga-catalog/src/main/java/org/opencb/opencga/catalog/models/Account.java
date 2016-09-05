package org.opencb.opencga.catalog.models;

/**
 * Created by pfurio on 02/09/16.
 */
public class Account {

    public static final String GUEST = "guest";
    public static final String FULL = "full";

    private String type;
    private String expirationDate;
    private String creationDate;
    private String authMode;

    public Account() {
    }

    public Account(String type, String expirationDate, String creationDate, String authMode) {
        this.type = type;
        this.expirationDate = expirationDate;
        this.creationDate = creationDate;
        this.authMode = authMode;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Account{");
        sb.append("type='").append(type).append('\'');
        sb.append(", expirationDate='").append(expirationDate).append('\'');
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", authMode='").append(authMode).append('\'');
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

    public String getAuthMode() {
        return authMode;
    }

    public Account setAuthMode(String authMode) {
        this.authMode = authMode;
        return this;
    }
}
