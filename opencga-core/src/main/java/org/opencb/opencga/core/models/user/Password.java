package org.opencb.opencga.core.models.user;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class Password {

    @DataField(id = "expirationDate", since = "3.2.1", description = FieldConstants.INTERNAL_ACCOUNT_PASSWORD_EXPIRATION_DATE_DESCRIPTION)
    private String expirationDate;

    @DataField(id = "lastModified", since = "3.2.1", description = FieldConstants.INTERNAL_ACCOUNT_PASSWORD_LAST_MODIFIED_DESCRIPTION)
    private String lastModified;

    public Password() {
    }

    public Password(String expirationDate, String lastModified) {
        this.expirationDate = expirationDate;
        this.lastModified = lastModified;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Password{");
        sb.append("expirationDate='").append(expirationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getExpirationDate() {
        return expirationDate;
    }

    public Password setExpirationDate(String expirationDate) {
        this.expirationDate = expirationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public Password setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }
}
