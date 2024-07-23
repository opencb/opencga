package org.opencb.opencga.core.models.user;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class Password {

    @DataField(id = "expirationDate", since = "3.2.1", description = FieldConstants.INTERNAL_ACCOUNT_PASSWORD_EXPIRATION_DATE_DESCRIPTION)
    private String expirationDate;

    @DataField(id = "lastChangeDate", since = "3.2.1", description = FieldConstants.INTERNAL_ACCOUNT_PASSWORD_LAST_CHANGE_DATE_DESCRIPTION)
    private String lastChangeDate;

    public Password() {
    }

    public Password(String expirationDate, String lastChangeDate) {
        this.expirationDate = expirationDate;
        this.lastChangeDate = lastChangeDate;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Password{");
        sb.append("expirationDate='").append(expirationDate).append('\'');
        sb.append(", lastChangeDate='").append(lastChangeDate).append('\'');
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

    public String getLastChangeDate() {
        return lastChangeDate;
    }

    public Password setLastChangeDate(String lastChangeDate) {
        this.lastChangeDate = lastChangeDate;
        return this;
    }
}
