package org.opencb.opencga.core.models.common;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public abstract class Internal {

    @DataField(id = "status",
            description = FieldConstants.INTERNAL_STATUS_DESCRIPTION)
    protected Status status;

    @DataField(id = "registrationDate",
            description = FieldConstants.INTERNAL_REGISTRATION_DATE_DESCRIPTION)
    protected String registrationDate;

    @DataField(id = "lastModified",
            description = FieldConstants.INTERNAL_LAST_MODIFIED_DESCRIPTION)
    protected String lastModified;

    public Internal() {
    }

    public Internal(Status status, String registrationDate, String lastModified) {
        this.status = status;
        this.registrationDate = registrationDate;
        this.lastModified = lastModified;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Internal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Status getStatus() {
        return status;
    }

    public Internal setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getRegistrationDate() {
        return registrationDate;
    }

    public Internal setRegistrationDate(String registrationDate) {
        this.registrationDate = registrationDate;
        return this;
    }

    public String getLastModified() {
        return lastModified;
    }

    public Internal setLastModified(String lastModified) {
        this.lastModified = lastModified;
        return this;
    }
}
