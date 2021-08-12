package org.opencb.opencga.core.models.common;

public abstract class Internal {

    protected Status status;
    protected String registrationDate;
    protected String modificationDate;

    public Internal() {
    }

    public Internal(Status status, String registrationDate, String modificationDate) {
        this.status = status;
        this.registrationDate = registrationDate;
        this.modificationDate = modificationDate;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Internal{");
        sb.append("status=").append(status);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
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

    public String getModificationDate() {
        return modificationDate;
    }

    public Internal setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }
}
