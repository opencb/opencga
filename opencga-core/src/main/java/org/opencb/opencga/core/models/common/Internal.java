package org.opencb.opencga.core.models.common;

public abstract class Internal {

    protected InternalStatus status;
    protected String registrationDate;
    protected String lastModified;

    public Internal() {
    }

    public Internal(InternalStatus status, String registrationDate, String lastModified) {
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

    public InternalStatus getStatus() {
        return status;
    }

    public Internal setStatus(InternalStatus status) {
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
