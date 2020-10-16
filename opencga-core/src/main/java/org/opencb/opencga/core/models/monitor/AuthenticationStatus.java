package org.opencb.opencga.core.models.monitor;

public class AuthenticationStatus {

    private String id;
    private String type;
    private String url;
    private HealthCheckResponse.Status status;
    private String exception;

    public AuthenticationStatus() {
    }

    public AuthenticationStatus(String id, String type, String url, HealthCheckResponse.Status status, String exception) {
        this.id = id;
        this.type = type;
        this.url = url;
        this.status = status;
        this.exception = exception;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AuthenticationStatus{");
        sb.append("id='").append(id).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append(", status=").append(status);
        sb.append(", exception='").append(exception).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public AuthenticationStatus setId(String id) {
        this.id = id;
        return this;
    }

    public String getType() {
        return type;
    }

    public AuthenticationStatus setType(String type) {
        this.type = type;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public AuthenticationStatus setUrl(String url) {
        this.url = url;
        return this;
    }

    public HealthCheckResponse.Status getStatus() {
        return status;
    }

    public AuthenticationStatus setStatus(HealthCheckResponse.Status status) {
        this.status = status;
        return this;
    }

    public String getException() {
        return exception;
    }

    public AuthenticationStatus setException(String exception) {
        this.exception = exception;
        return this;
    }
}
