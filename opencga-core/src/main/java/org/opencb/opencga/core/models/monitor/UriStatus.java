package org.opencb.opencga.core.models.monitor;

import org.opencb.opencga.core.config.UriCheck;

public class UriStatus {

    private String path;
    private UriCheck.Permission permission;
    private HealthCheckResponse.Status status;
    private String exception;

    public UriStatus() {
    }

    public UriStatus(String path, UriCheck.Permission permission, HealthCheckResponse.Status status, String exception) {
        this.path = path;
        this.permission = permission;
        this.status = status;
        this.exception = exception;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UriStatus{");
        sb.append("path='").append(path).append('\'');
        sb.append(", permission=").append(permission);
        sb.append(", status=").append(status);
        sb.append(", exception='").append(exception).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getPath() {
        return path;
    }

    public UriStatus setPath(String path) {
        this.path = path;
        return this;
    }

    public UriCheck.Permission getPermission() {
        return permission;
    }

    public UriStatus setPermission(UriCheck.Permission permission) {
        this.permission = permission;
        return this;
    }

    public HealthCheckResponse.Status getStatus() {
        return status;
    }

    public UriStatus setStatus(HealthCheckResponse.Status status) {
        this.status = status;
        return this;
    }

    public String getException() {
        return exception;
    }

    public UriStatus setException(String exception) {
        this.exception = exception;
        return this;
    }
}
