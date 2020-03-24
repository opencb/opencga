package org.opencb.opencga.core.models.project;

import org.opencb.opencga.core.models.common.Status;

public class ProjectInternal {

    private Datastores datastores;
    private Status status;

    public ProjectInternal() {
    }

    public ProjectInternal(Datastores datastores, Status status) {
        this.datastores = datastores;
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectInternal{");
        sb.append("datastores=").append(datastores);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public Datastores getDatastores() {
        return datastores;
    }

    public ProjectInternal setDatastores(Datastores datastores) {
        this.datastores = datastores;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public ProjectInternal setStatus(Status status) {
        this.status = status;
        return this;
    }
}
