package org.opencb.opencga.core.models.project;

import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.file.File;

import java.util.Map;

public class ProjectInternal {

    private Map<File.Bioformat, DataStore> datastores;
    private Status status;

    public ProjectInternal() {
    }

    public ProjectInternal(Map<File.Bioformat, DataStore> datastores, Status status) {
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

    public Map<File.Bioformat, DataStore> getDatastores() {
        return datastores;
    }

    public ProjectInternal setDatastores(Map<File.Bioformat, DataStore> datastores) {
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
