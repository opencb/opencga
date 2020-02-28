package org.opencb.opencga.core.models.project;

import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.file.File;

import java.util.Map;

public class ProjectInternal {

    private Map<File.Bioformat, DataStore> dataStores;
    private Status status;

    public ProjectInternal() {
    }

    public ProjectInternal(Map<File.Bioformat, DataStore> dataStores, Status status) {
        this.dataStores = dataStores;
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ProjectInternal{");
        sb.append("dataStores=").append(dataStores);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public Map<File.Bioformat, DataStore> getDataStores() {
        return dataStores;
    }

    public ProjectInternal setDataStores(Map<File.Bioformat, DataStore> dataStores) {
        this.dataStores = dataStores;
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
