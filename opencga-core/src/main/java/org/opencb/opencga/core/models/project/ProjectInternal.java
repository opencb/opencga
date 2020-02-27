package org.opencb.opencga.core.models.project;

public class ProjectInternal {

    private DataStores datastores;
//    private Status status; // TODO Move status Project


    public DataStores getDatastores() {
        return datastores;
    }

    public ProjectInternal setDatastores(DataStores datastores) {
        this.datastores = datastores;
        return this;
    }

}
