package org.opencb.opencga.core.models.common;

public class EntryParam {

    private String id;

    public EntryParam() {
    }

    public EntryParam(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public EntryParam setId(String id) {
        this.id = id;
        return this;
    }
}
