package org.opencb.opencga.core.models;

abstract public class PrivateFields {

    private long uid;

    public PrivateFields() {
    }

    public PrivateFields(long uid) {
        this.uid = uid;
    }

    public long getUid() {
        return uid;
    }

    public PrivateFields setUid(long uid) {
        this.uid = uid;
        return this;
    }
}
