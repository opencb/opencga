package org.opencb.opencga.core.models;

abstract public class PrivateFields implements IPrivateFields {

    private long uid;

    public PrivateFields() {
    }

    public PrivateFields(long uid) {
        this.uid = uid;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public PrivateFields setUid(long uid) {
        this.uid = uid;
        return this;
    }
}
