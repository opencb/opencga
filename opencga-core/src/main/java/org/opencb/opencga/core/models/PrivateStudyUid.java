package org.opencb.opencga.core.models;

abstract public class PrivateStudyUid extends PrivateFields implements IPrivateStudyUid {

    private String id;
    private long studyUid;

    public PrivateStudyUid() {
    }

    public PrivateStudyUid(long studyUid) {
        this.studyUid = studyUid;
    }

    @Override
    public long getStudyUid() {
        return studyUid;
    }

    @Override
    public PrivateStudyUid setStudyUid(long studyUid) {
        this.studyUid = studyUid;
        return this;
    }

    public String getId() {
        return id;
    }

    public PrivateStudyUid setId(String id) {
        this.id = id;
        return this;
    }
}
