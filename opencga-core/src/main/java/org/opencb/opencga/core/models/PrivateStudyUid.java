package org.opencb.opencga.core.models;

abstract public class PrivateStudyUid extends PrivateFields {

    private long studyUid;

    public PrivateStudyUid() {
    }

    public PrivateStudyUid(long studyUid) {
        this.studyUid = studyUid;
    }

    public long getStudyUid() {
        return studyUid;
    }

    public PrivateStudyUid setStudyUid(long studyUid) {
        this.studyUid = studyUid;
        return this;
    }
}
