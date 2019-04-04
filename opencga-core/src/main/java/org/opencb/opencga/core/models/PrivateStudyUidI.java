package org.opencb.opencga.core.models;

public interface PrivateStudyUidI extends PrivateFieldsI {

    String getId();

    long getStudyUid();

    PrivateStudyUidI setStudyUid(long studyUid);

}
