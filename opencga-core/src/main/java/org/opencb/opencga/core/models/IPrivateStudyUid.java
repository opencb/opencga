package org.opencb.opencga.core.models;

public interface IPrivateStudyUid extends IPrivateFields {

    String getId();

    long getStudyUid();

    IPrivateStudyUid setStudyUid(long studyUid);

}
