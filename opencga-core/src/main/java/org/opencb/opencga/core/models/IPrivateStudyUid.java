package org.opencb.opencga.core.models;

public interface IPrivateStudyUid extends IPrivateFields {

    String getId();

    String getUuid();

    long getStudyUid();

    IPrivateStudyUid setStudyUid(long studyUid);

}
