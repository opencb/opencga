package org.opencb.opencga.core.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"studyUid", "uid"})
public abstract class PrivateUidMixin {
}
