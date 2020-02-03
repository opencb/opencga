package org.opencb.opencga.core.models.family;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"uid", "studyUid", "creationDate", "status", "release", "version"})
public abstract class FamilyMixin {
}
