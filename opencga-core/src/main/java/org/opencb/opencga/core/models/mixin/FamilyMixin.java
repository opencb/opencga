package org.opencb.opencga.core.models.mixin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"uid", "studyUid", "creationDate", "status", "release", "version"})
public abstract class FamilyMixin {
}
