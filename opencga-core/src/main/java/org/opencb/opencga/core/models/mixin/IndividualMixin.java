package org.opencb.opencga.core.models.mixin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"uid", "studyUid", "fatherId", "motherId", "family", "species", "release", "version", "creationDate", "status"})
public class IndividualMixin {
}
