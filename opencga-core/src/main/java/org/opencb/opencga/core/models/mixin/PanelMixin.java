package org.opencb.opencga.core.models.mixin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"uid", "studyUid", "release", "version", "creationDate", "status"})
public class PanelMixin {
}
