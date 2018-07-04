package org.opencb.opencga.server.rest.json.mixin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"uid", "studyUid", "release", "version", "creationDate", "status"})
public class PanelMixin {
}
