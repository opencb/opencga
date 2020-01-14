package org.opencb.opencga.core.models.panel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"uid", "studyUid", "release", "version", "creationDate", "status"})
public interface PanelMixin {
//public interface PanelMixin extends PanelUnwrapMixin {

}
