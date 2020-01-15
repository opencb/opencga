package org.opencb.opencga.core.models.file;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"uid", "studyUid", "release", "external", "size"})
public class FileMixin {
}
