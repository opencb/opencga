package org.opencb.opencga.core.models.externalTool;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"user", "password"})
public interface ContainerRedactedMixin {
}
