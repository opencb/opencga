package org.opencb.opencga.core.models.externalTool.workflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties({"user", "password"})
public interface WorkflowRepositoryMixin {
}
