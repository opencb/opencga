package org.opencb.opencga.core.models.externalTool;

import java.util.Map;

public interface ExternalToolParams {

    String getId();

    Integer getVersion();

    Map<String, Object> toParams();

}
