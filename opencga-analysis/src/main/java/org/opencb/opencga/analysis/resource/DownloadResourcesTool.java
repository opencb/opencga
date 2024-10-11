package org.opencb.opencga.analysis.resource;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.resource.DownloadResourcesToolParams;
import org.opencb.opencga.core.tools.ResourceManager;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

@Tool(id= DownloadResourcesTool.ID,
        resource = Enums.Resource.RESOURCE,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.GLOBAL,
        description = DownloadResourcesTool.DESCRIPTION,
        priority = Enums.Priority.HIGH)
public class DownloadResourcesTool extends OpenCgaTool {

    public static final String ID = "resource-download-all";
    public static final String DESCRIPTION = "Download all resources";

    @ToolParams
    protected final DownloadResourcesToolParams analysisParams = new DownloadResourcesToolParams();

    @Override
    protected void run() throws Exception {
        // Download all resources
        step(ID, this::downloadResources);
    }

    private void downloadResources() throws IOException, NoSuchAlgorithmException {
        ResourceManager resourceManager = new ResourceManager(getOpencgaHome(), analysisParams.getBaseurl());
        resourceManager.downloadAllResources(Boolean.TRUE.equals(analysisParams.getOverwrite()));
    }
}
