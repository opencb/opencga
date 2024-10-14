package org.opencb.opencga.analysis.resource;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.resource.DownloadResourcesToolParams;
import org.opencb.opencga.core.tools.ResourceManager;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.core.tools.ResourceManager.RESOURCES_FOLDER_NAME;

@Tool(id= DownloadResourcesTool.ID,
        resource = Enums.Resource.RESOURCE,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.GLOBAL,
        description = DownloadResourcesTool.DESCRIPTION,
        priority = Enums.Priority.HIGH)
public class DownloadResourcesTool extends OpenCgaTool {

    public static final String ID = "resource-download-all";
    public static final String DESCRIPTION = "Download all resources";

    public static final String DOWNLOAD_RESOURCES_STEP = "download-resources";
    public static final String MOVE_RESOURCES_STEP = "move-resources";

    private Path resourcePath;

    @ToolParams
    protected final DownloadResourcesToolParams analysisParams = new DownloadResourcesToolParams();

    protected List<String> getSteps() {
        return Arrays.asList(DOWNLOAD_RESOURCES_STEP, MOVE_RESOURCES_STEP);
    }

    @Override
    protected void check() throws Exception {
        super.check();

        resourcePath = getOutDir().resolve(RESOURCES_FOLDER_NAME);
        if (!Files.exists(Files.createDirectories(resourcePath))) {
            throw new IOException("Error creating resource directory '" + resourcePath.toAbsolutePath() + "'");
        }
    }


    @Override
    protected void run() throws Exception {
        // Download all resources
        step(DOWNLOAD_RESOURCES_STEP, this::downloadResources);

        // Move resources to the installation folder
        step(MOVE_RESOURCES_STEP, this::moveResources);
    }

    private void downloadResources() throws IOException, NoSuchAlgorithmException {
        ResourceManager resourceManager = new ResourceManager(getOpencgaHome(), analysisParams.getBaseurl());
        resourceManager.downloadAllResources(getOutDir().resolve(RESOURCES_FOLDER_NAME),
                Boolean.TRUE.equals(analysisParams.getOverwrite()));
    }

    private void moveResources() {

    }


}
