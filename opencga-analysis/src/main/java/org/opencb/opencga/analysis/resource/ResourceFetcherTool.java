package org.opencb.opencga.analysis.resource;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.resource.ResourceFetcherToolParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.opencb.opencga.catalog.utils.ResourceManager.RESOURCES_DIRNAME;

@Tool(id= ResourceFetcherTool.ID,
        resource = Enums.Resource.RESOURCE,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.GLOBAL,
        description = ResourceFetcherTool.DESCRIPTION,
        priority = Enums.Priority.HIGH)
public class ResourceFetcherTool extends OpenCgaTool {

    public static final String ID = "resource-fetcher";
    public static final String DESCRIPTION = "Fetch all resources from the public server and save them into the OpenCGA local installation";

    public static final String ALL_RESOURCES = "all";

    private Path resourcePath;

    @ToolParams
    protected final ResourceFetcherToolParams analysisParams = new ResourceFetcherToolParams();

    @Override
    protected void check() throws Exception {
        super.check();

        resourcePath = getOutDir().resolve(RESOURCES_DIRNAME);
        if (!Files.exists(Files.createDirectories(resourcePath))) {
            throw new IOException("Error creating resource directory '" + resourcePath.toAbsolutePath() + "'");
        }
    }


    @Override
    protected void run() throws Exception {
        // Download all resources
        step(ID, this::fetchResources);
    }

    private void fetchResources() throws ResourceException, ToolException {
        if (CollectionUtils.isEmpty(analysisParams.getResources())) {
            addWarning("Nothing to fetch since input resource list is empty");
            return;
        }
        ResourceManager resourceManager = new ResourceManager(getOpencgaHome());
        if (analysisParams.getResources().contains(ALL_RESOURCES)) {
            resourceManager.fetchAllResources(getOutDir().resolve(RESOURCES_DIRNAME), catalogManager, token);
        } else {
            resourceManager.fetchResources(analysisParams.getResources(), getOutDir().resolve(RESOURCES_DIRNAME), catalogManager, token);
        }
    }
}
