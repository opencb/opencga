package org.opencb.opencga.analysis.resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.config.Resource;
import org.opencb.opencga.core.config.ResourceFile;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.resource.ResourceFetcherToolParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.opencb.opencga.catalog.utils.ResourceManager.RESOURCES_DIRNAME;

@Tool(id= ResourceFetcherTool.ID,
        resource = Enums.Resource.RESOURCE,
        type = Tool.Type.OPERATION,
        scope = Tool.Scope.GLOBAL,
        description = ResourceFetcherTool.DESCRIPTION,
        priority = Enums.Priority.HIGH)
public class ResourceFetcherTool extends OpenCgaTool {

    public static final String ID = "resource-fetcher";
    public static final String DESCRIPTION = "Fetch resources from the public server and save them into the OpenCGA local installation";

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

    private void fetchResources() throws ResourceException, ToolException, IOException {
        String msg;
        if (CollectionUtils.isEmpty(analysisParams.getResources())) {
            msg = "There are no resources to fetch because the input resource list is empty.";
            addInfo(msg);
            logger.info(msg);
            return;
        }
        ResourceManager resourceManager = new ResourceManager(getOpencgaHome());
        Resource resourceConfig = configuration.getAnalysis().getResource();

        Set<String> resourceIds = new HashSet<>();
        for (String inputPattern : analysisParams.getResources()) {
            // Convert the input pattern to a regex and compile the pattern
            String regex = inputPattern.replace("*", ".*");
            Pattern pattern = Pattern.compile(regex);

            // Filter the resource IDs using regex
            for (ResourceFile resourceFile : resourceConfig.getFiles()) {
                if (pattern.matcher(resourceFile.getId()).matches()) {
                    resourceIds.add(resourceFile.getId());
                }
            }
        }

        if (CollectionUtils.isEmpty(resourceIds)) {
            msg = "No resources to fetch from the input resource list: " + StringUtils.join(analysisParams.getResources(), ", ");
            addInfo(msg);
            logger.info(msg);
        } else {
            msg = "Fetching resources: " + StringUtils.join(resourceIds, ", ");
            addInfo(msg);
            logger.info(msg);
            resourceManager.fetchResources(new ArrayList<>(resourceIds), getOutDir().resolve(RESOURCES_DIRNAME), catalogManager, token);
        }
    }
}
