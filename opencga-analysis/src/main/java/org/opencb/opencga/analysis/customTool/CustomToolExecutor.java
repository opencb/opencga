package org.opencb.opencga.analysis.customTool;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaDockerToolScopeStudy;
import org.opencb.opencga.catalog.db.api.ExternalToolDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.externalTool.*;
import org.opencb.opencga.core.models.externalTool.custom.CustomToolParams;
import org.opencb.opencga.core.models.externalTool.custom.CustomToolRunParams;
import org.opencb.opencga.core.models.job.ToolInfoExecutor;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.ToolDependency;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tool(id = CustomToolExecutor.ID, resource = Enums.Resource.JOB, description = CustomToolExecutor.DESCRIPTION)
public class CustomToolExecutor extends OpenCgaDockerToolScopeStudy {

    public final static String ID = "custom-tool";
    public static final String DESCRIPTION = "Execute an analysis from a custom binary.";

    @ToolParams
    protected CustomToolParams runParams = new CustomToolParams();

    private String cliParams;
    private String dockerImage;

    // Docker credentials
    private String username;
    private String password;

    private final static Logger logger = LoggerFactory.getLogger(CustomToolExecutor.class);

    @Override
    protected void check() throws Exception {
        super.check();

        // Check any condition
        if (runParams == null) {
            throw new ToolException("Missing ExternalToolRunParams object.");
        } else if (StringUtils.isNotEmpty(runParams.getId()) && runParams.getContainer() == null) {
            Container container = generateDockerObject(runParams);
            checkDockerObject(container);
        } else if (runParams.getContainer() != null && StringUtils.isEmpty(runParams.getId())) {
            checkDockerObject(runParams.getContainer());
        } else {
            throw new ToolException("Unexpected ExternalToolRunParams object. "
                    + "Either 'id' and 'params/commandLine' or 'docker' should be set.");
        }
    }

    private Container generateDockerObject(ExternalToolParams<CustomToolRunParams> runParams) throws CatalogException, ToolException {
        OpenCGAResult<ExternalTool> result;
        if (runParams.getVersion() != null) {
            Query query = new Query(ExternalToolDBAdaptor.QueryParams.VERSION.key(), runParams.getVersion());
            result = catalogManager.getExternalToolManager().get(study, Collections.singletonList(runParams.getId()), query,
                    QueryOptions.empty(), false, token);
        } else {
            result = catalogManager.getExternalToolManager().get(study, runParams.getId(), QueryOptions.empty(), token);
        }
        if (result.getNumResults() == 0) {
            throw new ToolException("Custom tool '" + runParams.getId() + "' not found");
        }
        ExternalTool externalTool = result.first();

        if (externalTool == null) {
            throw new ToolException("Custom tool '" + runParams.getId() + "' is null");
        }
        if (externalTool.getType() != ExternalToolType.CUSTOM) {
            throw new ToolException("User tool '" + runParams.getId() + "' is not of type " + ExternalToolType.CUSTOM);
        }
        if (externalTool.getContainer() == null) {
            throw new ToolException("User tool '" + runParams.getId() + "' does not have a docker object");
        }

        Container container = externalTool.getContainer();
        String commandLine = runParams.getParams() != null && StringUtils.isNotEmpty(runParams.getParams().getCommandLine())
                ? runParams.getParams().getCommandLine()
                : container.getCommandLine();

        // Process docker command line to replace variables
        Map<String, String> params = new HashMap<>();
        if (runParams.getParams() != null) {
            for (Map.Entry<String, Object> entry : runParams.getParams().toParams().entrySet()) {
                params.put(entry.getKey(),  entry.getValue().toString());
            }
        }
        if (CollectionUtils.isNotEmpty(externalTool.getVariables())) {
            for (ExternalToolVariable variable : externalTool.getVariables()) {
                String variableId = removePrefix(variable.getId());
                if (!params.containsKey(variableId) && StringUtils.isNotEmpty(variable.getDefaultValue())) {
                    params.put(variableId, variable.getDefaultValue());
                }
            }
        }
        String processedCli = inputFileUtils.processCommandLine(study, commandLine, params, temporalInputDir, dockerInputBindings,
                getOutDir().toString(), token);
        container.setCommandLine(processedCli);

        return container;
    }

    private void checkDockerObject(Container container) throws ToolException, CatalogException {
        if (StringUtils.isEmpty(container.getName())) {
            container.setName("opencb/opencga-ext-tools");
            container.setTag(GitRepositoryState.getInstance().getBuildVersion());
        }
        this.dockerImage = container.getName();
        String tag= "";
        if (StringUtils.isNotEmpty(container.getTag())) {
            tag = container.getTag();
            if (StringUtils.isNotEmpty(container.getDigest())) {
                tag += "@" + container.getDigest();
            }
        }
        if (StringUtils.isNotEmpty(tag)) {
            this.dockerImage += ":" + tag;
        }

        // Update job tags and attributes
        ToolInfoExecutor toolInfoExecutor = new ToolInfoExecutor(container.getName(), tag);
        List<String> tags = new LinkedList<>();
        tags.add(ID);
        tags.add(this.dockerImage);

        List<ToolDependency> dependencyList = new ArrayList<>(1);
        dependencyList.add(new ToolDependency(container.getName(), tag));
        addDependencies(dependencyList);
        updateJobInformation(tags, toolInfoExecutor);

        this.cliParams = container.getCommandLine();
        this.username = container.getUser();
        this.password = container.getPassword();
    }

    @Override
    protected void run() throws Exception {
        StopWatch stopWatch = StopWatch.createStarted();

        List<AbstractMap.SimpleEntry<String, String>> outputBindings = new ArrayList<>(1);
        String outDirPath = getOutDir().toAbsolutePath().toString();
        outputBindings.add(new AbstractMap.SimpleEntry<>(outDirPath, outDirPath));

        Map<String, String> dockerParams = new HashMap<>();
        dockerParams.put("-e", "OPENCGA_TOKEN=" + getExpiringToken());
        String cmdline = runDocker(dockerImage, outputBindings, cliParams, dockerParams, null, username, password);

        logger.info("Docker command line: {}", cmdline);
        logger.info("Execution time: {}", TimeUtils.durationToString(stopWatch));
    }

}
