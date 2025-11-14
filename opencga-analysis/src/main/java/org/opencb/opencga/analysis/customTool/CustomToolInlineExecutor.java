package org.opencb.opencga.analysis.customTool;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.opencga.analysis.tools.OpenCgaDockerToolScopeStudy;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.externalTool.Container;
import org.opencb.opencga.core.models.externalTool.custom.CustomToolInlineParams;
import org.opencb.opencga.core.models.job.ToolInfoExecutor;
import org.opencb.opencga.core.tools.ToolDependency;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tool(id = CustomToolInlineExecutor.ID, resource = Enums.Resource.JOB, description = CustomToolInlineExecutor.DESCRIPTION)
public class CustomToolInlineExecutor extends OpenCgaDockerToolScopeStudy {

    public final static String ID = "custom-inline-tool";
    public static final String DESCRIPTION = "Execute an analysis from a custom binary.";

    @ToolParams
    protected CustomToolInlineParams runParams = new CustomToolInlineParams();

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
            throw new ToolException("Missing CustomToolInlineParams object.");
        } else if (runParams.getContainer() == null || StringUtils.isEmpty(runParams.getContainer().getCommandLine())) {
            throw new ToolException("Missing docker container or command line to be executed.");
        }
        checkDockerObject(runParams.getContainer());
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

        // Process CLI
        this.cliParams = inputFileUtils.processCommandLine(study, container.getCommandLine(), Collections.emptyMap(), null,
                temporalInputDir, dockerInputBindings, getOutDir().toString(), token);
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
