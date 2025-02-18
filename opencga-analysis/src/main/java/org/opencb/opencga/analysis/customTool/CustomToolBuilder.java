package org.opencb.opencga.analysis.customTool;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.JobToolBuildParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

@Tool(id = CustomToolBuilder.ID, resource = Enums.Resource.JOB, description = CustomToolBuilder.DESCRIPTION)
public class CustomToolBuilder extends OpenCgaToolScopeStudy {

    public static final String ID = "custom-tool-builder";
    public static final String DESCRIPTION = "Build an external bioinformatic tool from a GitHub repository URL.";

    private static final String PYTHON_SCRIPT_NAME = "custom-tool-docker-build.py";

    @ToolParams
    protected JobToolBuildParams toolBuildParams = new JobToolBuildParams();

    private final static Logger logger = LoggerFactory.getLogger(CustomToolBuilder.class);

    @Override
    protected void check() throws Exception {
        super.check();

        // Check any condition
        if (toolBuildParams == null) {
            throw new ToolException("Missing toolBuildParams");
        }
        if (StringUtils.isEmpty(toolBuildParams.getGitRepository())) {
            throw new ToolException("Missing Git repository URL");
        }
        if (toolBuildParams.getDocker() == null
                || StringUtils.isEmpty(toolBuildParams.getDocker().getOrganisation())
                || StringUtils.isEmpty(toolBuildParams.getDocker().getName())
                || StringUtils.isEmpty(toolBuildParams.getDocker().getTag())
                || StringUtils.isEmpty(toolBuildParams.getDocker().getUser())
                || StringUtils.isEmpty(toolBuildParams.getDocker().getPassword())
        ) {
            throw new ToolException("Missing Docker parameters, please provide the organisation, name, tag, user and password");
        }
        logger.debug("Checking finished, all seems fine");
    }

    @Override
    protected void run() throws Exception {
        // Build CLI params
        StringBuilder cliBuilder = new StringBuilder();

        // 1. Get the path to the custom-tool-docker-builder script
        Path customToolDockerBuilderPath = this.getOpencgaHome()
                .resolve("cloud")
                .resolve("docker")
                .resolve("custom-tool-docker-builder")
                .resolve(PYTHON_SCRIPT_NAME);
        logger.info("Executing custom-tool-docker-builder script: {}", customToolDockerBuilderPath);

        // 2. Prepare basic CLI params
        cliBuilder.append("python3").append(" ")
                .append(customToolDockerBuilderPath).append(" ")
                .append("push").append(" ")
                .append("-t").append(" ").append(toolBuildParams.getGitRepository()).append(" ")
                .append("-o").append(" ").append(toolBuildParams.getDocker().getOrganisation()).append(" ")
                .append("-n").append(" ").append(toolBuildParams.getDocker().getName()).append(" ")
                .append("-v").append(" ").append(toolBuildParams.getDocker().getTag()).append(" ")
                .append("-u").append(" ").append(toolBuildParams.getDocker().getUser()).append(" ")
                .append("-p").append(" ").append(toolBuildParams.getDocker().getPassword()).append(" ");

        // 3. Check if 'apt-get' is provided
        if (StringUtils.isNotEmpty(toolBuildParams.getAptGet())) {
            cliBuilder.append("--apt-get").append(" ").append(toolBuildParams.getAptGet()).append(" ");
        }

        // 4. Check if 'installR' is provided
        if (toolBuildParams.getInstallR() != null && toolBuildParams.getInstallR()) {
            cliBuilder.append("--install-r").append(" ");
        }

        // 5. Execute the script
        logger.info("CLI params: {}", cliBuilder);
        // 5.1 Split the CLI params into a list
        List<String> cliArgs = Arrays.asList(StringUtils.split(cliBuilder.toString(), " "));

        // 5.2 Statr the stop watch
        StopWatch stopWatch = StopWatch.createStarted();

        // 5.3 Execute Python CLI
        ProcessBuilder processBuilder = new ProcessBuilder(cliArgs);

        // 5.4 Set the working directory of the process
        processBuilder.directory(getOutDir().toFile());

        // 5.5 Start execution
        logger.info("Executing: {}", cliBuilder);
        Process p;
        try {
            p = processBuilder.start();
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line;
            while ((line = input.readLine()) != null) {
                logger.info("{}", line);
            }
            while ((line = error.readLine()) != null) {
                logger.error("{} ", line);
            }
            p.waitFor();
            input.close();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error executing cli: " + e.getMessage(), e);
        }
        logger.info("Execution time: {}", TimeUtils.durationToString(stopWatch));
    }

}
