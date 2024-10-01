package org.opencb.opencga.analysis.customTool;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.opencga.analysis.tools.OpenCgaDockerToolScopeStudy;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.JobRunDockerParams;
import org.opencb.opencga.core.models.job.JobRunParams;
import org.opencb.opencga.core.models.job.ToolInfoExecutor;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

@Tool(id = CustomToolExecutor.ID, resource = Enums.Resource.JOB, description = CustomToolExecutor.DESCRIPTION)
public class CustomToolExecutor extends OpenCgaDockerToolScopeStudy {

    public final static String ID = "custom-tool";
    public static final String DESCRIPTION = "Execute an analysis from a custom binary.";

    @ToolParams
    protected JobRunParams runParams = new JobRunParams();

    private String cliParams;
    private String dockerImage;

    private final static Logger logger = LoggerFactory.getLogger(CustomToolExecutor.class);

    @Override
    protected void check() throws Exception {
        super.check();

        // Check any condition
        if (runParams == null) {
            throw new ToolException("Missing runParams");
        }
        if (StringUtils.isEmpty(runParams.getCommandLine())) {
            throw new ToolException("Missing commandLine");
        }
        if (runParams.getDocker() == null || StringUtils.isEmpty(runParams.getDocker().getId())) {
            runParams.setDocker(new JobRunDockerParams("opencb/opencga-ext-tools", "3.2.1", null));
        }
        if (!runParams.getDocker().getId().contains("/")) {
            throw new ToolException("Missing repository organization. Format for the docker image should be 'organization/image'");
        }
        this.dockerImage = runParams.getDocker().getId();
        if (StringUtils.isNotEmpty(runParams.getDocker().getTag())) {
            this.dockerImage += ":" + runParams.getDocker().getTag();
        }

        // Update job tags and attributes
        ToolInfoExecutor toolInfoExecutor = new ToolInfoExecutor(runParams.getDocker().getId(), runParams.getDocker().getTag());
        List<String> tags = new LinkedList<>();
        tags.add(ID);
        tags.add(this.dockerImage);
        updateJobInformation(tags, toolInfoExecutor);

        StringBuilder cliParamsBuilder = new StringBuilder();
        processInputParams(runParams.getCommandLine(), cliParamsBuilder);
        this.cliParams = cliParamsBuilder.toString();
    }

    @Override
    protected void run() throws Exception {
        StopWatch stopWatch = StopWatch.createStarted();
        String cmdline = runDocker(dockerImage, cliParams);
        logger.info("Docker command line: " + cmdline);
        logger.info("Execution time: " + TimeUtils.durationToString(stopWatch));
    }

}
