package org.opencb.opencga.analysis.binary;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.DockerUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.analysis.utils.InputFileUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.JobRunDockerParams;
import org.opencb.opencga.core.models.job.JobRunParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Tool(id = BinaryExecutor.ID, resource = Enums.Resource.JOB, description = BinaryExecutor.DESCRIPTION)
public class BinaryExecutor extends OpenCgaToolScopeStudy {

    public final static String ID = "binary";
    public static final String DESCRIPTION = "Execute an analysis from a custom binary.";

    @ToolParams
    protected JobRunParams runParams = new JobRunParams();

    private String cliParams;
    // Build list of inputfiles in case we need to specifically mount them in read only mode
    List<AbstractMap.SimpleEntry<String, String>> inputBindings;
    private String dockerImage;

    private final static Logger logger = LoggerFactory.getLogger(BinaryExecutor.class);

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
        ObjectMap attributes = new ObjectMap()
                .append("DOCKER_ID", runParams.getDocker().getId())
                .append("DOCKER_TAG", runParams.getDocker().getTag());
        List<String> tags = new LinkedList<>();
        tags.add(ID);
        tags.add(this.dockerImage);
        updateJobInformation(tags, attributes);

        // Build input bindings
        this.inputBindings = new LinkedList<>();
        InputFileUtils inputFileUtils = new InputFileUtils(catalogManager);

        StringBuilder cliParamsBuilder = new StringBuilder();
        String[] params = runParams.getCommandLine().split(" ");
        Map<String, String> inputDirectoryMounts = new HashMap<>();
        for (String param : params) {
            if (inputFileUtils.isValidOpenCGAFile(param)) {
                File file = inputFileUtils.getOpenCGAFile(study, param, token);
                Path parent = Paths.get(file.getUri()).getParent();
                if (!inputDirectoryMounts.containsKey(parent.toString())) {
                    inputDirectoryMounts.put(parent.toString(), "/data/input" + inputDirectoryMounts.size());
                }
                String directoryMount = inputDirectoryMounts.get(parent.toString());
                inputBindings.add(new AbstractMap.SimpleEntry<>(parent.toString(), directoryMount));
                cliParamsBuilder.append(directoryMount).append("/").append(file.getName()).append(" ");
            } else {
                cliParamsBuilder.append(param).append(" ");
            }
        }
        this.cliParams = cliParamsBuilder.toString();
    }

    @Override
    protected void run() throws Exception {
        // Build output binding
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                "/data/output");

        StopWatch stopWatch = StopWatch.createStarted();
        String cmdline = DockerUtils.run(dockerImage, inputBindings, outputBinding, cliParams, null);
        logger.info("Docker command line: " + cmdline);
        logger.info("Execution time: " + TimeUtils.durationToString(stopWatch));
    }

}
