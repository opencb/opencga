package org.opencb.opencga.analysis.wrappers.fastqc;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@ToolExecutor(id = FastqcWrapperAnalysisExecutor.ID,
        tool = FastqcWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class FastqcWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public final static String ID = FastqcWrapperAnalysis.ID + "-local";

    private String study;
    private String inputFile;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String getDockerImageName() {
        return "dceoy/fastqc";
    }

    @Override
    public String getDockerImageVersion() {
        return "";
    }

    @Override
    protected void run() throws Exception {

        String commandLine = getCommandLine();

        logger.info("Fastqc command line: " + commandLine);
        try {
            // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
            Command cmd = new Command(commandLine)
                    .setOutputOutputStream(
                            new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDOUT_FILENAME).toFile())))
                    .setErrorOutputStream(
                            new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDERR_FILENAME).toFile())));

            cmd.run();

        } catch (Exception e) {
            throw new ToolException(e);
        }
    }

    public String getCommandLine() throws ToolException {

        List<String> inputFiles = Arrays.asList(getInputFile());

        Map<String, String> srcTargetMap = getDockerMountMap(inputFiles);

        StringBuilder sb = initDockerCommandLine(srcTargetMap, getDockerImageName(), getDockerImageVersion());

        // Input file parameter
        File file;
        file = new File(getInputFile());
        sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());

        // Fastqc options
        for (String param : getExecutorParams().keySet()) {
            if (skipParameter(param)) {
                String sep = param.length() == 1 ? " -" : " --";
                String value = getExecutorParams().getString(param);
                if (StringUtils.isEmpty(value)) {
                    sb.append(sep).append(param);
                } else {
                    switch (value.toLowerCase()) {
                        case "false":
                            // Nothing to do
                            break;
                        case "null":
                        case "true":
                            // Only param must be appended
                            sb.append(sep).append(param);
                            break;
                        default:
                            // Otherwise, param + value must be appended
                            sb.append(sep).append(param).append(" ").append(value);
                            break;
                    }
                }
            }
        }

        // Output dir
        sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/");

        return sb.toString();
    }

    @Override
    protected boolean skipParameter(String param) {
        if (!super.skipParameter(param)) {
            return false;
        }

        switch (param) {
            case "dir":
            case "d":
            case "outdir":
            case "o": {
                return false;
            }
        }

        return true;
    }

    public String getStudy() {
        return study;
    }

    public FastqcWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public String getInputFile() {
        return inputFile;
    }

    public FastqcWrapperAnalysisExecutor setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }

}
