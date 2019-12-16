package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.models.common.Enums;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

@Tool(id = DeeptoolsWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = DeeptoolsWrapperAnalysis.DESCRIPTION)
public class DeeptoolsWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "deeptools";
    public final static String DESCRIPTION = "Deeptools is a suite of python tools particularly developed for the efficient analysis of"
        + " high-throughput sequencing data, such as ChIP-seq, RNA-seq or MNase-seq.";

    public final static String DEEPTOOLS_DOCKER_IMAGE = "dhspence/docker-deeptools";

    private String command;
    private String bamFile;
    private String coverageFile;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(command)) {
            throw new ToolException("Missing deeptools command. Supported command is 'bamCoverage'");
        }

        switch (command) {
            case "bamCoverage":
                if (StringUtils.isEmpty(bamFile)) {
                    throw new ToolException("Missing BAM file when executing 'deeptools " + command + "'.");
                }
                if (StringUtils.isEmpty(coverageFile)) {
                    throw new ToolException("Missing coverage file when executing 'deeptools " + command + "'.");
                }
                break;
            default:
                // TODO: support the remaining deeptools commands
                throw new ToolException("Deeptools command '" + command + "' is not available. Supported command is"
                        + " 'bamCoverage'");
        }

    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Deeptools command line: " + commandLine);
            try {
                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(getCommandLine())
                        .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Check deeptools errors
                boolean success = false;
                switch (command) {
                    case "bamCoverage": {
                        if (new File(coverageFile).exists()) {
                            success = true;
                        }
                        break;
                    }
                }
                if (!success) {
                    File file = new File(getOutDir() + "/" + STDERR_FILENAME);
                    String msg = "Something wrong executing Samtools";
                    if (file.exists()) {
                        msg = StringUtils.join(FileUtils.readLines(file, Charset.defaultCharset()), ". ");
                    }
                    throw new ToolException(msg);
                }
            } catch (Exception e) {
                throw new ToolException(e);
            }
        });
    }

    @Override
    public String getDockerImageName() {
        return DEEPTOOLS_DOCKER_IMAGE;
    }

    @Override
    public String getCommandLine() throws ToolException {
        StringBuilder sb = new StringBuilder("docker run ");

        // Mount management
        Map<String, String> srcTargetMap = new HashMap<>();
        updateFileMaps(bamFile, sb, fileUriMap, srcTargetMap);

        sb.append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ");

        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // Deeptools command
        sb.append(" ").append(command);

        // Deeptools options
        for (String param : params.keySet()) {
            if (checkParam(param)) {
                String value = params.getString(param);
                sb.append(" -").append(param);
                if (StringUtils.isNotEmpty(value)) {
                    sb.append(" ").append(value);
                }
            }
        }

        switch (command) {
            case "bamCoverage": {
                File file = new File(fileUriMap.get(bamFile).getPath());
                sb.append(" -b ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(new File(coverageFile).getName());
                break;
            }
        }

        return sb.toString();
    }

    private boolean checkParam(String param) {
        if (param.equals(DOCKER_IMAGE_VERSION_PARAM)) {
            return false;
        } else if ("bamCoverage".equals(command)) {
            if ("o".equals(param) || "b".equals(param)) {
                return false;
            }
        }
        return true;
    }

    public String getCommand() {
        return command;
    }

    public DeeptoolsWrapperAnalysis setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getBamFile() {
        return bamFile;
    }

    public DeeptoolsWrapperAnalysis setBamFile(String bamFile) {
        this.bamFile = bamFile;
        return this;
    }

    public String getCoverageFile() {
        return coverageFile;
    }

    public DeeptoolsWrapperAnalysis setCoverageFile(String coverageFile) {
        this.coverageFile = coverageFile;
        return this;
    }
}
