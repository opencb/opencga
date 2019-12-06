package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.tools.result.FileResult;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.*;

@Tool(id = DeeptoolsWrapperAnalysis.ID, type = Tool.ToolType.ALIGNMENT, description = DeeptoolsWrapperAnalysis.DESCRIPTION)
public class DeeptoolsWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "deeptools";
    public final static String DESCRIPTION = "Deeptools is a suite of python tools particularly developed for the efficient analysis of"
        + " high-throughput sequencing data, such as ChIP-seq, RNA-seq or MNase-seq.";

    public final static String DEEPTOOLS_DOCKER_IMAGE = "dhspence/docker-deeptools";

    private String executable;
    private String bamFile;
    private String coverageFile;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(executable)) {
            throw new ToolException("Missing deeptools executable. Supported executable is 'bamCoverage'");
        }

        switch (executable) {
            case "bamCoverage":
                if (StringUtils.isEmpty(bamFile)) {
                    throw new ToolException("Missing BAM file when executing 'deeptools " + executable + "'.");
                }
                if (StringUtils.isEmpty(coverageFile)) {
                    throw new ToolException("Missing coverage file when executing 'deeptools " + executable + "'.");
                }
                break;
            default:
                // TODO: support the remaining deeptools executable
                throw new ToolException("Deeptools executable '" + executable + "' is not available. Supported executable is"
                        + " 'bamCoverage'");
        }

    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Deeptools command line: " + commandLine);
            try {
                Set<String> filenamesBeforeRunning = new HashSet<>(getFilenames(getOutDir()));

                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(getCommandLine())
                        .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Add the output files to the analysis result file
                List<String> outNames = getFilenames(getOutDir());
                for (String name : outNames) {
                    if (!filenamesBeforeRunning.contains(name)) {
                        if (FileUtils.sizeOf(new File(getOutDir() + "/" + name)) > 0) {
                            FileResult.FileType fileType = FileResult.FileType.TAB_SEPARATED;
                            if (name.endsWith("txt") || name.endsWith("log")) {
                                fileType = FileResult.FileType.PLAIN_TEXT;
                            } else if (name.endsWith("bw") || name.endsWith("bigwig")  || name.endsWith("bedgraph")) {
                                fileType = FileResult.FileType.BINARY;
                            }
                            addFile(getOutDir().resolve(name), fileType);
                        }
                    }
                }

                // Check deeptools errors
                boolean success = false;
                switch (executable) {
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
        updateSrcTargetMap(bamFile, sb, srcTargetMap);

        sb.append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ");

        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // Deeptools executable
        sb.append(" ").append(executable);

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

        switch (executable) {
            case "bamCoverage": {
                File file = new File(bamFile);
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
        } else if ("bamCoverage".equals(executable)) {
            if ("o".equals(param) || "b".equals(param)) {
                return false;
            }
        }
        return true;
    }

    public String getExecutable() {
        return executable;
    }

    public DeeptoolsWrapperAnalysis setExecutable(String executable) {
        this.executable = executable;
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
