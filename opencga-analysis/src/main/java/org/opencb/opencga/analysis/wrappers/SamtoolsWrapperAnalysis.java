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

@Tool(id = SamtoolsWrapperAnalysis.ID, type = Tool.ToolType.VARIANT, description = SamtoolsWrapperAnalysis.DESCRIPTION)
public class SamtoolsWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "samtools";
    public final static String DESCRIPTION = "Samtools is a program for interacting with high-throughput sequencing data in SAM, BAM"
            + " and CRAM formats.";

    public final static String SAMTOOLS_DOCKER_IMAGE = "zlskidmore/samtools";

    private String command;
    private String inputFile;
    private String outputFile;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(command)) {
            throw new ToolException("Missing samtools command. Supported commands are 'sort', 'index' and 'view'");
        }

        switch (command) {
            case "view":
            case "sort":
            case "index":
            case "stats":
                break;
            default:
                // TODO: support the remaining samtools commands
                throw new ToolException("Samtools command '" + command + "' is not available. Supported commands are 'sort', 'index'"
                        + " , 'view' and 'stats'");
        }

        if (StringUtils.isEmpty(inputFile)) {
            throw new ToolException("Missing input file when executing 'samtools " + command + "'.");
        }

        if (StringUtils.isEmpty(outputFile)) {
            throw new ToolException("Missing input file when executing 'samtools " + command + "'.");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Samtools command line: " + commandLine);
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
                            if (name.endsWith("txt") || name.endsWith("log") || name.endsWith("sam")) {
                                fileType = FileResult.FileType.PLAIN_TEXT;
                            } else if (name.endsWith("bam") || name.endsWith("cram") || name.endsWith("bai") || name.endsWith("crai")) {
                                fileType = FileResult.FileType.BINARY;
                            }
                            addFile(getOutDir().resolve(name), fileType);
                        }
                    }
                }

                // Check samtools errors
                boolean success = false;
                switch (command) {
                    case "index":
                    case "sort":
                    case "view": {
                        if (new File(outputFile).exists()) {
                            success = true;
                        }
                        break;
                    }
                    case "stats": {
                        File file = new File(getOutDir() + "/" + STDOUT_FILENAME);
                        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
                        if (lines.size() > 0 && lines.get(0).startsWith("# This file was produced by samtools stats")) {
                            FileUtils.copyFile(file, new File(outputFile));
                            addFile(new File(outputFile).toPath(), FileResult.FileType.PLAIN_TEXT);
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
        return SAMTOOLS_DOCKER_IMAGE;
    }

    @Override
    public String getCommandLine() throws ToolException {
        StringBuilder sb = new StringBuilder("docker run ");

        // Mount management
        Map<String, String> srcTargetMap = new HashMap<>();
        updateSrcTargetMap(inputFile, sb, srcTargetMap);

        sb.append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ");

        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // Samtools command
        sb.append(" samtools ").append(command);

        // Samtools options
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
            case "stats": {
                if (StringUtils.isNotEmpty(inputFile)) {
                    File file = new File(inputFile);
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                break;
            }
            case "index": {
                if (StringUtils.isNotEmpty(inputFile)) {
                    File file = new File(inputFile);
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }

                if (StringUtils.isNotEmpty(outputFile)) {
                    File file = new File(outputFile);
                    sb.append(" ").append(DOCKER_OUTPUT_PATH).append("/").append(file.getName());
                }
                break;
            }
            case "sort":
            case "view": {
                sb.append(" -o ").append(DOCKER_OUTPUT_PATH).append("/").append(new File(outputFile).getName());

                if (StringUtils.isNotEmpty(inputFile)) {
                    File file = new File(inputFile);
                    sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
                }
                break;
            }
        }

        return sb.toString();
    }

    private boolean checkParam(String param) {
        if (param.equals(DOCKER_IMAGE_VERSION_PARAM)) {
            return false;
        } else if ("index".equals(command) || "view".equals(command) || "sort".equals(command)) {
            if ("o".equals(param)) {
                return false;
            }
        }
        return true;
    }

    public String getCommand() {
        return command;
    }

    public SamtoolsWrapperAnalysis setCommand(String command) {
        this.command = command;
        return this;
    }

    public String getInputFile() {
        return inputFile;
    }

    public SamtoolsWrapperAnalysis setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public SamtoolsWrapperAnalysis setOutputFile(String outputFile) {
        this.outputFile = outputFile;
        return this;
    }
}
