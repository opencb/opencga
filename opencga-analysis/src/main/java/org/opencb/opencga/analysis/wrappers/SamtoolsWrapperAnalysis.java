package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.update.FileUpdateParams;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.models.AnnotationSet;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine.ALIGNMENT_STATS_VARIABLE_SET;

@Tool(id = SamtoolsWrapperAnalysis.ID, type = Tool.ToolType.ALIGNMENT, description = SamtoolsWrapperAnalysis.DESCRIPTION)
public class SamtoolsWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "samtools";
    public final static String DESCRIPTION = "Samtools is a program for interacting with high-throughput sequencing data in SAM, BAM"
            + " and CRAM formats.";

    public final static String SAMTOOLS_DOCKER_IMAGE = "zlskidmore/samtools";

    public static final String INDEX_STATS_PARAM = "stats-index";

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

//        if (StringUtils.isEmpty(outputFile)) {
//            throw new AnalysisException("Missing input file when executing 'samtools " + command + "'.");
//        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Samtools command line: " + commandLine);
            try {
                Set<String> filenamesBeforeRunning = new HashSet<>(getFilenames(getOutDir()));
                filenamesBeforeRunning.add(STDOUT_FILENAME);
                filenamesBeforeRunning.add(STDERR_FILENAME);

                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(getCommandLine())
                        .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

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
                        File file = getOutDir().resolve(STDOUT_FILENAME).toFile();
                        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
                        if (lines.size() > 0 && lines.get(0).startsWith("# This file was produced by samtools stats")) {
                            outputFile = getOutDir() + "/" + new File(fileUriMap.get(inputFile)).getName() + ".stats.txt";

                            FileUtils.copyFile(file, new File(outputFile));
                            if (params.containsKey(INDEX_STATS_PARAM) && params.getBoolean(INDEX_STATS_PARAM)) {
                                indexStats();
                            }
                            success = true;
                        }
                        break;
                    }
                }
                if (!success) {
                    File file = getOutDir().resolve(STDERR_FILENAME).toFile();
                    String msg = "Something wrong happened when executing Samtools";
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
        updateFileMaps(inputFile, sb, fileUriMap, srcTargetMap);

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
                    File file = new File(fileUriMap.get(inputFile).getPath());
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
        if (param.equals(DOCKER_IMAGE_VERSION_PARAM) || param.equals(INDEX_STATS_PARAM)) {
            return false;
        } else if ("index".equals(command) || "view".equals(command) || "sort".equals(command)) {
            if ("o".equals(param)) {
                return false;
            }
        }
        return true;
    }

    private void indexStats() throws CatalogException, IOException {
        // TODO: remove when daemon copies the stats file
        Files.createSymbolicLink(new File(fileUriMap.get(inputFile).getPath()).getParentFile().toPath().resolve(new File(outputFile).getName()),
                Paths.get(new File(outputFile).getAbsolutePath()));

        // Create a variable set with the summary numbers of the statistics
        Map<String, Object> annotations = new HashMap<>();
        List<String> lines = org.apache.commons.io.FileUtils.readLines(new File(outputFile), Charset.defaultCharset());
        int count = 0;

        for (String line : lines) {
            // Only take into account the "SN" section (summary numbers)
            if (line.startsWith("SN")) {
                count++;
                String[] splits = line.split("\t");
                String key = splits[1].split("\\(")[0].trim().replace(" ", "_").replace(":", "");
                // Special case
                if (line.contains("bases mapped (cigar):")) {
                    key += "_cigar";
                }
                String value = splits[2].split(" ")[0];
                annotations.put(key, value);
            } else if (count > 0) {
                // SN (summary numbers) section has been processed
                break;
            }
        }

        AnnotationSet annotationSet = new AnnotationSet(ALIGNMENT_STATS_VARIABLE_SET, ALIGNMENT_STATS_VARIABLE_SET, annotations);

        FileUpdateParams updateParams = new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet));

        catalogManager.getFileManager().update(getStudy(), inputFile, updateParams, QueryOptions.empty(), token);
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
