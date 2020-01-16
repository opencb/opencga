package org.opencb.opencga.analysis.wrappers;


import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Tool(id = FastqcWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = FastqcWrapperAnalysis.DESCRIPTION)
public class FastqcWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "fastqc";
    public final static String DESCRIPTION = "A quality control tool for high throughput sequence data.";

    public final static String SAMTOOLS_DOCKER_IMAGE = "dceoy/fastqc";

    private String file;

    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(file)) {
            throw new ToolException("Missing input file when executing 'fastqc'.");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("FastQC command line: " + commandLine);
            try {
                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(getCommandLine())
                        .setOutputOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Check fastqc errors
                boolean success = false;
                List<String> filenames = Files.walk(getOutDir()).map(f -> f.getFileName().toString()).collect(Collectors.toList());
                for (String filename : filenames) {
                    if (filename.endsWith("html")) {
                        success = true;
                        break;
                    }
                }
                if (!success) {
                    File file = getScratchDir().resolve(STDERR_FILENAME).toFile();
                    String msg = "Something wrong happened when executing FastQC";
                    if (file.exists()) {
                        msg = StringUtils.join(FileUtils.readLines(file, Charset.defaultCharset()), "\n");
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
        updateFileMaps(file, sb, fileUriMap, srcTargetMap);

        sb.append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ");

        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // FastQC options
        for (String param : params.keySet()) {
            if (checkParam(param)) {
                String value = params.getString(param);
                sb.append(param.length() == 1 ? " -" : " --").append(param);
                if (StringUtils.isNotEmpty(value) && !"null".equals(value)) {
                    sb.append(" ").append(value);
                }
            }
        }

        sb.append(" -o ").append(DOCKER_OUTPUT_PATH);

        File file = new File(fileUriMap.get(this.file).getPath());
        sb.append(" ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());

        return sb.toString();
    }

    private boolean checkParam(String param) {
        if ("o".equals(param) || param.equals(DOCKER_IMAGE_VERSION_PARAM)) {
            return false;
        }
        return true;
    }

    public String getFile() {
        return file;
    }

    public FastqcWrapperAnalysis setFile(String file) {
        this.file = file;
        return this;
    }
}
