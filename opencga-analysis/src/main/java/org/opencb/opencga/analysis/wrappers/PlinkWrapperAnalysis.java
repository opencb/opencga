package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.analysis.result.FileResult;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.exception.AnalysisException;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Analysis(id = PlinkWrapperAnalysis.ID, type = Analysis.AnalysisType.VARIANT)
public class PlinkWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "plink";
    public final static String PLINK_DOCKER_IMAGE = "gelog/plink";
    public final static String OUT_NAME = "plink";

    protected void check() throws Exception {
        super.check();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Plink command line:" + commandLine);
            try {
                Set<String> beforeNames = new HashSet<>(getFilenames(getOutDir()));
                beforeNames.add("status.json");

                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(getCommandLine())
                        .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Add the output files to the analysis result file
                List<String> outNames = getFilenames(getOutDir());
                for (String name : outNames) {
                    if (!beforeNames.contains(name)) {
                        if (FileUtils.sizeOf(new File(getOutDir() + "/" + name)) > 0) {
                            FileResult.FileType fileType = FileResult.FileType.TAB_SEPARATED;
                            if (name.endsWith("txt") || name.endsWith("log")) {
                                fileType = FileResult.FileType.PLAIN_TEXT;
                            }
                            addFile(getOutDir().resolve(name), fileType);
                        }
                    }
                }
                // Check Plink errors by reading the stderr file
                File stderrFile = new File(getOutDir() + "/" + STDERR_FILENAME);
                if (FileUtils.sizeOf(stderrFile) > 0) {
                    throw new AnalysisException(StringUtils.join(FileUtils.readLines(stderrFile, Charset.defaultCharset()), ". "));
                }
            } catch (Exception e) {
                throw new AnalysisException(e);
            }
        });
    }

    @Override
    public String getDockerImageName() {
        return PLINK_DOCKER_IMAGE;
    }

    @Override
    public String getCommandLine() {
        StringBuilder sb = new StringBuilder("docker run ").append("--mount type=bind,source=\"").append(getOutDir().toAbsolutePath())
                .append("\",target=\"").append(DOCKER_INPUT_PATH).append("\" ").append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ")
                .append(PLINK_DOCKER_IMAGE);
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        for (String key : params.keySet()) {
            if (!key.equals(DOCKER_IMAGE_VERSION_PARAM) && !key.equals("noweb")) {
                String value = params.getString(key);
                if (key.equals("file") || key.equals("bfile")) {
                    sb.append(" --").append(key).append(" ").append(DOCKER_INPUT_PATH).append("/").append(value);
                } else if (key.equals("out")) {
                    sb.append(" --out ").append(" ").append(DOCKER_OUTPUT_PATH).append("/").append(value);
                } else {
                    sb.append(" --").append(key);
                    if (!StringUtils.isEmpty(value)) {
                        sb.append(" ").append(value);
                    }
                }
            }
        }
        if (!params.containsKey("out")) {
            sb.append(" --out ").append(DOCKER_OUTPUT_PATH).append("/").append(OUT_NAME);
        }

        return sb.toString();
    }
}
