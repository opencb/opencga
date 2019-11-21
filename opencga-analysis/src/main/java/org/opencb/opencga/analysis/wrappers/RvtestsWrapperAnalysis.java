package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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

@Analysis(id = RvtestsWrapperAnalysis.ID, type = Analysis.AnalysisType.VARIANT)
public class RvtestsWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "rvtests";
    public final static String RVTESTS_DOCKER_IMAGE = "zhanxw/rvtests-docker";
    public final static String OUT_NAME = "out";

    protected void check() throws Exception {
        super.check();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Rvtests command line:" + commandLine);
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
                            }
                            addFile(getOutDir().resolve(name), fileType);
                        }
                    }
                }
                // Check Rvtests errors by reading the stdout and stderr files
                boolean success = false;
                File file = new File(getOutDir() + "/" + STDOUT_FILENAME);
                List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
                if (lines.get(lines.size() - 1).contains("successfully")) {
                    success = true;
                }
                if (!success) {
                    file = new File(getOutDir() + "/" + STDERR_FILENAME);
                    String msg = "Something wrong executing Rvtests";
                    if (file.exists()) {
                        msg = StringUtils.join(FileUtils.readLines(file, Charset.defaultCharset()), ". ");
                    }
                    throw new AnalysisException(msg);
                }
            } catch (Exception e) {
                throw new AnalysisException(e);
            }
        });
    }

    @Override
    public String getDockerImageName() {
        return RVTESTS_DOCKER_IMAGE;
    }

    @Override
    public String getCommandLine() {
        StringBuilder sb = new StringBuilder("docker run ").append("--mount type=bind,source=\"").append(getOutDir().toAbsolutePath())
                .append("\",target=\"").append(DOCKER_INPUT_PATH).append("\" ").append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ")
                .append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // TODO: support for rvtests commands, e.g.: rvtest, vcf2kinship
        sb.append(" rvtest ");
//        sb.append(" vcf2kinship ");

        for (String key : params.keySet()) {
            if (!key.equals(DOCKER_IMAGE_VERSION_PARAM)) {
                String value = params.getString(key);
                if (key.equals("inVcf") || key.equals("pheno")) {
                    sb.append(" --").append(key).append(" ").append(DOCKER_INPUT_PATH).append("/").append(value);
                } else if (key.equals("out")) {
                    sb.append(" --out ").append(" ").append(DOCKER_OUTPUT_PATH).append("/").append(value);
                } else {
                    sb.append(" --").append(key);
                    if (StringUtils.isNotEmpty(value)) {
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
