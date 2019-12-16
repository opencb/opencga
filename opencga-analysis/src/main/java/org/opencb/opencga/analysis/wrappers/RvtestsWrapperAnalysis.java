package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.*;

@Tool(id = RvtestsWrapperAnalysis.ID, type = Tool.ToolType.VARIANT, description = RvtestsWrapperAnalysis.DESCRIPTION)
public class RvtestsWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public final static String ID = "rvtests";
    public static final String DESCRIPTION = "Rvtests is a flexible software package for genetic association studies";

    public final static String RVTESTS_DOCKER_IMAGE = "zhanxw/rvtests-docker";
    public final static String OUT_NAME = "out";

    public static final String EXECUTABLE_PARAM = "executable";
    public static final String VCF_FILE_PARAM = "vcfFile";
    public static final String PHENOTYPE_FILE_PARAM = "phenoFile";
    public static final String PEDIGREE_FILE_PARAM = "pedigreeFile";
    public static final String KINSHIP_FILE_PARAM = "kinshipFile";
    public static final String COVAR_FILE_PARAM = "covarFile";

    protected void check() throws Exception {
        super.check();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Rvtests command line:" + commandLine);
            try {
                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(getCommandLine())
                        .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Check Rvtests errors by reading the stdout and stderr files
                boolean success = false;
                File file;
                if ("rvtest".equals(params.getString(EXECUTABLE_PARAM))) {
                    file = new File(getOutDir() + "/" + STDOUT_FILENAME);
                    List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
                    if (lines.get(lines.size() - 1).contains("successfully")) {
                        success = true;
                    }
                } else if ("vcf2kinship".equals(params.getString(EXECUTABLE_PARAM))) {
                    file = new File(getOutDir() + "/" + STDERR_FILENAME);
                    List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
                    if (lines.get(lines.size() - 1).contains("Analysis took")) {
                        success = true;
                    }
                }
                if (!success) {
                    file = new File(getOutDir() + "/" + STDERR_FILENAME);
                    String msg = "Something wrong executing Rvtests";
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
        return RVTESTS_DOCKER_IMAGE;
    }

    @Override
    public String getCommandLine() {
        StringBuilder sb = new StringBuilder("docker run ");

        // Mount management
        Map<String, String> srcTargetMap = new HashMap<>();
        String[] names = {VCF_FILE_PARAM, PHENOTYPE_FILE_PARAM, PEDIGREE_FILE_PARAM, KINSHIP_FILE_PARAM, COVAR_FILE_PARAM};
        for (String name : names) {
            if (params.containsKey(name) && StringUtils.isNotEmpty(params.getString(name))) {
                String src = new File(params.getString(name)).getParentFile().getAbsolutePath();
                if (!srcTargetMap.containsKey(src)) {
                    srcTargetMap.put(src, DOCKER_INPUT_PATH + srcTargetMap.size());
                    sb.append("--mount type=bind,source=\"").append(src).append("\",target=\"").append(srcTargetMap.get(src)).append("\" ");
                }
            }
        }
        sb.append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ");

        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // Executable values: rvtest or vcf2kinship
        sb.append(" ").append(params.getString(EXECUTABLE_PARAM)).append(" ");

        for (String key : params.keySet()) {
            if (checkParam(key)) {
                String value = params.getString(key);
                if (key.equals("out")) {
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

        // Input files management
        String filename = params.getString(VCF_FILE_PARAM, null);
        if (StringUtils.isNotEmpty(filename)) {
            File file = new File(filename);
            sb.append(" --inVcf ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
        }
        filename = params.getString(PHENOTYPE_FILE_PARAM, null);
        if (StringUtils.isNotEmpty(filename)) {
            File file = new File(filename);
            sb.append(" --pheno ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
        }
        filename = params.getString(PEDIGREE_FILE_PARAM, null);
        if (StringUtils.isNotEmpty(filename)) {
            File file = new File(filename);
            sb.append(" --pedigree ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
        }
        filename = params.getString(KINSHIP_FILE_PARAM, null);
        if (StringUtils.isNotEmpty(filename)) {
            File file = new File(filename);
            sb.append(" --kinship ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
        }
        filename = params.getString(COVAR_FILE_PARAM, null);
        if (StringUtils.isNotEmpty(filename)) {
            File file = new File(filename);
            sb.append(" --covar ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
        }

        return sb.toString();
    }

    private String getMountParameters() {
        Set<String> sources = new HashSet<>();
        String[] names = {VCF_FILE_PARAM, PHENOTYPE_FILE_PARAM, PEDIGREE_FILE_PARAM, KINSHIP_FILE_PARAM, COVAR_FILE_PARAM};
        for (String name : names) {
            if (params.containsKey(name) && StringUtils.isNotEmpty(params.getString(name))) {
                sources.add(new File(params.getString(name)).getParentFile().getAbsolutePath());
            }
        }

        StringBuilder sb = new StringBuilder();
        sources.forEach(s
                -> sb.append("--mount type=bind,source=\"").append(s).append("\",target=\"").append(DOCKER_INPUT_PATH).append("\" "));
        sb.append("--mount type=bind,source=\"")
                .append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH).append("\" ");

        return sb.toString();
    }

    private boolean checkParam(String key) {
        if (key.equals(DOCKER_IMAGE_VERSION_PARAM) || key.equals(EXECUTABLE_PARAM) || key.equals(VCF_FILE_PARAM)
                || key.equals(PHENOTYPE_FILE_PARAM) || key.equals(PEDIGREE_FILE_PARAM) || key.equals(KINSHIP_FILE_PARAM)
                || key.equals(COVAR_FILE_PARAM)) {
            return false;
        }
        return true;
    }
}
