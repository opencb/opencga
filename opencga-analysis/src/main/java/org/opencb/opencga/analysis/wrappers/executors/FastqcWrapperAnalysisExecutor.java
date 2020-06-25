package org.opencb.opencga.analysis.wrappers.executors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.sequence.fastqc.FastQc;
import org.opencb.biodata.formats.sequence.fastqc.io.FastQcParser;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.wrappers.FastqcWrapperAnalysis;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class FastqcWrapperAnalysisExecutor extends OpenCgaWrapperAnalysisExecutor {

    private String file;

    public FastqcWrapperAnalysisExecutor(String studyId, ObjectMap params, Path outDir, Path scratchDir, CatalogManager catalogManager,
                                         String token) {
        super(studyId, params, outDir, scratchDir, catalogManager, token);
    }

    @Override
    public void run() throws ToolException {
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

        // Input file
        File f = new File(fileUriMap.get(file).getPath());
        sb.append(" ").append(srcTargetMap.get(f.getParentFile().getAbsolutePath())).append("/").append(f.getName());

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

        try {
            // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
            runCommandLine(sb.toString());
        } catch (FileNotFoundException e) {
            throw new ToolException(e);
        }
    }

    public FastQc getResult() throws ToolException {
        File[] files = outDir.toFile().listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().endsWith("_fastqc") && file.isDirectory()) {
                    try {
                        Path dataPath = file.toPath().resolve("fastqc_data.txt");
                        return FastQcParser.parse(dataPath.toFile());
                    } catch (IOException e) {
                        throw new ToolException(e);
                    }
                }
            }
        }
        String msg = "Something wrong when reading FastQC result.\n";
        try {
            msg += StringUtils.join(FileUtils.readLines(scratchDir.resolve(getId() + ".stderr.txt").toFile()), "\n");
        } catch (IOException e) {
            throw new ToolException(e);
        }

        throw new ToolException(msg);
    }

    @Override
    protected String getId() {
        return FastqcWrapperAnalysis.ID;
    }

    @Override
    protected String getDockerImageName() {
        return FastqcWrapperAnalysis.FASTQC_DOCKER_IMAGE;
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

    public FastqcWrapperAnalysisExecutor setFile(String file) {
        this.file = file;
        return this;
    }
}
