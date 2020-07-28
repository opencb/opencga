package org.opencb.opencga.analysis.wrappers.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class OpenCgaWrapperAnalysisExecutor {

    public final String DOCKER_IMAGE_VERSION_PARAM = "DOCKER_IMAGE_VERSION";
    public final String DOCKER_INPUT_PATH = "/data/input";
    public final String DOCKER_OUTPUT_PATH = "/data/output";

    public final String STDOUT_FILENAME = "stdout.txt";
    public final String STDERR_FILENAME = "stderr.txt";

    protected String studyId;
    protected Path outDir;
    protected Path scratchDir;
    protected String token;

    protected String sep;
    protected String shortPrefix;
    protected String longPrefix;

    protected final ObjectMap params;

    protected Map<String, URI> fileUriMap;

    private CatalogManager catalogManager;

    public OpenCgaWrapperAnalysisExecutor(String studyId, ObjectMap params, Path outDir, Path scratchDir, CatalogManager catalogManager,
                                          String token) {
        this.studyId = studyId;
        this.params = params;
        this.outDir = outDir;
        this.scratchDir = scratchDir;
        this.catalogManager = catalogManager;
        this.token = token;

        fileUriMap = new HashMap<>();
    }

    public abstract void run() throws ToolException;

    protected abstract String getId();

    protected abstract String getDockerImageName();

    protected StringBuilder initCommandLine() {
        return new StringBuilder("docker run --log-driver=none -a stdin -a stdout -a stderr ");
    }

    protected void appendMounts(List<Pair<String, String>> inputFilenames, Map<String, String> srcTargetMap, StringBuilder sb) throws ToolException {
        // Mount input dirs
        for (Pair<String, String> pair : inputFilenames) {
            updateFileMaps(pair.getValue(), sb, fileUriMap, srcTargetMap);
        }

        // Mount output dir
        sb.append("--mount type=bind,source=\"").append(getOutDir().toAbsolutePath()).append("\",target=\"").append(DOCKER_OUTPUT_PATH)
                .append("\" ");
    }

    protected void appendCommand(String command, StringBuilder sb) {
        // Docker image and version
        sb.append(getDockerImageName());
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // Append command
        sb.append(" ").append(command);
//        sb.append(" java -jar /usr/picard/picard.jar ").append(command);
    }

    protected void appendInputFiles(List<Pair<String, String>> inputFilenames, Map<String, String> srcTargetMap, StringBuilder sb) {
        File file;
        for (Pair<String, String> pair : inputFilenames) {
            file = new File(fileUriMap.get(pair.getValue()).getPath());
            sb.append(" ");
            if (StringUtils.isNotEmpty(pair.getKey())) {
                if (pair.getKey().length() <= 1) {
                    sb.append(shortPrefix);
                } else {
                    sb.append(longPrefix);
                }
                sb.append(pair.getKey()).append(sep);
            }
            sb.append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/")
                    .append(file.getName());
        }
    }

    protected void appendOutputFiles(List<Pair<String, String>> outputFilenames, StringBuilder sb) {
        for (Pair<String, String> pair : outputFilenames) {
            sb.append(" ");
            if (StringUtils.isNotEmpty(pair.getKey())) {
                if (pair.getKey().length() <= 1) {
                    sb.append(shortPrefix);
                } else {
                    sb.append(longPrefix);
                }
                sb.append(pair.getKey()).append(sep);
            }
            sb.append(DOCKER_OUTPUT_PATH);
            if (StringUtils.isNotEmpty(pair.getValue())) {
                sb.append("/").append(pair.getValue());
            }
        }
    }

    protected void appendOtherParams(Set<String> skipParams, StringBuilder sb) {
        for (String paramName : params.keySet()) {
            if (skipParams.contains(paramName)) {
                continue;
            }
            sb.append(" ");
            if (StringUtils.isNotEmpty(paramName)) {
                if (paramName.length() <= 1) {
                    sb.append(shortPrefix);
                } else {
                    sb.append(longPrefix);
                }
                sb.append(paramName).append(sep);
            }
            String value = params.getString(paramName);
            if (StringUtils.isNotEmpty(value) && !"true".equals(value)) {
                sb.append(params.getString(paramName));
            }
        }
    }

    protected void runCommandLine(String cmdline) throws ToolException {
        try {
            System.out.println("Docker command line:\n" + cmdline);
            new Command(cmdline)
                    .setOutputOutputStream(
                            new DataOutputStream(new FileOutputStream(getScratchDir().resolve(getId() + "." + STDOUT_FILENAME).toFile())))
                    .setErrorOutputStream(
                            new DataOutputStream(new FileOutputStream(getScratchDir().resolve(getId() + "." + STDERR_FILENAME).toFile())))
                    .run();
        } catch (FileNotFoundException e) {
            throw new ToolException(e);
        }
    }

    protected void updateFileMaps(String filename, StringBuilder sb, Map<String, URI> fileUriMap, Map<String, String> srcTargetMap)
            throws ToolException {
        if (StringUtils.isEmpty(filename)) {
            // Skip
            return;
        }

        OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult;
        try {
            fileResult = catalogManager.getFileManager().get(studyId, filename, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing file '" + filename + "' of the study " + studyId + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException("File '" + filename + "' not found in study '" + studyId + "'");
        }
        URI uri = fileResult.getResults().get(0).getUri();

        if (StringUtils.isNotEmpty(uri.toString())) {
            fileUriMap.put(filename, uri);
            if (srcTargetMap != null) {
                String src = new File(uri.getPath()).getParentFile().getAbsolutePath();
                if (!srcTargetMap.containsKey(src)) {
                    srcTargetMap.put(src, DOCKER_INPUT_PATH + srcTargetMap.size());
                    sb.append("--mount type=bind,source=\"").append(src).append("\",target=\"").append(srcTargetMap.get(src)).append("\" ");
                }
            }
        }
    }

    public String getStudyId() {
        return studyId;
    }

    public OpenCgaWrapperAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public Path getOutDir() {
        return outDir;
    }

    public Path getScratchDir() {
        return scratchDir;
    }

    public OpenCgaWrapperAnalysisExecutor setScratchDir(Path scratchDir) {
        this.scratchDir = scratchDir;
        return this;
    }

    public OpenCgaWrapperAnalysisExecutor setOutDir(Path outDir) {
        this.outDir = outDir;
        return this;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public OpenCgaWrapperAnalysisExecutor setCatalogManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        return this;
    }

    public String getSep() {
        return sep;
    }

    public OpenCgaWrapperAnalysisExecutor setSep(String sep) {
        this.sep = sep;
        return this;
    }

    public String getShortPrefix() {
        return shortPrefix;
    }

    public OpenCgaWrapperAnalysisExecutor setShortPrefix(String shortPrefix) {
        this.shortPrefix = shortPrefix;
        return this;
    }

    public String getLongPrefix() {
        return longPrefix;
    }

    public OpenCgaWrapperAnalysisExecutor setLongPrefix(String longPrefix) {
        this.longPrefix = longPrefix;
        return this;
    }
}
