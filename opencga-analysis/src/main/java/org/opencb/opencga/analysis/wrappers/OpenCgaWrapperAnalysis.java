package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.results.OpenCGAResult;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class OpenCgaWrapperAnalysis extends OpenCgaTool {

    public final String DOCKER_IMAGE_VERSION_PARAM = "DOCKER_IMAGE_VERSION";
    public final static String DOCKER_INPUT_PATH = "/data/input";
    public final static String DOCKER_OUTPUT_PATH = "/data/output";

    public final String STDOUT_FILENAME = "stdout.txt";
    public final String STDERR_FILENAME = "stderr.txt";

    private String study;

    public abstract String getDockerImageName();

    protected String getCommandLine() throws ToolException {
        return getCommandLine("--");
    }

    protected String getCommandLine(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append("run docker ").append(getDockerImageName());
        for (String key : params.keySet()) {
            String value = params.getString(key);
            sb.append(" ").append(prefix).append(key);
            if (StringUtils.isNotEmpty(value)) {
                sb.append(" ").append(value);
            }
        }
        return sb.toString();
    }

    protected void updateSrcTargetMap(String filename, StringBuilder sb, Map<String, String> srcTargetMap) throws ToolException {
        if (StringUtils.isEmpty(filename)) {
            // Skip
            return;
        }

        OpenCGAResult<org.opencb.opencga.core.models.File> fileResult;
        try {
            fileResult = catalogManager.getFileManager().get(getStudy(), filename,
                    QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing file '" + filename + "' of the study " + getStudy() + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException("File '" + filename + "' not found in study '" + getStudy() + "'");
        }
        URI uri = fileResult.getResults().get(0).getUri();
        logger.info("filename = " + filename + " ---> uri = " + uri + " ---> path = " + uri.getPath());

        if (StringUtils.isNotEmpty(uri.toString())) {
            String src = new File(uri.getPath()).getParentFile().getAbsolutePath();
            if (!srcTargetMap.containsKey(src)) {
                srcTargetMap.put(src, DOCKER_INPUT_PATH + srcTargetMap.size());
                sb.append("--mount type=bind,source=\"").append(src).append("\",target=\"").append(srcTargetMap.get(src)).append("\" ");
            }
        }
    }

    protected void updateFileMaps(String filename, StringBuilder sb, Map<String, URI> fileUriMap, Map<String, String> srcTargetMap)
            throws ToolException {
        if (StringUtils.isEmpty(filename)) {
            // Skip
            return;
        }

        OpenCGAResult<org.opencb.opencga.core.models.File> fileResult;
        try {
            fileResult = catalogManager.getFileManager().get(getStudy(), filename,
                    QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing file '" + filename + "' of the study " + getStudy() + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException("File '" + filename + "' not found in study '" + getStudy() + "'");
        }
        URI uri = fileResult.getResults().get(0).getUri();
        logger.info("filename = " + filename + " ---> uri = " + uri + " ---> path = " + uri.getPath());

        if (StringUtils.isNotEmpty(uri.toString())) {
            fileUriMap.put(filename, uri);
            String src = new File(uri.getPath()).getParentFile().getAbsolutePath();
            if (!srcTargetMap.containsKey(src)) {
                srcTargetMap.put(src, DOCKER_INPUT_PATH + srcTargetMap.size());
                sb.append("--mount type=bind,source=\"").append(src).append("\",target=\"").append(srcTargetMap.get(src)).append("\" ");
            }
        }
    }

    @Deprecated
    protected List<String> getFilenames(Path dir) throws IOException {
        Stream<Path> walk = Files.walk(dir);
        return walk.map(x -> x.getFileName().toString()).collect(Collectors.toList());
    }

    public String getStudy() {
        return study;
    }

    public OpenCgaWrapperAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }
}