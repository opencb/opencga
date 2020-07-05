/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.wrappers;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public abstract class OpenCgaWrapperAnalysis extends OpenCgaTool {

    public final String DOCKER_IMAGE_VERSION_PARAM = "DOCKER_IMAGE_VERSION";
    public final static String DOCKER_INPUT_PATH = "/data/input";
    public final static String DOCKER_OUTPUT_PATH = "/data/output";

    public static final String STDOUT_FILENAME = "stdout.txt";
    public static final String STDERR_FILENAME = "stderr.txt";

    private String study;

    protected Map<String, URI> fileUriMap = new HashMap<>();

    @Deprecated
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

    protected String getCatalogPath(String inputFile) throws ToolException {
        return new File(getCatalogFile(inputFile).getPath()).getParent();
    }

    protected org.opencb.opencga.core.models.file.File getCatalogFile(String inputFile) throws ToolException {
        // Get catalog path
        OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult;
        try {
            fileResult = catalogManager.getFileManager().get(getStudy(), inputFile, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error accessing file '" + inputFile + "' of the study " + getStudy() + "'", e);
        }
        if (fileResult.getNumResults() <= 0) {
            throw new ToolException("File '" + inputFile + "' not found in study '" + getStudy() + "'");
        }

        return fileResult.getResults().get(0);
    }

    protected boolean isValidFile(File file) {
        return file.exists() && (FileUtils.sizeOf(file) > 0);
    }

    protected void updateSrcTargetMap(String filename, StringBuilder sb, Map<String, String> srcTargetMap) throws ToolException {
        if (StringUtils.isEmpty(filename)) {
            // Skip
            return;
        }

        OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult;
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

        OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult;
        try {
            fileResult = catalogManager.getFileManager().get(getStudy(), filename, QueryOptions.empty(), token);
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
            if (srcTargetMap != null) {
                String src = new File(uri.getPath()).getParentFile().getAbsolutePath();
                if (!srcTargetMap.containsKey(src)) {
                    srcTargetMap.put(src, DOCKER_INPUT_PATH + srcTargetMap.size());
                    sb.append("--mount type=bind,source=\"").append(src).append("\",target=\"").append(srcTargetMap.get(src)).append("\" ");
                }
            }
        }
    }

    public String getStudy() {
        return study;
    }

    public OpenCgaWrapperAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }
}