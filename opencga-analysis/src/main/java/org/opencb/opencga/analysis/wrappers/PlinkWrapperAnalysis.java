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
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

@Tool(id = PlinkWrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = PlinkWrapperAnalysis.DESCRIPTION)
public class PlinkWrapperAnalysis extends OpenCgaWrapperAnalysis {

    public static final String ID = "plink";
    public static final String DESCRIPTION = "Plink is a whole genome association analysis toolset, designed to perform"
            + " a range of basic, large-scale analyses.";

    public static final String PLINK_DOCKER_IMAGE = "jrose77/plinkdocker"; //"gelog/plink";
    public static final String OUT_NAME = "plink";

    public static final String TPED_FILE_PARAM = "tpedFile";
    public static final String TFAM_FILE_PARAM = "tfamFile";
    public static final String COVAR_FILE_PARAM = "covarFile";

    protected void check() throws Exception {
        super.check();
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            String commandLine = getCommandLine();
            logger.info("Plink command line:" + commandLine);
            try {
                // Execute command and redirect stdout and stderr to the files: stdout.txt and stderr.txt
                Command cmd = new Command(getCommandLine())
                        .setOutputOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDOUT_FILENAME).toFile())))
                        .setErrorOutputStream(
                                new DataOutputStream(new FileOutputStream(getScratchDir().resolve(STDERR_FILENAME).toFile())));

                cmd.run();

                // Check Plink errors by reading the stderr file
                File stderrFile = new File(getScratchDir() + "/" + STDERR_FILENAME);
                if (FileUtils.sizeOf(stderrFile) > 0) {
                    throw new ToolException(StringUtils.join(FileUtils.readLines(stderrFile, Charset.defaultCharset()), ". "));
                }
            } catch (Exception e) {
                throw new ToolException(e);
            }
        });
    }

//    @Override
//    public String getDockerImageName() {
//        return PLINK_DOCKER_IMAGE;
//    }

//    @Override
    public String getCommandLine() {
        StringBuilder sb = new StringBuilder("docker run ");

        // Mount management
        Map<String, String> srcTargetMap = new HashMap<>();
        String[] names = {TPED_FILE_PARAM, TFAM_FILE_PARAM, COVAR_FILE_PARAM};
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
        sb.append(PLINK_DOCKER_IMAGE);
        if (params.containsKey(DOCKER_IMAGE_VERSION_PARAM)) {
            sb.append(":").append(params.getString(DOCKER_IMAGE_VERSION_PARAM));
        }

        // Plink parameters
        for (String key : params.keySet()) {
            if (checkParam(key)) {
                String value = params.getString(key);
                if (key.equals("out")) {
                    String[] split = value.split("/");
                    value = split[split.length - 1];
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
        // Input file management
        String filename = params.getString(TPED_FILE_PARAM, null);
        if (StringUtils.isNotEmpty(filename)) {
            String prefix = new File(filename).getName().split("\\.")[0];
            sb.append(" --tfile ").append(srcTargetMap.get(new File(filename).getParentFile().getAbsolutePath())).append("/")
                    .append(prefix);
        }
        filename = params.getString(COVAR_FILE_PARAM, null);
        if (StringUtils.isNotEmpty(filename)) {
            File file = new File(filename);
            sb.append(" --covar ").append(srcTargetMap.get(file.getParentFile().getAbsolutePath())).append("/").append(file.getName());
        }

        return sb.toString();
    }

    private boolean checkParam(String key) {
        if (key.equals(DOCKER_IMAGE_VERSION_PARAM)
                || key.equals("noweb") || key.equals("file") || key.equals("bfile")
                || key.equals(TFAM_FILE_PARAM) || key.equals(TPED_FILE_PARAM) || key.equals(COVAR_FILE_PARAM)) {
            return false;
        }
        return true;
    }
}
