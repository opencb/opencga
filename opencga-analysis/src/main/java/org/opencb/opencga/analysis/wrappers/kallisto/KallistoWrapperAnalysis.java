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

package org.opencb.opencga.analysis.wrappers.kallisto;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.wrapper.kallisto.KallistoParams;
import org.opencb.opencga.core.models.wrapper.kallisto.KallistoWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.analysis.wrappers.BaseDockerWrapperAnalysisExecutor.OUTPUT_FILE_PREFIX;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.checkParams;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.checkPaths;
import static org.opencb.opencga.core.models.wrapper.kallisto.KallistoParams.*;

@Tool(id = KallistoWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = KallistoWrapperAnalysis.DESCRIPTION)
public class KallistoWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "kallisto";
    public final static String DESCRIPTION = "Kallisto is a software tool used for quantifying the abundance of RNA transcripts from"
            + " sequencing data, particularly RNA-Seq data.";

    @ToolParams
    protected final KallistoWrapperParams analysisParams = new KallistoWrapperParams();

    private KallistoParams updatedParams = new KallistoParams();

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check command
        if (StringUtils.isEmpty(analysisParams.getKallistoParams().getCommand())) {
            throw new ToolException("Missing mandatory parameter 'command'. Please, specify the Kallisto command to run."
                    + " Valid commands are: " + StringUtils.join(KallistoParams.VALID_COMMANDS, ", ") + ".");
        }
        if (!KallistoParams.VALID_COMMANDS.contains(analysisParams.getKallistoParams().getCommand())) {
            throw new ToolException("Invalid Kallisto command '" + analysisParams.getKallistoParams().getCommand() + "'."
                    + " Valid commands are: " + StringUtils.join(KallistoParams.VALID_COMMANDS, ", ") + ".");
        }

        // Command
        updatedParams.setCommand(analysisParams.getKallistoParams().getCommand());

        switch (updatedParams.getCommand()) {
            case INDEX_CMD: {
                checkKallistoIndexParams();
                break;
            }

            case QUANT_CMD: {
                checkKallistoQuantParams();
                break;
            }

            case QUANT_TCC_CMD: {
                checkKallistoQuantTccParams();
                break;
            }

            case CITE_CMD:
            case VERSION_CMD: {
                // No parameters to check for version command
                break;
            }

            default:
                throw new ToolException("Unsupported Kallisto command '" + updatedParams.getCommand() + "'. Supported commands are: "
                        + INDEX_CMD + ".");
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID);
    }

    protected void run() throws ToolException, IOException {
        // Run Kallisto
        step(ID, this::runKallisto);
    }

    protected void runKallisto() throws ToolException {
        // Get executor
        KallistoWrapperAnalysisExecutor executor = getToolExecutor(KallistoWrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setKallistoParams(updatedParams)
                .execute();
    }

    private void checkKallistoIndexParams() throws ToolException {
        // Check inputs
        if (CollectionUtils.isEmpty(analysisParams.getKallistoParams().getInput())) {
            throw new ToolException("Missing input parameters: FASTA-files");
        }
        updatedParams.setInput(checkPaths(analysisParams.getKallistoParams().getInput(), study, catalogManager, token));

        // Check options
        if (MapUtils.isEmpty(analysisParams.getKallistoParams().getOptions())
                || (!analysisParams.getKallistoParams().getOptions().containsKey(INDEX_PARAM)
                && !analysisParams.getKallistoParams().getOptions().containsKey(I_PARAM))
                || (StringUtils.isEmpty(analysisParams.getKallistoParams().getOptions().getString(INDEX_PARAM))
                && StringUtils.isEmpty(analysisParams.getKallistoParams().getOptions().getString(I_PARAM)))) {
            throw new ToolException("Missing mandatory parameter 'index'. Please, specify the output index file name."
                    + " It can be set using the '" + INDEX_PARAM + "' or '" + I_PARAM + "' parameters.");
        }

        List<String> fileParams = getFileParams(INDEX_CMD, analysisParams.getKallistoParams().getOptions());
        List<String> skippedParams = getSkippedParams(INDEX_CMD, analysisParams.getKallistoParams().getOptions());
        updatedParams.setOptions(checkParams(analysisParams.getKallistoParams().getOptions(), fileParams, skippedParams, study,
                catalogManager, token));

        // Temporary output directory
        logger.warn("The tmp directory parameter ('{}') is set to the JOB scratch dir.", TMP_PARAM);
        updatedParams.getOptions().put(TMP_PARAM, OUTPUT_FILE_PREFIX + getScratchDir().toAbsolutePath());
    }

    private void checkKallistoQuantParams() throws ToolException {
        // Check inputs
        if (CollectionUtils.isEmpty(analysisParams.getKallistoParams().getInput())) {
            throw new ToolException("Missing input parameters: FASTQ-files");
        }
        updatedParams.setInput(checkPaths(analysisParams.getKallistoParams().getInput(), study, catalogManager, token));

        // Check options
        if (MapUtils.isEmpty(analysisParams.getKallistoParams().getOptions())
                || (!analysisParams.getKallistoParams().getOptions().containsKey(INDEX_PARAM)
                && !analysisParams.getKallistoParams().getOptions().containsKey(I_PARAM))
                || (StringUtils.isEmpty(analysisParams.getKallistoParams().getOptions().getString(INDEX_PARAM))
                && StringUtils.isEmpty(analysisParams.getKallistoParams().getOptions().getString(I_PARAM)))) {
            throw new ToolException("Missing mandatory parameter 'index'. Please, specify the output index file name."
                    + " It can be set using the '" + INDEX_PARAM + "' or '" + I_PARAM + "' parameters.");
        }

        List<String> fileParams = getFileParams(QUANT_CMD, analysisParams.getKallistoParams().getOptions());
        List<String> skippedParams = getSkippedParams(QUANT_CMD, analysisParams.getKallistoParams().getOptions());
        updatedParams.setOptions(checkParams(analysisParams.getKallistoParams().getOptions(), fileParams, skippedParams, study,
                catalogManager, token));

        // Temporary output directory
        logger.warn("The output directory parameter ('{}') is set to the JOB dir.", OUTPUT_DIR_PARAM);
        updatedParams.getOptions().remove(O_PARAM);
        updatedParams.getOptions().put(OUTPUT_DIR_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath());
    }

    private void checkKallistoQuantTccParams() throws ToolException {
        // Check inputs
        if (CollectionUtils.isEmpty(analysisParams.getKallistoParams().getInput())) {
            throw new ToolException("Missing input parameters: transcript-compatibility-counts-file");
        }
        updatedParams.setInput(checkPaths(analysisParams.getKallistoParams().getInput(), study, catalogManager, token));

        // Check options
        List<String> fileParams = getFileParams(QUANT_TCC_CMD, analysisParams.getKallistoParams().getOptions());
        List<String> skippedParams = getSkippedParams(QUANT_TCC_CMD, analysisParams.getKallistoParams().getOptions());
        updatedParams.setOptions(checkParams(analysisParams.getKallistoParams().getOptions(), fileParams, skippedParams, study,
                catalogManager, token));

        // Temporary output directory
        logger.warn("The output directory parameter ('{}') is set to the JOB dir.", OUTPUT_DIR_PARAM);
        updatedParams.getOptions().remove(O_PARAM);
        updatedParams.getOptions().put(OUTPUT_DIR_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath());
    }

    private List<String> getFileParams(String command, ObjectMap options) {
        List<String> fileParams = new ArrayList<>();
        for (String name : options.keySet()) {
            if (KallistoParams.isFileParam(command, name)) {
                fileParams.add(name);
            }
        }
        return fileParams;
    }

    private List<String> getSkippedParams(String command, ObjectMap options) {
        List<String> skippedParams = new ArrayList<>();
        for (String name : options.keySet()) {
            if (KallistoParams.isSkippedParam(command, name)) {
                logger.info("Skipping parameter '{}' since it will be set later or ignored", name);
                skippedParams.add(name);
            }
        }
        return skippedParams;
    }
}
