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

package org.opencb.opencga.analysis.wrappers.salmon;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.wrapper.salmon.SalmonParams;
import org.opencb.opencga.core.models.wrapper.salmon.SalmonWrapperParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.OUTPUT_FILE_PREFIX;
import static org.opencb.opencga.analysis.wrappers.WrapperUtils.checkParams;
import static org.opencb.opencga.core.models.wrapper.salmon.SalmonParams.*;

@Tool(id = SalmonWrapperAnalysis.ID, resource = Enums.Resource.ALIGNMENT, description = SalmonWrapperAnalysis.DESCRIPTION)
public class SalmonWrapperAnalysis extends OpenCgaToolScopeStudy {

    public final static String ID = "salmon";
    public final static String DESCRIPTION = "Salmon is a tool for quantifying the expression of transcripts using RNA-seq data.";

    @ToolParams
    protected final SalmonWrapperParams analysisParams = new SalmonWrapperParams();

    private SalmonParams updatedParams = new SalmonParams();

    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Command
        String command = analysisParams.getSalmonParams().getCommand();
        ObjectMap options = analysisParams.getSalmonParams().getOptions();
        if (StringUtils.isEmpty(command) && MapUtils.isEmpty(options)) {
            throw new ToolException("Salmon command and options are missing. Please, specify the Salmon command and/or options to run.");
        }

        if (StringUtils.isEmpty(command)) {
            updatedParams.setOptions(options);
        } else {
            updatedParams.setCommand(command);

            switch (command) {
                case INDEX_CMD: {
                    checkSalmonIndexParams();
                    break;
                }

                case QUANT_CMD: {
                    checkSalmonQuantParams();
                    break;
                }

                case ALEVIN_CMD: {
                    checkSalmonAlevinParams();
                    break;
                }

                case SWIM_CMD: {
                    checkSalmonSwimParams();
                    break;

                }

                case QUANTMERGE_CMD: {
                    checkSalmonQuantmergeParams();
                    break;
                }

                default:
                    throw new ToolException("Unsupported Salmon command '" + updatedParams.getCommand() + "'. Supported commands are: "
                            + StringUtils.join(VALID_COMMANDS, ", ") + ".");
            }
        }
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(ID);
    }

    protected void run() throws ToolException, IOException {
        // Run Kallisto
        step(ID, this::runSalmon);
    }

    protected void runSalmon() throws ToolException {
        // Get executor
        SalmonWrapperAnalysisExecutor executor = getToolExecutor(SalmonWrapperAnalysisExecutor.class);

        // Set parameters and execute
        executor.setStudy(study)
                .setSalmonParams(updatedParams)
                .execute();
    }

    private void checkSalmonIndexParams() throws ToolException {
        // Check options
        if (MapUtils.isEmpty(analysisParams.getSalmonParams().getOptions())
                || (!analysisParams.getSalmonParams().getOptions().containsKey(INDEX_PARAM)
                && !analysisParams.getSalmonParams().getOptions().containsKey(I_PARAM))
                || (StringUtils.isEmpty(analysisParams.getSalmonParams().getOptions().getString(INDEX_PARAM))
                && StringUtils.isEmpty(analysisParams.getSalmonParams().getOptions().getString(I_PARAM)))) {
            throw new ToolException("Missing mandatory parameter 'index'. Please, specify the output index file name."
                    + " It can be set using the '" + INDEX_PARAM + "' or '" + I_PARAM + "' parameters.");
        }
        List<String> fileParams = getFileParams(INDEX_CMD, analysisParams.getSalmonParams().getOptions());
        List<String> skippedParams = getSkippedParams(INDEX_CMD, analysisParams.getSalmonParams().getOptions());
        updatedParams.setOptions(checkParams(analysisParams.getSalmonParams().getOptions(), fileParams, skippedParams, study,
                catalogManager, token));

        // Index directory
        logger.warn("The index directory parameter ('{}') is set to the JOB dir.", INDEX_PARAM);
        updatedParams.getOptions().remove(I_PARAM);
        updatedParams.getOptions().put(INDEX_PARAM, OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath());

        // Temporary output directory
        logger.warn("The tmp directory parameter ('{}') is set to the JOB scratch dir.", TMPDIR_PARAM);
        updatedParams.getOptions().put(TMPDIR_PARAM, OUTPUT_FILE_PREFIX + getScratchDir().toAbsolutePath());
    }

    private void checkSalmonQuantParams() throws ToolException {
        throw new ToolException("Salmon command '" + QUANT_CMD + "' is not yet supported.");
    }

    private void checkSalmonAlevinParams() throws ToolException {
        throw new ToolException("Salmon command '" + ALEVIN_CMD + "' is not yet supported.");
    }

    private void checkSalmonSwimParams() throws ToolException {
        throw new ToolException("Salmon command '" + SWIM_CMD + "' is not yet supported.");
    }

    private void checkSalmonQuantmergeParams() throws ToolException {
        throw new ToolException("Salmon command '" + QUANTMERGE_CMD + "' is not yet supported.");
    }

    private List<String> getFileParams(String command, ObjectMap options) {
        List<String> fileParams = new ArrayList<>();
        for (String name : options.keySet()) {
            if (SalmonParams.isFileParam(command, name)) {
                fileParams.add(name);
            }
        }
        return fileParams;
    }

    private List<String> getSkippedParams(String command, ObjectMap options) {
        List<String> skippedParams = new ArrayList<>();
        for (String name : options.keySet()) {
            if (SalmonParams.isSkippedParam(command, name)) {
                logger.info("Skipping parameter '{}' since it will be set later or ignored", name);
                skippedParams.add(name);
            }
        }
        return skippedParams;
    }
}
