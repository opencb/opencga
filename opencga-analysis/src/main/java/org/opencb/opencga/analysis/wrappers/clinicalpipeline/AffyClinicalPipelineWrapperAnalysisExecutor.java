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

package org.opencb.opencga.analysis.wrappers.clinicalpipeline;


import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.clinical.pipeline.AffyPipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineInput;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineSample;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.*;

@ToolExecutor(id = AffyClinicalPipelineWrapperAnalysisExecutor.ID,
        tool = AffyClinicalPipelineWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class AffyClinicalPipelineWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = AffyClinicalPipelineWrapperAnalysis.ID + "-local";

    private String study;

    private Path scriptPath;
    private AffyPipelineConfig pipelineConfig;
    private List<String> pipelineSteps;

    @Override
    protected void run() throws Exception {
        try {
            AffyPipelineConfig pipeline = ClinicalPipelineUtils.copyPipelineConfig(pipelineConfig);

            // Input bindings (and read only input bindings)
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Set<String> readOnlyInputBindings = new HashSet<>();
            List<AbstractMap.SimpleEntry<String, String>> outputBindings = new ArrayList<>();

            // Get pipeline filename
            StringBuilder pipelineFilename = new StringBuilder(PIPELINE_PARAMS_FILENAME_PREFIX);
            if (pipelineSteps.contains(QUALITY_CONTROL_PIPELINE_STEP)) {
                pipelineFilename.append("_").append(QUALITY_CONTROL_PIPELINE_STEP.replace("-", "_"));
            }
            if (pipelineSteps.contains(GENOTYPE_PIPELINE_STEP)) {
                pipelineFilename.append("_").append(GENOTYPE_PIPELINE_STEP);
            }
            pipelineFilename.append(".json");

            // Steps parameter
            StringBuilder steps = new StringBuilder();
            if (pipelineSteps.contains(QUALITY_CONTROL_PIPELINE_STEP)) {
                steps.append(QUALITY_CONTROL_PIPELINE_STEP);
            }
            if (pipelineSteps.contains(GENOTYPE_PIPELINE_STEP)) {
                if (steps.length() > 0) {
                    steps.append(",");
                }
                steps.append(GENOTYPE_PIPELINE_STEP);
            }

            // Build the script CLI to be executed in docker
            String scriptCli = buildScriptCli(pipeline, pipelineFilename.toString(), steps.toString(), inputBindings, readOnlyInputBindings,
                    outputBindings);

            // Execute Python script in docker
            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBindings.get(0), scriptCli, null);
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);

            // Execute docker command line
            runCommandLine(dockerCli);
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    private String buildScriptCli(PipelineConfig pipeline, String pipelineFilename, String steps,
                                  List<AbstractMap.SimpleEntry<String, String>> inputBindings,
                                  Set<String> readOnlyInputBindings, List<AbstractMap.SimpleEntry<String, String>> outputBindings)
            throws IOException {
        PipelineInput pipelineInput = pipeline.getInput();

        // Script binding
        Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH);
        inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.toAbsolutePath().toString(), virtualScriptPath.toString()));
        readOnlyInputBindings.add(virtualScriptPath.toString());

        // Index binding
//        Path virtualIndexPath = Paths.get(INDEX_VIRTUAL_PATH);
//        inputBindings.add(new AbstractMap.SimpleEntry<>(pipelineInput.getIndexDir(), virtualIndexPath.toString()));
//        readOnlyInputBindings.add(virtualIndexPath.toString());
//        pipelineInput.setIndexDir(virtualIndexPath.toAbsolutePath().toString());

        // Input binding, and update samples with virtual paths
        int inputCounter = 0;
        for (PipelineSample pipelineSample : pipelineInput.getSamples()) {
            List<String> virtualPaths = new ArrayList<>(pipelineSample.getFiles().size());
            for (String file : pipelineSample.getFiles()) {
                Path virtualInputPath = Paths.get(INPUT_VIRTUAL_PATH + "_" + inputCounter).resolve(Paths.get(file).getFileName());
                inputBindings.add(new AbstractMap.SimpleEntry<>(Paths.get(file).toAbsolutePath().toString(),
                        virtualInputPath.toString()));
                readOnlyInputBindings.add(virtualInputPath.toString());
                inputCounter++;

                // Add to the list of virtual files
                virtualPaths.add(virtualInputPath.toString());
            }
            pipelineSample.setFiles(virtualPaths);
        }

        // Pipeline params binding
        Path pipelineParamsPath = getOutDir().resolve(pipelineFilename);
        Path virtualPipelineParamsPath = Paths.get(INPUT_VIRTUAL_PATH).resolve(pipelineParamsPath.getFileName());
        // Write the JSON file for Python script
        JacksonUtils.getDefaultObjectMapper().writerFor(PipelineConfig.class).writeValue(pipelineParamsPath.toFile(), pipeline);
        inputBindings.add(new AbstractMap.SimpleEntry<>(pipelineParamsPath.toAbsolutePath().toString(),
                virtualPipelineParamsPath.toString()));
        readOnlyInputBindings.add(virtualPipelineParamsPath.toString());

        // Output binding
        outputBindings.add(new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(), OUTPUT_VIRTUAL_PATH));

        // Main command line and params, e.g.:
        // ./analysis/ngs-pipeline/main.py affy
        // -o /tmp/ngs/
        // -p /input/pipeline.json
        // --steps quality-control,genotype
        return "python3 " + virtualScriptPath + "/" + NGS_PIPELINE_SCRIPT + " " + AFFY_PIPELINE_SCRIPT_COMMAND
                + " -o " + OUTPUT_VIRTUAL_PATH
                + " -p " + virtualPipelineParamsPath
                + " --steps " + steps;
    }

    public String getStudy() {
        return study;
    }

    public AffyClinicalPipelineWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Path getScriptPath() {
        return scriptPath;
    }

    public AffyClinicalPipelineWrapperAnalysisExecutor setScriptPath(Path scriptPath) {
        this.scriptPath = scriptPath;
        return this;
    }

    public AffyPipelineConfig getPipelineConfig() {
        return pipelineConfig;
    }

    public AffyClinicalPipelineWrapperAnalysisExecutor setPipelineConfig(AffyPipelineConfig pipelineConfig) {
        this.pipelineConfig = pipelineConfig;
        return this;
    }

    public List<String> getPipelineSteps() {
        return pipelineSteps;
    }

    public AffyClinicalPipelineWrapperAnalysisExecutor setPipelineSteps(List<String> pipelineSteps) {
        this.pipelineSteps = pipelineSteps;
        return this;
    }
}
