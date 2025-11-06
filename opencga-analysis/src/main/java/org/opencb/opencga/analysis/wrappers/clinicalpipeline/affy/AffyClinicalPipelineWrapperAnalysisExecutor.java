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

package org.opencb.opencga.analysis.wrappers.clinicalpipeline.affy;


import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.affy.AffyPipelineConfig;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.IOException;
import java.nio.file.Path;
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

            // Build the script CLI to be executed in docker
            String scriptCli = buildScriptCli(pipeline, inputBindings, readOnlyInputBindings, outputBindings);

            // Execute Python script in docker
            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();
            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBindings.get(0), scriptCli,
                    getDefaultDockerParams());
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);

            // Execute docker command line
            runCommandLine(dockerCli);
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    private String buildScriptCli(PipelineConfig pipeline, List<AbstractMap.SimpleEntry<String, String>> inputBindings,
                                  Set<String> readOnlyInputBindings, List<AbstractMap.SimpleEntry<String, String>> outputBindings)
            throws IOException, ToolException {
        // Get the pipeline configuration file path
        Path pipelineConfigPath = getOutDir().resolve(buildPipelineFilename(pipelineSteps));

        // Set input bindings
        setInputBindings(pipeline.getInput(), pipelineConfigPath, pipelineSteps, scriptPath, inputBindings, readOnlyInputBindings);

        // Output binding
        outputBindings.add(new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(), OUTPUT_VIRTUAL_PATH));

        // Write the pipeline configuration file since it has been modified with virtual paths to be executed in docker
        JacksonUtils.getDefaultObjectMapper().writerFor(AffyPipelineConfig.class).writeValue(pipelineConfigPath.toFile(), pipeline);

        // And return the Python command line
        return "python3 " + SCRIPT_VIRTUAL_PATH + "/" + NGS_PIPELINE_SCRIPT + " " + AFFY_PIPELINE_SCRIPT_COMMAND
                + " -o " + OUTPUT_VIRTUAL_PATH
                + " -p " + getVirtualPath(pipelineConfigPath, inputBindings)
                + " --steps " + buildStepsParam(pipelineSteps);
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
