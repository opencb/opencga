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

package org.opencb.opencga.analysis.wrappers.clinicalpipeline.genomics;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.clinical.pipeline.PipelineSample;
import org.opencb.opencga.core.models.clinical.pipeline.genomics.GenomicsPipelineConfig;
import org.opencb.opencga.core.models.clinical.pipeline.genomics.GenomicsPipelineInput;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.*;

@ToolExecutor(id = GenomicsClinicalPipelineWrapperAnalysisExecutor.ID,
        tool = GenomicsClinicalPipelineWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class GenomicsClinicalPipelineWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = GenomicsClinicalPipelineWrapperAnalysis.ID + "-local";

    private String study;

    private Path scriptPath;
    private GenomicsPipelineConfig pipelineConfig;
    private List<String> pipelineSteps;

    @Override
    protected void run() throws Exception {
        try {
            GenomicsPipelineConfig pipeline = ClinicalPipelineUtils.copyPipelineConfig(pipelineConfig);

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

    private String buildScriptCli(GenomicsPipelineConfig pipeline, List<AbstractMap.SimpleEntry<String, String>> inputBindings,
                                  Set<String> readOnlyInputBindings, List<AbstractMap.SimpleEntry<String, String>> outputBindings)
            throws IOException, ToolException {
        // Get the pipeline configuration file path
        Path pipelineConfigPath = getOutDir().resolve(buildPipelineFilename(pipelineSteps));

        // Set input bindings
        setInputBindings(pipeline.getInput(), pipelineConfigPath, scriptPath, inputBindings, readOnlyInputBindings);

        // TODO: Handle special cases for alignment (i.e., index dir) and variant calling steps (i.e., reference dir)

        // Output binding
        outputBindings.add(new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(), getOutDir().toAbsolutePath().toString()));

        // Write the pipeline configuration file since it has been modified with virtual paths to be executed in docker
        JacksonUtils.getDefaultObjectMapper().writerFor(GenomicsPipelineConfig.class).writeValue(pipelineConfigPath.toFile(), pipeline);

        // And return the Python command line
        return "python3 " + SCRIPT_VIRTUAL_PATH + "/" + NGS_PIPELINE_SCRIPT + " " + GENOMICS_NGS_PIPELINE_SCRIPT_COMMAND
                + " -o " + getOutDir().toAbsolutePath()
                + " -p " + getVirtualPath(pipelineConfigPath, inputBindings)
                + " --steps " + buildStepsParam(pipelineSteps);
    }

    public static void setInputBindings(GenomicsPipelineInput pipelineInput, Path pipelineConfigPath, Path scriptPath,
                                        List<AbstractMap.SimpleEntry<String, String>> inputBindings, Set<String> readOnlyInputBindings)
            throws IOException {

        // Common bindings
        setCommonInputBindings(pipelineConfigPath, scriptPath, inputBindings, readOnlyInputBindings);

        // Input binding, and update samples with virtual paths
        if (!CollectionUtils.isEmpty(pipelineInput.getSamples())) {
            for (PipelineSample pipelineSample : pipelineInput.getSamples()) {
                List<String> virtualPaths = new ArrayList<>(pipelineSample.getFiles().size());
                for (int i = 0; i < pipelineSample.getFiles().size(); i++) {
                    Path path = Paths.get(pipelineSample.getFiles().get(i)).toAbsolutePath();
                    inputBindings.add(new AbstractMap.SimpleEntry<>(path.toString(), path.toString()));
                    readOnlyInputBindings.add(path.toString());

                    // Add to the list of virtual files
                    virtualPaths.add(path.toString());
                }
                pipelineSample.setFiles(virtualPaths);
            }
        }

        // Index dir binding
        if (!StringUtils.isEmpty(pipelineInput.getIndexDir())) {
            Path path = Paths.get(pipelineInput.getIndexDir()).toAbsolutePath();
            inputBindings.add(new AbstractMap.SimpleEntry<>(path.toString(), path.toString()));
            readOnlyInputBindings.add(path.toString());
            pipelineInput.setIndexDir(path.toString());
        }
    }

    public String getStudy() {
        return study;
    }

    public GenomicsClinicalPipelineWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Path getScriptPath() {
        return scriptPath;
    }

    public GenomicsClinicalPipelineWrapperAnalysisExecutor setScriptPath(Path scriptPath) {
        this.scriptPath = scriptPath;
        return this;
    }

    public GenomicsPipelineConfig getPipelineConfig() {
        return pipelineConfig;
    }

    public GenomicsClinicalPipelineWrapperAnalysisExecutor setPipelineConfig(GenomicsPipelineConfig pipelineConfig) {
        this.pipelineConfig = pipelineConfig;
        return this;
    }

    public List<String> getPipelineSteps() {
        return pipelineSteps;
    }

    public GenomicsClinicalPipelineWrapperAnalysisExecutor setPipelineSteps(List<String> pipelineSteps) {
        this.pipelineSteps = pipelineSteps;
        return this;
    }
}
