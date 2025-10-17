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


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.clinical.pipeline.*;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.*;

@ToolExecutor(id = ClinicalPipelineGenomicsWrapperAnalysisExecutor.ID,
        tool = ClinicalPipelineGenomicsWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class ClinicalPipelineGenomicsWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = ClinicalPipelineGenomicsWrapperAnalysis.ID + "-local";

    private String study;

    private Path scriptPath;
    private PipelineConfig pipelineConfig;
    private List<String> pipelineSteps;

    @Override
    protected void run() throws Exception {
        // First, run QC (e.g. FastQC) and alignment (e.g. BWA)
        if (pipelineSteps.contains(QUALITY_CONTROL_PIPELINE_STEP) || pipelineSteps.contains(ALIGNMENT_PIPELINE_STEP)) {
            PipelineConfig pipeline = ClinicalPipelineUtils.copyPipelineConfig(pipelineConfig);
            runQcAndAlignment(pipeline);
        }

        // Then, run the variant calling (e.g. GATK HaplotypeCaller)
        if (pipelineSteps.contains(VARIANT_CALLING_PIPELINE_STEP)) {
            PipelineConfig pipeline = ClinicalPipelineUtils.copyPipelineConfig(pipelineConfig);
            runVariantCalling(pipeline);
        }
    }

    private void runQcAndAlignment(PipelineConfig pipeline) throws ToolExecutorException {
        try {
            // Input bindings (and read only input bindings)
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Set<String> readOnlyInputBindings = new HashSet<>();
            List<AbstractMap.SimpleEntry<String, String>> outputBindings = new ArrayList<>();

            // Get pipeline filename
            StringBuilder pipelineFilename = new StringBuilder(PIPELINE_PARAMS_FILENAME_PREFIX);
            if (pipelineSteps.contains(QUALITY_CONTROL_PIPELINE_STEP)) {
                pipelineFilename.append("_").append(QUALITY_CONTROL_PIPELINE_STEP.replace("-", "_"));
            }
            if (pipelineSteps.contains(ALIGNMENT_PIPELINE_STEP)) {
                pipelineFilename.append("_").append(ALIGNMENT_PIPELINE_STEP);
            }
            pipelineFilename.append(".json");

            // Steps parameter
            StringBuilder steps = new StringBuilder();
            if (pipelineSteps.contains(QUALITY_CONTROL_PIPELINE_STEP)) {
                steps.append(QUALITY_CONTROL_PIPELINE_STEP);
            }
            if (pipelineSteps.contains(ALIGNMENT_PIPELINE_STEP)) {
                if (steps.length() > 0) {
                    steps.append(",");
                }
                steps.append(ALIGNMENT_PIPELINE_STEP);
            }

            // Update alignment index if necessary
            if (pipelineSteps.contains(QUALITY_CONTROL_PIPELINE_STEP)
                    && pipeline.getAlignmentStep() != null
                    && pipeline.getAlignmentStep().getTool() != null
                    && StringUtils.isNotEmpty(pipeline.getAlignmentStep().getTool().getIndex())) {
                PipelineAlignmentTool tool = pipeline.getAlignmentStep().getTool();
                Path virtualPath = Paths.get(INDEX_VIRTUAL_PATH + "_" + tool.getId().replace("-", "_"));
                inputBindings.add(new AbstractMap.SimpleEntry<>(Paths.get(tool.getIndex()).toAbsolutePath().toString(),
                        virtualPath.toString()));
                readOnlyInputBindings.add(virtualPath.toString());

                // Update the tool index
                tool.setIndex(virtualPath.toString());
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

            if (pipelineSteps.contains(ALIGNMENT_PIPELINE_STEP) && pipelineSteps.contains(VARIANT_CALLING_PIPELINE_STEP)) {
                prepareCallerInputFromAlignmentOutput(pipeline);
            }
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    private void runVariantCalling(PipelineConfig pipeline) throws ToolExecutorException {
        try {
            // Input bindings (and read only input bindings)
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Set<String> readOnlyInputBindings = new HashSet<>();
            List<AbstractMap.SimpleEntry<String, String>> outputBindings = new ArrayList<>();

            // Get pipeline filename
            String pipelineFilename = PIPELINE_PARAMS_FILENAME_PREFIX + "_" + VARIANT_CALLING_PIPELINE_STEP.replace("-", "_") + ".json";

            // Update variant calling reference if necessary
            if (pipeline.getVariantCallingStep() != null && CollectionUtils.isNotEmpty(pipeline.getVariantCallingStep().getTools())) {
                for (PipelineVariantCallingTool tool : pipeline.getVariantCallingStep().getTools()) {
                    if (StringUtils.isNotEmpty(tool.getReference())) {
                        Path virtualPath = Paths.get(REFERENCE_VIRTUAL_PATH + "_" + tool.getId().replace("-", "_"));
                        inputBindings.add(new AbstractMap.SimpleEntry<>(Paths.get(tool.getReference()).toAbsolutePath().toString(),
                                virtualPath.toString()));
                        readOnlyInputBindings.add(virtualPath.toString());

                        // Update the tool reference
                        tool.setReference(virtualPath.toString());
                    }
                }
            }

            // Build the script CLI to be executed in docker
            String scriptCli = buildScriptCli(pipeline, pipelineFilename, VARIANT_CALLING_PIPELINE_STEP, inputBindings,
                    readOnlyInputBindings, outputBindings);

            // Execute Python script in GATK docker
            String dockerImage =  "broadinstitute/gatk:4.6.2.0";

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
        Path virtualIndexPath = Paths.get(INDEX_VIRTUAL_PATH);
        inputBindings.add(new AbstractMap.SimpleEntry<>(pipelineInput.getIndexDir(), virtualIndexPath.toString()));
        readOnlyInputBindings.add(virtualIndexPath.toString());
        pipelineInput.setIndexDir(virtualIndexPath.toAbsolutePath().toString());

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
        // Write the JSON file expected by the Python script
        writePipeline(pipeline, pipelineParamsPath);
        inputBindings.add(new AbstractMap.SimpleEntry<>(pipelineParamsPath.toAbsolutePath().toString(),
                virtualPipelineParamsPath.toString()));
        readOnlyInputBindings.add(virtualPipelineParamsPath.toString());

        // Output binding
        outputBindings.add(new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(), OUTPUT_VIRTUAL_PATH));

        // Main command line and params, e.g.:
        // ./analysis/ngs-pipeline/main.py genomics
        // -o /tmp/ngs/
        // -p /input/pipeline.json
        // --steps quality-control,alignment
        return "python3 " + virtualScriptPath + "/" + NGS_PIPELINE_SCRIPT + " " + GENOMICS_NGS_PIPELINE_SCRIPT_COMMAND
                + " -o " + OUTPUT_VIRTUAL_PATH
                + " -p " + virtualPipelineParamsPath
                + " --steps " + steps;
    }

    private void writePipeline(PipelineConfig pipeline, Path pipelineParamsPath) throws IOException {
        // Convert the pipeline configuration to an ObjectMap to be understand by the Python script; and then write it as a JSON file
        ObjectMap objectMap = new ObjectMap();
        objectMap.put("name", pipeline.getName());
        objectMap.put("version", pipeline.getVersion());
        objectMap.put("description", pipeline.getDescription());
        objectMap.put("input", pipeline.getInput());

        // Steps in an array
        ObjectMapper mapper = JacksonUtils.getDefaultObjectMapper();
        List<ObjectMap> steps = new ArrayList<>();
        if (pipeline.getQualityControlStep() != null) {
            ObjectMap step = mapper.convertValue(pipeline.getQualityControlStep(), ObjectMap.class);
            step.put("id", QUALITY_CONTROL_PIPELINE_STEP);
            steps.add(step);
        }
        if (pipeline.getAlignmentStep() != null) {
            ObjectMap step = mapper.convertValue(pipeline.getAlignmentStep(), ObjectMap.class);
            step.put("id", ALIGNMENT_PIPELINE_STEP);
            steps.add(step);
        }
        if (pipeline.getVariantCallingStep() != null) {
            ObjectMap step = mapper.convertValue(pipeline.getVariantCallingStep(), ObjectMap.class);
            step.put("id", VARIANT_CALLING_PIPELINE_STEP);
            steps.add(step);
        }
        objectMap.put("steps", steps);

        mapper.writerFor(ObjectMap.class).writeValue(pipelineParamsPath.toFile(), objectMap);
    }

    private void prepareCallerInputFromAlignmentOutput(PipelineConfig pipeline) throws ToolExecutorException {
        // Check output files from the alignment step from the output directory (alignment directory) and update the input of the
        // pipeline configuration to be executed by the variant-calling step (overwritten the pipeline configuration input)
        Path alignmentPath = getOutDir().resolve(ALIGNMENT_PIPELINE_STEP);
        if (Files.exists(alignmentPath) && Files.exists(alignmentPath.resolve("DONE"))) {
            for (PipelineSample sample : pipeline.getInput().getSamples()) {
                String basename = getBaseFilename(sample.getFiles());
                Path bamPath = alignmentPath.resolve(basename + ".sorted.bam");
                if (!Files.exists(bamPath)) {
                    throw new ToolExecutorException("Missing BAM file from alignment step: " + bamPath);
                }
                sample.setFiles(Collections.singletonList(bamPath.toAbsolutePath().toString()));
            }
        } else {
            throw new ToolExecutorException("Missing DONE file from alignment step: " + alignmentPath.resolve("DONE"));
        }
    }

    public String getStudy() {
        return study;
    }

    public ClinicalPipelineGenomicsWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Path getScriptPath() {
        return scriptPath;
    }

    public ClinicalPipelineGenomicsWrapperAnalysisExecutor setScriptPath(Path scriptPath) {
        this.scriptPath = scriptPath;
        return this;
    }

    public PipelineConfig getPipelineConfig() {
        return pipelineConfig;
    }

    public ClinicalPipelineGenomicsWrapperAnalysisExecutor setPipelineConfig(PipelineConfig pipelineConfig) {
        this.pipelineConfig = pipelineConfig;
        return this;
    }

    public List<String> getPipelineSteps() {
        return pipelineSteps;
    }

    public ClinicalPipelineGenomicsWrapperAnalysisExecutor setPipelineSteps(List<String> pipelineSteps) {
        this.pipelineSteps = pipelineSteps;
        return this;
    }
}
