package org.opencb.opencga.analysis.wrappers.clinicalpipeline.prepare;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.ngspipeline.NgsPipelineWrapperAnalysis;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.clinical.pipeline.prepare.PrepareClinicalPipelineParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.clinicalpipeline.ClinicalPipelineUtils.*;

@ToolExecutor(id = PrepareClinicalPipelineWrapperAnalysisExecutor.ID,
        tool = NgsPipelineWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class PrepareClinicalPipelineWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor {

    public static final String ID = PrepareClinicalPipelineWrapperAnalysis.ID + "-local";

    private String study;

    private Path scriptPath;
    private PrepareClinicalPipelineParams prepareParams;

    @Override
    protected void run() throws Exception {
        try {
            // Input bindings
            List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
            Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH);
            inputBindings.add(new AbstractMap.SimpleEntry<>(scriptPath.toAbsolutePath().toString(), virtualScriptPath.toString()));

            // Read only input bindings
            Set<String> readOnlyInputBindings = new HashSet<>();
            readOnlyInputBindings.add(virtualScriptPath.toString());

            String reference = prepareParams.getReferenceGenome();
            if (!isURL(reference)) {
                // We need to bind to a virtual path
                inputBindings.add(new AbstractMap.SimpleEntry<>(reference, reference));
                readOnlyInputBindings.add(reference);
            }

            // Output binding
            AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(getOutDir().toAbsolutePath().toString(),
                    getOutDir().toAbsolutePath().toString());

            // Python command line
            String params = "python3 " + virtualScriptPath + "/" + NGS_PIPELINE_SCRIPT + " " + PREPARERE_NGS_PIPELINE_SCRIPT_COMMAND
                    + " -r " + reference
                    + " -o " + getOutDir().toAbsolutePath();
            params += (" -i " + REFERENCE_GENOME_INDEX);
            if (CollectionUtils.isNotEmpty(prepareParams.getIndexes())) {
                params += ("," + StringUtils.join(prepareParams.getIndexes(), ","));
            }

            // Execute Python script in docker
            String dockerImage = getDockerImageName() + ":" + getDockerImageVersion();

            String dockerCli = buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params,
                    getDefaultDockerParams());
            addEvent(Event.Type.INFO, "Docker command line: " + dockerCli);
            logger.info("Docker command line: {}", dockerCli);

            // Execute docker command line
            runCommandLine(dockerCli);
        } catch (IOException | ToolException e) {
            throw new ToolExecutorException(e);
        }
    }

    public String getStudy() {
        return study;
    }

    public PrepareClinicalPipelineWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Path getScriptPath() {
        return scriptPath;
    }

    public PrepareClinicalPipelineWrapperAnalysisExecutor setScriptPath(Path scriptPath) {
        this.scriptPath = scriptPath;
        return this;
    }

    public PrepareClinicalPipelineParams getPrepareParams() {
        return prepareParams;
    }

    public PrepareClinicalPipelineWrapperAnalysisExecutor setPrepareParams(PrepareClinicalPipelineParams prepareParams) {
        this.prepareParams = prepareParams;
        return this;
    }
}
