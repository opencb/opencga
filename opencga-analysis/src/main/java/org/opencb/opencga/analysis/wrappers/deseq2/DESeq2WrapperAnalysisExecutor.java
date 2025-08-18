package org.opencb.opencga.analysis.wrappers.deseq2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.wrappers.BaseDockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.hisat2.Hisat2WrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Analysis;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.WrapperParams;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2Input;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2Params;
import org.opencb.opencga.core.models.wrapper.deseq2.DESeq2WrapperParams;
import org.opencb.opencga.core.models.wrapper.hisat2.Hisat2Params;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.WrapperUtils.*;
import static org.opencb.opencga.core.models.wrapper.hisat2.Hisat2Params.X_PARAM;

@ToolExecutor(id = DESeq2WrapperAnalysisExecutor.ID,
        tool = DESeq2WrapperAnalysis.ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class DESeq2WrapperAnalysisExecutor extends BaseDockerWrapperAnalysisExecutor {

    public static final String ID = DESeq2WrapperAnalysis.ID + "-docker";

    private DESeq2Params deSeq2Params;
    private DESeq2Params updatedParams = new DESeq2Params();

    @Override
    protected String getTool() {
        return "Rscript";
    }

    @Override
    protected WrapperParams getWrapperParams() {
        return new WrapperParams();
    }

    @Override
    protected void validateOutputDirectory() throws ToolExecutorException {
        // Nothing to do
    }

    @Override
    protected void buildVirtualParams(WrapperParams params, List<AbstractMap.SimpleEntry<String, String>> bindings,
                                      Set<String> readOnlyBindings) throws ToolException {
        // DESeq2 input
        updatedParams.getInput().setCountsFile(buildVirtualPath(deSeq2Params.getInput().getCountsFile(), "counts", bindings,
                readOnlyBindings));
        updatedParams.getInput().setMetadataFile(buildVirtualPath(deSeq2Params.getInput().getMetadataFile(), "metadata", bindings,
                readOnlyBindings));

        // DESeq2 analysis
        updatedParams.setAnalysis(deSeq2Params.getAnalysis());

        // DESeq2 output
        updatedParams.setOutput(deSeq2Params.getOutput());
        String virtualOutputPath = buildVirtualPath(OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath(), "output", bindings, readOnlyBindings);
        updatedParams.getOutput().setBasename(virtualOutputPath + deSeq2Params.getOutput().getBasename());

        // Write updated parameters to file in the output directory, it will be the input
        Path deSeq2ParamsPath = getOutDir().resolve(DESeq2WrapperAnalysis.ID + ".params.json");
        try (OutputStream outputStream = Files.newOutputStream(deSeq2ParamsPath)) {
            // Get the default ObjectMapper instance and configure the ObjectMapper to ignore all fields with null values
            ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
            objectMapper.writeValue(outputStream, updatedParams);
        } catch (IOException e) {
            logger.error("Error writing DESeq2 parameters to file '{}'", deSeq2ParamsPath, e);
        }

        // And finally, set the input parameter to the virtual path of the DESeq2 parameters file
        params.setInput(Arrays.asList(ANALYSIS_VIRTUAL_PATH + "/" + DESeq2WrapperAnalysis.ID + "/" + "deseq2_script.R",
                virtualOutputPath + deSeq2ParamsPath.getFileName()));
    }

    public DESeq2WrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public DESeq2Params getDESeq2Params() {
        return deSeq2Params;
    }

    public DESeq2WrapperAnalysisExecutor setDESeq2Params(DESeq2Params deSeq2Params) {
        this.deSeq2Params = deSeq2Params;
        return this;
    }
}
