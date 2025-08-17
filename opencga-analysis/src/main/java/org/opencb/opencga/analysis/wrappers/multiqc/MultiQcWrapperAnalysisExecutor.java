package org.opencb.opencga.analysis.wrappers.multiqc;

import org.opencb.opencga.analysis.wrappers.BaseDockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.WrapperParams;
import org.opencb.opencga.core.models.wrapper.multiqc.MultiQcParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

@ToolExecutor(id = MultiQcWrapperAnalysisExecutor.ID,
        tool = MultiQcWrapperAnalysis.ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class MultiQcWrapperAnalysisExecutor extends BaseDockerWrapperAnalysisExecutor {

    public static final String ID = MultiQcWrapperAnalysis.ID + "-docker";

    private MultiQcParams multiQcParams;

    @Override
    protected String getTool() {
        return "multiqc";
    }

    @Override
    protected WrapperParams getWrapperParams() {
        WrapperParams params = new WrapperParams();
        params.setInput(multiQcParams.getInput());
        params.setOptions(multiQcParams.getOptions());
        return params;
    }

    @Override
    protected void validateOutputDirectory() throws ToolExecutorException {
        String outDir = ((String) multiQcParams.getOptions().get(MultiQcParams.OUTDIR_PARAM))
                .substring(WrapperUtils.OUTPUT_FILE_PREFIX.length());
        if (!getOutDir().toAbsolutePath().toString().equals(outDir)) {
            throw new ToolExecutorException("The output directory '" + outDir + "' does not match the expected job directory '"
                    + getOutDir() + "'");
        }
    }

    public MultiQcWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public MultiQcParams getMultiQcParams() {
        return multiQcParams;
    }

    public MultiQcWrapperAnalysisExecutor setMultiQcParams(MultiQcParams multiQcParams) {
        this.multiQcParams = multiQcParams;
        return this;
    }
}
