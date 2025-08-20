package org.opencb.opencga.analysis.wrappers.star_fusion;

import org.opencb.opencga.analysis.wrappers.BaseDockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.WrapperParams;
import org.opencb.opencga.core.models.wrapper.star.StarParams;
import org.opencb.opencga.core.models.wrapper.star_fusion.StarFusionParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis.ID;
import static org.opencb.opencga.core.models.wrapper.star_fusion.StarFusionParams.OUTPUT_DIR_PARAM;

@ToolExecutor(id = StarFusionWrapperAnalysisExecutor.ID,
        tool = ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class StarFusionWrapperAnalysisExecutor extends BaseDockerWrapperAnalysisExecutor {

    public static final String ID = StarFusionWrapperAnalysis.ID + "-docker";

    private StarFusionParams starFusionParams;

    @Override
    protected String getTool() {
        return "STAR-Fusion";
    }

    @Override
    protected WrapperParams getWrapperParams() {
        WrapperParams params = new WrapperParams();
        params.setOptions(starFusionParams.getOptions());
        return params;
    }

    @Override
    protected void validateOutputDirectory() throws ToolExecutorException {
        String outDir = starFusionParams.getOptions().getString(OUTPUT_DIR_PARAM)
                .substring(WrapperUtils.OUTPUT_FILE_PREFIX.length());
        if (!getOutDir().toAbsolutePath().startsWith(outDir)) {
            throw new ToolExecutorException("Output file parameter '" + OUTPUT_DIR_PARAM + "' does not match expected output directory '"
                    + getOutDir() + "'");
        }
    }

    public StarFusionWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public StarFusionParams getStarFusionParams() {
        return starFusionParams;
    }

    public StarFusionWrapperAnalysisExecutor setStarFusionParams(StarFusionParams starFusionParams) {
        this.starFusionParams = starFusionParams;
        return this;
    }
}
