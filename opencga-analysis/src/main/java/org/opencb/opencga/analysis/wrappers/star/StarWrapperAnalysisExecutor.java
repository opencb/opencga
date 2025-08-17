package org.opencb.opencga.analysis.wrappers.star;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.wrappers.BaseDockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.WrapperParams;
import org.opencb.opencga.core.models.wrapper.star.StarParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.util.Map;

import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis.ID;

@ToolExecutor(id = StarWrapperAnalysisExecutor.ID,
        tool = ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class StarWrapperAnalysisExecutor extends BaseDockerWrapperAnalysisExecutor {

    public static final String ID = StarWrapperAnalysis.ID + "-docker";

    private StarParams starParams;

    @Override
    protected String getTool() {
        return "STAR";
    }

    @Override
    protected WrapperParams getWrapperParams() {
        WrapperParams params = new WrapperParams();
        params.setOptions(starParams.getOptions());
        return params;
    }

    @Override
    protected void validateOutputDirectory() throws ToolExecutorException {
        for (Map.Entry<String, Object> entry : starParams.getOptions().entrySet()) {
            if (entry.getValue() instanceof String) {
                String value = (String) entry.getValue();
                if (StringUtils.isNotEmpty(value) && value.startsWith(WrapperUtils.OUTPUT_FILE_PREFIX)) {
                    String outDir = value.substring(WrapperUtils.OUTPUT_FILE_PREFIX.length());
                    if (!getOutDir().toAbsolutePath().startsWith(outDir)) {
                        throw new ToolExecutorException("Output file parameter '" + entry.getKey() + "' does not match expected output directory '" + getOutDir() + "'");
                    }
                }
            }
        }
    }

    public StarWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public StarParams getStarWrapperParams() {
        return starParams;
    }

    public StarWrapperAnalysisExecutor setStarParams(StarParams starParams) {
        this.starParams = starParams;
        return this;
    }
}
