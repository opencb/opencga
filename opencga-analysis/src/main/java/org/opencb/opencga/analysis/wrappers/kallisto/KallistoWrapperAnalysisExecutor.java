package org.opencb.opencga.analysis.wrappers.kallisto;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.wrappers.BaseDockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.WrapperParams;
import org.opencb.opencga.core.models.wrapper.kallisto.KallistoParams;
import org.opencb.opencga.core.models.wrapper.multiqc.MultiQcParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.util.AbstractMap;
import java.util.List;
import java.util.Set;

import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis.ID;
import static org.opencb.opencga.core.models.wrapper.kallisto.KallistoParams.*;

@ToolExecutor(id = KallistoWrapperAnalysisExecutor.ID,
        tool = ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class KallistoWrapperAnalysisExecutor extends BaseDockerWrapperAnalysisExecutor {

    public static final String ID = KallistoWrapperAnalysis.ID + "-docker";

    private KallistoParams kallistoParams;

    @Override
    protected String getTool() {
        return KallistoWrapperAnalysis.ID;
    }

    @Override
    protected WrapperParams getWrapperParams() {
        return WrapperParams.copy(kallistoParams);
    }

    @Override
    protected void validateOutputDirectory() throws ToolExecutorException {
        switch (kallistoParams.getCommand()) {
            case QUANT_CMD: {
                String outDir = ((String) kallistoParams.getOptions().get(KallistoParams.OUTPUT_DIR_PARAM))
                        .substring(WrapperUtils.OUTPUT_FILE_PREFIX.length());
                if (!getOutDir().toAbsolutePath().toString().equals(outDir)) {
                    throw new ToolExecutorException("The output directory '" + outDir + "' does not match the expected job directory '"
                            + getOutDir() + "'");
                }
                break;
            }
        }
    }

    @Override
    protected void buildVirtualParams(WrapperParams params,
                                      List<AbstractMap.SimpleEntry<String, String>> bindings,
                                      Set<String> readOnlyBindings) throws ToolException {
        // Call the super method to build the virtual params
        super.buildVirtualParams(params, bindings, readOnlyBindings);

        switch (kallistoParams.getCommand()) {
            case INDEX_CMD: {
                String virtualOutputPath = buildVirtualPath(OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath(), "output", bindings,
                        readOnlyBindings);
                String indexFilename = null;
                if (params.getOptions().containsKey(INDEX_PARAM)) {
                    indexFilename = params.getOptions().getString(INDEX_PARAM);
                } else {
                    indexFilename = kallistoParams.getOptions().getString(I_PARAM);
                }
                if (StringUtils.isEmpty(indexFilename)) {
                    throw new ToolExecutorException("Missing mandatory parameter '" + INDEX_PARAM + "'. Please, specify the Kallisto"
                            + " index file.");
                }
                params.getOptions().put(INDEX_PARAM, virtualOutputPath + indexFilename);
                break;
            }
        }
    }

    public KallistoWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public KallistoParams getKallistoParams() {
        return kallistoParams;
    }

    public KallistoWrapperAnalysisExecutor setKallistoParams(KallistoParams kallistoParams) {
        this.kallistoParams = kallistoParams;
        return this;
    }
}
