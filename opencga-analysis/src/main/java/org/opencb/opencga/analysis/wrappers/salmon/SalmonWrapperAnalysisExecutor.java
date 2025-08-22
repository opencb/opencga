package org.opencb.opencga.analysis.wrappers.salmon;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.wrappers.BaseDockerWrapperAnalysisExecutor;
import org.opencb.opencga.analysis.wrappers.WrapperUtils;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.WrapperParams;
import org.opencb.opencga.core.models.wrapper.salmon.SalmonParams;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis.ID;
import static org.opencb.opencga.core.models.wrapper.salmon.SalmonParams.INDEX_CMD;

@ToolExecutor(id = SalmonWrapperAnalysisExecutor.ID,
        tool = ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class SalmonWrapperAnalysisExecutor extends BaseDockerWrapperAnalysisExecutor {

    public static final String ID = SalmonWrapperAnalysis.ID + "-docker";

    private SalmonParams salmonParams;

    @Override
    protected String getTool() {
        return SalmonWrapperAnalysis.ID;
    }

    @Override
    protected WrapperParams getWrapperParams() {
        return new WrapperParams(StringUtils.isEmpty(salmonParams.getCommand()) ? null : salmonParams.getCommand(),
                null, salmonParams.getOptions());
    }

    @Override
    protected void validateOutputDirectory() throws ToolExecutorException {
        switch (salmonParams.getCommand()) {
            case INDEX_CMD: {
                String outDir = ((String) salmonParams.getOptions().get(SalmonParams.INDEX_PARAM))
                        .substring(WrapperUtils.OUTPUT_FILE_PREFIX.length());
                if (!getOutDir().toAbsolutePath().toString().equals(outDir)) {
                    throw new ToolExecutorException("The output directory '" + outDir + "' does not match the expected job directory '"
                            + getOutDir() + "'");
                }
                break;
            }
        }
    }

//    @Override
//    protected void buildVirtualParams(WrapperParams params,
//                                      List<AbstractMap.SimpleEntry<String, String>> bindings,
//                                      Set<String> readOnlyBindings) throws ToolException {
//        // Call the super method to build the virtual params
//        super.buildVirtualParams(params, bindings, readOnlyBindings);
//
//        switch (salmonParams.getCommand()) {
//            case INDEX_CMD: {
//                String virtualOutputPath = buildVirtualPath(OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath(), "output", bindings,
//                        readOnlyBindings);
//                String indexFilename = null;
//                if (params.getOptions().containsKey(INDEX_PARAM)) {
//                    indexFilename = params.getOptions().getString(INDEX_PARAM);
//                } else {
//                    indexFilename = salmonParams.getOptions().getString(I_PARAM);
//                }
//                if (StringUtils.isEmpty(indexFilename)) {
//                    throw new ToolExecutorException("Missing mandatory parameter '" + INDEX_PARAM + "'. Please, specify the Kallisto"
//                            + " index file.");
//                }
//                params.getOptions().put(INDEX_PARAM, virtualOutputPath + indexFilename);
//                break;
//            }
//        }
//    }

    public SalmonWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public SalmonParams getSalmonParams() {
        return salmonParams;
    }

    public SalmonWrapperAnalysisExecutor setSalmonParams(SalmonParams salmonParams) {
        this.salmonParams = salmonParams;
        return this;
    }
}
