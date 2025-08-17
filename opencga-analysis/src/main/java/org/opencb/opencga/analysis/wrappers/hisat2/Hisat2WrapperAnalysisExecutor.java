package org.opencb.opencga.analysis.wrappers.hisat2;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.wrappers.BaseDockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.wrapper.WrapperParams;
import org.opencb.opencga.core.models.wrapper.hisat2.Hisat2Params;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.wrappers.multiqc.MultiQcWrapperAnalysis.ID;
import static org.opencb.opencga.core.models.wrapper.hisat2.Hisat2Params.X_PARAM;

@ToolExecutor(id = Hisat2WrapperAnalysisExecutor.ID,
        tool = ID,
        source = ToolExecutor.Source.FILE,
        framework = ToolExecutor.Framework.DOCKER)
public class Hisat2WrapperAnalysisExecutor extends BaseDockerWrapperAnalysisExecutor {

    public static final String ID = Hisat2WrapperAnalysis.ID + "-docker";

    private Hisat2Params hisat2Params;

    @Override
    protected String getTool() {
        return hisat2Params.getCommand();
    }

    @Override
    protected WrapperParams getWrapperParams() {
        WrapperParams params = new WrapperParams();
        params.setCommand(hisat2Params.getCommand());
        params.setInput(hisat2Params.getInput());
        params.setOptions(hisat2Params.getOptions());
        return params;
    }

    @Override
    protected void validateOutputDirectory() throws ToolExecutorException {
        // Nothing to do
    }

    @Override
    protected void buildVirtualParams(WrapperParams params, List<AbstractMap.SimpleEntry<String, String>> bindings,
                                      Set<String> readOnlyBindings) throws ToolException {
        switch (hisat2Params.getCommand()) {
            case Hisat2Params.HISAT2_TOOL: {
                // Set S_SAM parameter to the output directory
                String output = buildVirtualPath(OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath(), "output", bindings, readOnlyBindings);
                params.getOptions().put(Hisat2Params.S_PARAM, output + params.getOptions().getString(Hisat2Params.S_PARAM));

                // Add the index basename to the X_PARAM parameter
                Path indexPath = Paths.get(params.getOptions().getString(X_PARAM).substring(INPUT_FILE_PREFIX.length()));
                String virtualIndexPath = null;
                String indexBasename = null;
                for (File file : indexPath.toFile().listFiles()) {
                    if (file.isFile() && file.getName().endsWith(".ht2")) {
                        // Add index files to the input bindings
                        indexBasename = file.getName().substring(0, file.getName().indexOf('.'));
                        virtualIndexPath = buildVirtualPath(INPUT_FILE_PREFIX + file.getParent(), "index", bindings, readOnlyBindings);
                        break;
                    }
                }
                if (StringUtils.isEmpty(virtualIndexPath) && StringUtils.isEmpty(indexBasename)) {
                    throw new ToolExecutorException("No index files found in the specified path: " + indexPath);
                }
                params.getOptions().put(X_PARAM, virtualIndexPath + indexBasename);
                break;
            }

            case Hisat2Params.HISAT2_BUILD_TOOL: {
                // Update input
                List<String> input = new ArrayList<>();
                List<String> input0 = buildVirtualPaths(Arrays.asList(hisat2Params.getInput().get(0).split(",")), "input", bindings,
                        readOnlyBindings);
                input.add(StringUtils.join(input0, ","));

                String indexBasename = hisat2Params.getInput().get(1);
                String input1 = buildVirtualPath(OUTPUT_FILE_PREFIX + getOutDir().toAbsolutePath(), "output", bindings, readOnlyBindings);
                input.add(input1 + indexBasename);

                params.setInput(input);
                break;
            }

            default:
                throw new ToolExecutorException("Unsupported HISAT2 command: " + hisat2Params.getCommand());
        }

        // Handle options
        params.setOptions(updateParams(params.getOptions(), "data", bindings, readOnlyBindings));
    }

    public Hisat2WrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public Hisat2Params getHisat2Params() {
        return hisat2Params;
    }

    public Hisat2WrapperAnalysisExecutor setHisat2WrapperParams(Hisat2Params hisat2Params) {
        this.hisat2Params = hisat2Params;
        return this;
    }
}
