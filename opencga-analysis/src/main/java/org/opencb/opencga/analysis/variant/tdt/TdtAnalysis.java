package org.opencb.opencga.analysis.variant.tdt;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.tools.variant.TdtAnalysisExecutor;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.exception.ToolException;


@Tool(id = TdtAnalysis.ID, type = Tool.ToolType.VARIANT)
public class TdtAnalysis extends OpenCgaTool {
    public static final String ID = "tdt";

    private String phenotype;

    @Override
    protected void check() throws ToolException {
        // checks
    }

    @Override
    public void run() throws ToolException {
        step(() -> {
            getToolExecutor(TdtAnalysisExecutor.class)
                    .execute();
        });
    }

}
