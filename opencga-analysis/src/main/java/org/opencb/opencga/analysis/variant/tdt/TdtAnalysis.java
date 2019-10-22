package org.opencb.opencga.analysis.variant.tdt;

import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.core.analysis.variant.TdtAnalysisExecutor;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.exception.AnalysisException;


@Analysis(id = TdtAnalysis.ID, type = Analysis.AnalysisType.VARIANT)
public class TdtAnalysis extends OpenCgaAnalysis {
    public static final String ID = "tdt";

    private String phenotype;

    @Override
    protected void check() throws AnalysisException {
        // checks
    }

    @Override
    public void run() throws AnalysisException {
        step(() -> {
            getAnalysisExecutor(TdtAnalysisExecutor.class)
                    .execute();
        });
    }

}
