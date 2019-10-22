package org.opencb.opencga.analysis.variant.tdt;

import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.core.analysis.variant.TdtAnalysisExecutor;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.exception.AnalysisException;


@Analysis(id = TdtAnalysis.ID, data = Analysis.AnalysisData.VARIANT)
public class TdtAnalysis extends OpenCgaAnalysis {
    public static final String ID = "TDT";

    private String phenotype;

    @Override
    protected void check() throws AnalysisException {
        // checks
    }

    @Override
    public void exec() throws AnalysisException {
        TdtAnalysisExecutor tdtExecutor = getAnalysisExecutor(TdtAnalysisExecutor.class);

        arm.startStep("tdt");
        tdtExecutor.exec();
        arm.endStep(100);
    }

}
