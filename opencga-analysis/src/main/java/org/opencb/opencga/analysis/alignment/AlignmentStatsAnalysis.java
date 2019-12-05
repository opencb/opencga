package org.opencb.opencga.analysis.alignment;

import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.core.annotations.Analysis;

@Analysis(id = AlignmentStatsAnalysis.ID, type = Analysis.AnalysisType.ALIGNMENT,
        description = "Compute alignment stats for a given alignment file, i.e. a BAM file.")
public class AlignmentStatsAnalysis extends OpenCgaAnalysis {

    public final static String ID = "alignment-stats";

    @Override
    protected void check() throws Exception {
        super.check();
    }

    @Override
    protected void run() throws Exception {
        step(() -> computeStats());
    }

    private void computeStats() {

    }
}
