package org.opencb.opencga.analysis.alignment;

import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.core.annotations.Analysis;

@Analysis(id = AlignmentStatsAnalysis.ID, type = Analysis.AnalysisType.ALIGNMENT,
        description = "Index alignment.")
public class AlignmentIndexOperation extends OpenCgaAnalysis {

    @Override
    protected void run() throws Exception {

    }
}
