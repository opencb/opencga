package org.opencb.opencga.analysis.clinical;

import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;

public class FamilyAnalysis extends OpenCgaAnalysis<Interpretation> {

    protected final static String SEPARATOR = "__";

    public FamilyAnalysis(String opencgaHome, String studyStr, String token) {
        super(opencgaHome, studyStr, token);
    }

    @Override
    public AnalysisResult<Interpretation> execute() throws Exception {
        // Never has to be called!!!
        return null;
    }
}
