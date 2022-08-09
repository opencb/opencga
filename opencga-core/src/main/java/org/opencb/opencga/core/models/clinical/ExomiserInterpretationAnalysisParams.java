package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;



public class ExomiserInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Exomizer interpretation analysis params";

    private String clinicalAnalysis;

    public ExomiserInterpretationAnalysisParams() {
    }

    public ExomiserInterpretationAnalysisParams(String clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TieringInterpretationAnalysisParams{");
        sb.append("clinicalAnalysis='").append(clinicalAnalysis).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getClinicalAnalysis() {
        return clinicalAnalysis;
    }

    public ExomiserInterpretationAnalysisParams setClinicalAnalysis(String clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;
        return this;
    }


}
