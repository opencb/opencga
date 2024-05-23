package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;



public class ExomiserInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Exomizer interpretation analysis params";

    private String clinicalAnalysis;
    private String exomiserVersion;

    public ExomiserInterpretationAnalysisParams() {
    }

    public ExomiserInterpretationAnalysisParams(String clinicalAnalysis, String exomiserVersion) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.exomiserVersion = exomiserVersion;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExomiserInterpretationAnalysisParams{");
        sb.append("clinicalAnalysis='").append(clinicalAnalysis).append('\'');
        sb.append(", exomiserVersion='").append(exomiserVersion).append('\'');
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

    public String getExomiserVersion() {
        return exomiserVersion;
    }

    public ExomiserInterpretationAnalysisParams setExomiserVersion(String exomiserVersion) {
        this.exomiserVersion = exomiserVersion;
        return this;
    }
}
