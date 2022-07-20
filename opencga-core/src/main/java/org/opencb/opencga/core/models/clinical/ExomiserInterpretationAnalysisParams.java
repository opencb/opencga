package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;



import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class ExomiserInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Exomizer interpretation analysis params";

    @DataField(description = ParamConstants.EXOMISER_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION)
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
