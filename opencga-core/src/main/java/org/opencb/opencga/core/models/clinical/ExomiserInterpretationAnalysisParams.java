package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

import static org.opencb.opencga.core.api.FieldConstants.EXOMISER_DEFAULT_VERSION;


public class ExomiserInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Exomiser interpretation analysis params";

    @DataField(id = "clinicalAnalysis", description = FieldConstants.EXOMISER_CLINICAL_ANALYSIS_DESCRIPTION, required = true)
    private String clinicalAnalysis;

    @DataField(id = "exomiserVersion", description = FieldConstants.EXOMISER_VERSION_DESCRIPTION, defaultValue = EXOMISER_DEFAULT_VERSION)
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
