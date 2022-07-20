package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class CancerTieringInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Cancer tiering interpretation analysis params";

    @DataField(description = ParamConstants.CANCER_TIERING_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION)
    private String clinicalAnalysis;
    @DataField(description = ParamConstants.CANCER_TIERING_INTERPRETATION_ANALYSIS_PARAMS_DISCARDED_VARIANTS_DESCRIPTION)
    private List<String> discardedVariants;

    @JsonProperty(defaultValue = "false")
    @DataField(description = ParamConstants.CANCER_TIERING_INTERPRETATION_ANALYSIS_PARAMS_PRIMARY_DESCRIPTION)
    private boolean primary; // primary interpretation (vs secondary interpretation)

    public CancerTieringInterpretationAnalysisParams() {
    }

    public CancerTieringInterpretationAnalysisParams(String clinicalAnalysis, List<String> discardedVariants, boolean primary) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.discardedVariants = discardedVariants;
        this.primary = primary;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CancerTieringInterpretationAnalysisParams{");
        sb.append("clinicalAnalysis='").append(clinicalAnalysis).append('\'');
        sb.append(", discardedVariants=").append(discardedVariants);
        sb.append(", primary=").append(primary);
        sb.append('}');
        return sb.toString();
    }

    public String getClinicalAnalysis() {
        return clinicalAnalysis;
    }

    public CancerTieringInterpretationAnalysisParams setClinicalAnalysis(String clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;
        return this;
    }

    public List<String> getDiscardedVariants() {
        return discardedVariants;
    }

    public CancerTieringInterpretationAnalysisParams setDiscardedVariants(List<String> discardedVariants) {
        this.discardedVariants = discardedVariants;
        return this;
    }

    public boolean isPrimary() {
        return primary;
    }

    public CancerTieringInterpretationAnalysisParams setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }
}
