package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class CancerTieringInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Cancer tiering interpretation analysis params";

    private String clinicalAnalysis;
    private List<String> discardedVariants;

    @JsonProperty(defaultValue = "false")
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
