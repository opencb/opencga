package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class CancerTieringInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Cancer tiering interpretation analysis params";

    private String clinicalAnalysis;
    private List<String> discardedVariants;

    @JsonProperty(defaultValue = "false")
    private boolean secondary; // secondary interpretation (vs primary interpretation)

    public CancerTieringInterpretationAnalysisParams() {
    }

    public CancerTieringInterpretationAnalysisParams(String clinicalAnalysis, List<String> discardedVariants, boolean secondary) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.discardedVariants = discardedVariants;
        this.secondary = secondary;
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

    public boolean isSecondary() {
        return secondary;
    }

    public CancerTieringInterpretationAnalysisParams setSecondary(boolean secondary) {
        this.secondary = secondary;
        return this;
    }
}
