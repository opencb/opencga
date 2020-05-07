package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class CancerTieringInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Cancer tiering interpretation analysis params";

    private String clinicalAnalysis;
    private List<String> discardedVariants;

    private boolean secondary; // secondary interpretation (vs primary interpretation)
    private boolean index;     // save interpretation in catalog DB

    public CancerTieringInterpretationAnalysisParams() {
        secondary = false;
        index = true;
    }

    public CancerTieringInterpretationAnalysisParams(String clinicalAnalysis, List<String> discardedVariants, boolean secondary,
                                                     boolean index) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.discardedVariants = discardedVariants;
        this.secondary = secondary;
        this.index = index;
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

    public boolean isIndex() {
        return index;
    }

    public CancerTieringInterpretationAnalysisParams setIndex(boolean index) {
        this.index = index;
        return this;
    }
}
