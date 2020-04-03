package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class CancerTieringInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Cancer tiering interpretation analysis params";

    private String clinicalAnalysis;
    private String discardedVariant;;

    public CancerTieringInterpretationAnalysisParams() {
    }

    public CancerTieringInterpretationAnalysisParams(String clinicalAnalysis, String discardedVariant) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.discardedVariant = discardedVariant;
    }
}
