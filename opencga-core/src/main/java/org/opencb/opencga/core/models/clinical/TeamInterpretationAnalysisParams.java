package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class TeamInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "TEAM interpretation analysis params";

    private String clinicalAnalysis;
    private List<String> panels;
    private String familySegregation;

    @JsonProperty(defaultValue = "false")
    private boolean secondary; // secondary interpretation (vs primary interpretation)

    public TeamInterpretationAnalysisParams() {
    }

    public TeamInterpretationAnalysisParams(String clinicalAnalysis, List<String> panels, String familySegregation,
                                            boolean secondary) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.panels = panels;
        this.familySegregation = familySegregation;
        this.secondary = secondary;
    }

    public String getClinicalAnalysis() {
        return clinicalAnalysis;
    }

    public TeamInterpretationAnalysisParams setClinicalAnalysis(String clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;
        return this;
    }

    public List<String> getPanels() {
        return panels;
    }

    public TeamInterpretationAnalysisParams setPanels(List<String> panels) {
        this.panels = panels;
        return this;
    }

    public String getFamilySegregation() {
        return familySegregation;
    }

    public TeamInterpretationAnalysisParams setFamilySegregation(String familySegregation) {
        this.familySegregation = familySegregation;
        return this;
    }

    public boolean isSecondary() {
        return secondary;
    }

    public TeamInterpretationAnalysisParams setSecondary(boolean secondary) {
        this.secondary = secondary;
        return this;
    }
}
