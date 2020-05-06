package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class TeamInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "TEAM interpretation analysis params";

    private String clinicalAnalysis;
    private List<String> panels;
    private String familySegregation;

    private boolean secondary; // secondary interpretation (vs primary interpretation)
    private boolean index;     // save interpretation in catalog DB

    public TeamInterpretationAnalysisParams() {
    }

    public TeamInterpretationAnalysisParams(String clinicalAnalysis, List<String> panels, String familySegregation,
                                            boolean secondary, boolean index) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.panels = panels;
        this.familySegregation = familySegregation;
        this.secondary = secondary;
        this.index = index;
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

    public boolean isIndex() {
        return index;
    }

    public TeamInterpretationAnalysisParams setIndex(boolean index) {
        this.index = index;
        return this;
    }
}
