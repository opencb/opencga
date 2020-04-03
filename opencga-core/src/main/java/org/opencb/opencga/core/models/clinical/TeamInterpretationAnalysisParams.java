package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class TeamInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "GEL tiering interpretation analysis params";

    private String clinicalAnalysis;
    private String panel;
    private String familySegregation;

    private int maxLowCoverage;
    private boolean includeLowCoverage;

    public TeamInterpretationAnalysisParams() {
    }

    public TeamInterpretationAnalysisParams(String clinicalAnalysis, String panel, String familySegregation, int maxLowCoverage,
                                            boolean includeLowCoverage) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.panel = panel;
        this.familySegregation = familySegregation;
        this.maxLowCoverage = maxLowCoverage;
        this.includeLowCoverage = includeLowCoverage;
    }

    public String getClinicalAnalysis() {
        return clinicalAnalysis;
    }

    public TeamInterpretationAnalysisParams setClinicalAnalysis(String clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;
        return this;
    }

    public String getPanel() {
        return panel;
    }

    public TeamInterpretationAnalysisParams setPanel(String panel) {
        this.panel = panel;
        return this;
    }

    public String getFamilySegregation() {
        return familySegregation;
    }

    public TeamInterpretationAnalysisParams setFamilySegregation(String familySegregation) {
        this.familySegregation = familySegregation;
        return this;
    }

    public int getMaxLowCoverage() {
        return maxLowCoverage;
    }

    public TeamInterpretationAnalysisParams setMaxLowCoverage(int maxLowCoverage) {
        this.maxLowCoverage = maxLowCoverage;
        return this;
    }

    public boolean isIncludeLowCoverage() {
        return includeLowCoverage;
    }

    public TeamInterpretationAnalysisParams setIncludeLowCoverage(boolean includeLowCoverage) {
        this.includeLowCoverage = includeLowCoverage;
        return this;
    }
}
