package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class TeamInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "GEL tiering interpretation analysis params";

    private String clinicalAnalysis;
    private List<String> panels;
    private String familySegregation;

    private int maxLowCoverage;
    private boolean includeLowCoverage;

    public TeamInterpretationAnalysisParams() {
    }

    public TeamInterpretationAnalysisParams(String clinicalAnalysis, List<String> panels, String familySegregation, int maxLowCoverage,
                                            boolean includeLowCoverage) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.panels = panels;
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
