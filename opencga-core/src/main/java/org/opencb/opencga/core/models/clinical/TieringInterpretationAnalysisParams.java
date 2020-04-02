package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class TieringInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "GEL tiering interpretation analysis params";

    private String clinicalAnalysisId;
    private List<String> diseasePanelIds;
    private ClinicalProperty.Penetrance penetrance;
    private int maxLowCoverage;
    private boolean includeLowCoverage;

    public TieringInterpretationAnalysisParams() {
    }

    public TieringInterpretationAnalysisParams(String clinicalAnalysisId, List<String> diseasePanelIds,
                                               ClinicalProperty.Penetrance penetrance, int maxLowCoverage, boolean includeLowCoverage) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.diseasePanelIds = diseasePanelIds;
        this.penetrance = penetrance;
        this.maxLowCoverage = maxLowCoverage;
        this.includeLowCoverage = includeLowCoverage;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public TieringInterpretationAnalysisParams setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public List<String> getDiseasePanelIds() {
        return diseasePanelIds;
    }

    public TieringInterpretationAnalysisParams setDiseasePanelIds(List<String> diseasePanelIds) {
        this.diseasePanelIds = diseasePanelIds;
        return this;
    }

    public ClinicalProperty.Penetrance getPenetrance() {
        return penetrance;
    }

    public TieringInterpretationAnalysisParams setPenetrance(ClinicalProperty.Penetrance penetrance) {
        this.penetrance = penetrance;
        return this;
    }

    public int getMaxLowCoverage() {
        return maxLowCoverage;
    }

    public TieringInterpretationAnalysisParams setMaxLowCoverage(int maxLowCoverage) {
        this.maxLowCoverage = maxLowCoverage;
        return this;
    }

    public boolean isIncludeLowCoverage() {
        return includeLowCoverage;
    }

    public TieringInterpretationAnalysisParams setIncludeLowCoverage(boolean includeLowCoverage) {
        this.includeLowCoverage = includeLowCoverage;
        return this;
    }
}
