package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class TieringInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Tiering interpretation analysis params";

    private String clinicalAnalysis;
    private List<String> panels;
    private ClinicalProperty.Penetrance penetrance;

    private boolean secondary; // secondary interpretation (vs primary interpretation)
    private boolean index;     // save interpretation in catalog DB

    public TieringInterpretationAnalysisParams() {
    }

    public TieringInterpretationAnalysisParams(String clinicalAnalysis, List<String> panels, ClinicalProperty.Penetrance penetrance,
                                               boolean secondary, boolean index) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.panels = panels;
        this.penetrance = penetrance;
        this.secondary = secondary;
        this.index = index;
    }

    public String getClinicalAnalysis() {
        return clinicalAnalysis;
    }

    public TieringInterpretationAnalysisParams setClinicalAnalysis(String clinicalAnalysis) {
        this.clinicalAnalysis = clinicalAnalysis;
        return this;
    }

    public List<String> getPanels() {
        return panels;
    }

    public TieringInterpretationAnalysisParams setPanels(List<String> panels) {
        this.panels = panels;
        return this;
    }

    public ClinicalProperty.Penetrance getPenetrance() {
        return penetrance;
    }

    public TieringInterpretationAnalysisParams setPenetrance(ClinicalProperty.Penetrance penetrance) {
        this.penetrance = penetrance;
        return this;
    }

    public boolean isSecondary() {
        return secondary;
    }

    public TieringInterpretationAnalysisParams setSecondary(boolean secondary) {
        this.secondary = secondary;
        return this;
    }

    public boolean isIndex() {
        return index;
    }

    public TieringInterpretationAnalysisParams setIndex(boolean index) {
        this.index = index;
        return this;
    }
}
