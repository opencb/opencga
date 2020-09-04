package org.opencb.opencga.core.models.clinical;

import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class TieringInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Tiering interpretation analysis params";

    private String clinicalAnalysis;
    private List<String> panels;
    private ClinicalProperty.Penetrance penetrance;

    private boolean primary; // primary interpretation (vs secondary interpretation)

    public TieringInterpretationAnalysisParams() {
    }

    public TieringInterpretationAnalysisParams(String clinicalAnalysis, List<String> panels, ClinicalProperty.Penetrance penetrance,
                                               boolean primary) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.panels = panels;
        this.penetrance = penetrance;
        this.primary = primary;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TieringInterpretationAnalysisParams{");
        sb.append("clinicalAnalysis='").append(clinicalAnalysis).append('\'');
        sb.append(", panels=").append(panels);
        sb.append(", penetrance=").append(penetrance);
        sb.append(", primary=").append(primary);
        sb.append('}');
        return sb.toString();
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

    public boolean isPrimary() {
        return primary;
    }

    public TieringInterpretationAnalysisParams setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }
}
