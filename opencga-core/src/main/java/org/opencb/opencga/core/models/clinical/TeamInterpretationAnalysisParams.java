package org.opencb.opencga.core.models.clinical;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class TeamInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "TEAM interpretation analysis params";

    @DataField(description = ParamConstants.TEAM_INTERPRETATION_ANALYSIS_PARAMS_CLINICAL_ANALYSIS_DESCRIPTION)
    private String clinicalAnalysis;
    @DataField(description = ParamConstants.TEAM_INTERPRETATION_ANALYSIS_PARAMS_PANELS_DESCRIPTION)
    private List<String> panels;
    @DataField(description = ParamConstants.TEAM_INTERPRETATION_ANALYSIS_PARAMS_FAMILY_SEGREGATION_DESCRIPTION)
    private String familySegregation;

    @JsonProperty(defaultValue = "false")
    @DataField(description = ParamConstants.TEAM_INTERPRETATION_ANALYSIS_PARAMS_PRIMARY_DESCRIPTION)
    private boolean primary; // primary interpretation (vs secondary interpretation)

    public TeamInterpretationAnalysisParams() {
    }

    public TeamInterpretationAnalysisParams(String clinicalAnalysis, List<String> panels, String familySegregation, boolean primary) {
        this.clinicalAnalysis = clinicalAnalysis;
        this.panels = panels;
        this.familySegregation = familySegregation;
        this.primary = primary;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TeamInterpretationAnalysisParams{");
        sb.append("clinicalAnalysis='").append(clinicalAnalysis).append('\'');
        sb.append(", panels=").append(panels);
        sb.append(", familySegregation='").append(familySegregation).append('\'');
        sb.append(", primary=").append(primary);
        sb.append('}');
        return sb.toString();
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

    public boolean isPrimary() {
        return primary;
    }

    public TeamInterpretationAnalysisParams setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }
}
