package org.opencb.opencga.core.models.clinical.interpretation;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.clinical.tiering.TieringParams;
import org.opencb.opencga.core.tools.ToolParams;

public class ClinicalInterpretationAnalysisParams extends ToolParams {
    @DataField(id = "clinicalAnalysisId", description = FieldConstants.CLINICAL_INTERPRETATION_CLINICAL_ANALYSIS_ID_DESCRIPTION, required = true)
    private String clinicalAnalysisId;

    @DataField(id = "primary", description = FieldConstants.CLINICAL_INTERPRETATION_PRIMARY_DESCRIPTION)
    private boolean primary;

    @DataField(id = "tieringParams", description = FieldConstants.CLINICAL_INTERPRETATION_PARAMS_DESCRIPTION)
    private ClinicalInterpretationParams clinicalInterpretationParams;

    @DataField(id = "configFile", description = FieldConstants.CLINICAL_INTERPRETATION_CONFIG_FILE_DESCRIPTION)
    private String configFile;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public ClinicalInterpretationAnalysisParams() {
    }

    public ClinicalInterpretationAnalysisParams(String clinicalAnalysisId, boolean primary,
                                                ClinicalInterpretationParams clinicalInterpretationParams, String configFile, String outdir) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.primary = primary;
        this.clinicalInterpretationParams = clinicalInterpretationParams;
        this.configFile = configFile;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalInterpretationAnalysisParams{");
        sb.append("clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
        sb.append(", primary=").append(primary);
        sb.append(", clinicalInterpretationParams=").append(clinicalInterpretationParams);
        sb.append(", configFile='").append(configFile).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public ClinicalInterpretationAnalysisParams setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public boolean isPrimary() {
        return primary;
    }

    public ClinicalInterpretationAnalysisParams setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }

    public ClinicalInterpretationParams getClinicalInterpretationParams() {
        return clinicalInterpretationParams;
    }

    public ClinicalInterpretationAnalysisParams setClinicalInterpretationParams(ClinicalInterpretationParams clinicalInterpretationParams) {
        this.clinicalInterpretationParams = clinicalInterpretationParams;
        return this;
    }

    public String getConfigFile() {
        return configFile;
    }

    public ClinicalInterpretationAnalysisParams setConfigFile(String configFile) {
        this.configFile = configFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public ClinicalInterpretationAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
