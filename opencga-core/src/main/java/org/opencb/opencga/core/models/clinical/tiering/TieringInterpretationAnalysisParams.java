package org.opencb.opencga.core.models.clinical.tiering;

import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class TieringInterpretationAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = FieldConstants.TIERING_INTERPRETATION_PARAMS_DESCRIPTION;

    @DataField(id = "clinicalAnalysisId", description = FieldConstants.TIERING_CLINICAL_ANALYSIS_ID_DESCRIPTION, required = true)
    private String clinicalAnalysisId;

    @DataField(id = "primary", description = FieldConstants.TIERING_PRIMARY_DESCRIPTION)
    private boolean primary;

    @DataField(id = "tieringParams", description = FieldConstants.TIERING_PARAMS_DESCRIPTION)
    private TieringParams tieringParams;

    @DataField(id = "configFile", description = FieldConstants.TIERING_CONFIG_FILE_DESCRIPTION)
    private String configFile;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public TieringInterpretationAnalysisParams() {
    }

    public TieringInterpretationAnalysisParams(String clinicalAnalysisId, boolean primary, TieringParams tieringParams, String configFile,
                                               String outdir) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.primary = primary;
        this.tieringParams = tieringParams;
        this.configFile = configFile;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TieringInterpretationAnalysisParams{");
        sb.append("clinicalAnalysisId='").append(clinicalAnalysisId).append('\'');
        sb.append(", primary=").append(primary);
        sb.append(", tieringParams=").append(tieringParams);
        sb.append(", configFile='").append(configFile).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public TieringInterpretationAnalysisParams setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public boolean isPrimary() {
        return primary;
    }

    public TieringInterpretationAnalysisParams setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }

    public TieringParams getTieringParams() {
        return tieringParams;
    }

    public TieringInterpretationAnalysisParams setTieringParams(TieringParams tieringParams) {
        this.tieringParams = tieringParams;
        return this;
    }

    public String getConfigFile() {
        return configFile;
    }

    public TieringInterpretationAnalysisParams setConfigFile(String configFile) {
        this.configFile = configFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public TieringInterpretationAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
