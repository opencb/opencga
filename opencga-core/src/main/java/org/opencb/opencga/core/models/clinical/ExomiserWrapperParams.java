package org.opencb.opencga.core.models.clinical;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class ExomiserWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Exomiser parameters";

    @DataField(id = "sample", description = FieldConstants.SAMPLE_ID_DESCRIPTION)
    private String sample;

    @DataField(id = "clinicalAnalysisType", description = FieldConstants.EXOMISER_CLINICAL_ANALYSIS_TYPE_DESCRIPTION,
            defaultValue = "SINGLE")
    private String clinicalAnalysisType;

    @DataField(id = "outdir", description = FieldConstants.JOB_OUT_DIR_DESCRIPTION)
    private String outdir;

    public ExomiserWrapperParams() {
    }

    public ExomiserWrapperParams(String sample, String clinicalAnalysisType, String outdir) {
        this.sample = sample;
        this.clinicalAnalysisType = clinicalAnalysisType;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExomiserWrapperParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append(", clinicalAnalysisType=").append(clinicalAnalysisType);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public ExomiserWrapperParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getClinicalAnalysisType() {
        return clinicalAnalysisType;
    }

    public ExomiserWrapperParams setClinicalAnalysisType(String clinicalAnalysisType) {
        this.clinicalAnalysisType = clinicalAnalysisType;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public ExomiserWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
