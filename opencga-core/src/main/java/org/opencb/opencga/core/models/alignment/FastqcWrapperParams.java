package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FastqcWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "FastQC parameters";

    @DataField(description = ParamConstants.FASTQC_WRAPPER_PARAMS_INPUT_FILE_DESCRIPTION)
    private String inputFile;
    @DataField(description = ParamConstants.FASTQC_WRAPPER_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;
    @DataField(description = ParamConstants.FASTQC_WRAPPER_PARAMS_FASTQC_PARAMS_DESCRIPTION)
    private Map<String, String> fastqcParams;

    public FastqcWrapperParams() {
    }

    public FastqcWrapperParams(String inputFile, String outdir, Map<String, String> fastqcParams) {
        this.inputFile = inputFile;
        this.outdir = outdir;
        this.fastqcParams = fastqcParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FastqcWrapperParams{");
        sb.append("inputFile='").append(inputFile).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", fastqcParams=").append(fastqcParams);
        sb.append('}');
        return sb.toString();
    }

    public String getInputFile() {
        return inputFile;
    }

    public FastqcWrapperParams setInputFile(String inputFile) {
        this.inputFile = inputFile;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public FastqcWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getFastqcParams() {
        return fastqcParams;
    }

    public FastqcWrapperParams setFastqcParams(Map<String, String> fastqcParams) {
        this.fastqcParams = fastqcParams;
        return this;
    }
}
