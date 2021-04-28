package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class FastqcWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "FastQC parameters";

    private String inputFile;
    private String contaminantsFile;
    private String adaptersFile;
    private String limitsFile;
    private String outdir;
    private Map<String, String> fastqcParams;

    public FastqcWrapperParams() {
    }

    public FastqcWrapperParams(String inputFile, String contaminantsFile, String adaptersFile, String limitsFile, String outdir,
                               Map<String, String> fastqcParams) {
        this.inputFile = inputFile;
        this.contaminantsFile = contaminantsFile;
        this.adaptersFile = adaptersFile;
        this.limitsFile = limitsFile;
        this.outdir = outdir;
        this.fastqcParams = fastqcParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FastqcWrapperParams{");
        sb.append("inputFile='").append(inputFile).append('\'');
        sb.append(", contaminantsFile='").append(contaminantsFile).append('\'');
        sb.append(", adaptersFile='").append(adaptersFile).append('\'');
        sb.append(", limitsFile='").append(limitsFile).append('\'');
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

    public String getContaminantsFile() {
        return contaminantsFile;
    }

    public FastqcWrapperParams setContaminantsFile(String contaminantsFile) {
        this.contaminantsFile = contaminantsFile;
        return this;
    }

    public String getAdaptersFile() {
        return adaptersFile;
    }

    public FastqcWrapperParams setAdaptersFile(String adaptersFile) {
        this.adaptersFile = adaptersFile;
        return this;
    }

    public String getLimitsFile() {
        return limitsFile;
    }

    public FastqcWrapperParams setLimitsFile(String limitsFile) {
        this.limitsFile = limitsFile;
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
