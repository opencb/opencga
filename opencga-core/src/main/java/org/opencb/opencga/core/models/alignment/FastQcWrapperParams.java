package org.opencb.opencga.core.models.alignment;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class FastQcWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "FastQC parameters";

    private String file;        // Input file
    private String outdir;
    private Map<String, String> fastqcParams;

    public FastQcWrapperParams() {
    }

    public FastQcWrapperParams(String file, String outdir, Map<String, String> fastqcParams) {
        this.file = file;
        this.outdir = outdir;
        this.fastqcParams = fastqcParams;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FastQcWrapperParams{");
        sb.append("file='").append(file).append('\'');
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append(", fastqcParams=").append(fastqcParams);
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public FastQcWrapperParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public FastQcWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }

    public Map<String, String> getFastqcParams() {
        return fastqcParams;
    }

    public FastQcWrapperParams setFastqcParams(Map<String, String> fastqcParams) {
        this.fastqcParams = fastqcParams;
        return this;
    }
}
