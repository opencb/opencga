package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class ExomiserWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Exomiser parameters";

    private String sample;
    private String exomiserVersion;
    private String outdir;

    public ExomiserWrapperParams() {
    }

    public ExomiserWrapperParams(String sample, String exomiserVersion, String outdir) {
        this.sample = sample;
        this.exomiserVersion = exomiserVersion;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExomiserWrapperParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append("exomiserVersion='").append(exomiserVersion).append('\'');
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

    public String getExomiserVersion() {
        return exomiserVersion;
    }

    public ExomiserWrapperParams setExomiserVersion(String exomiserVersion) {
        this.exomiserVersion = exomiserVersion;
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
