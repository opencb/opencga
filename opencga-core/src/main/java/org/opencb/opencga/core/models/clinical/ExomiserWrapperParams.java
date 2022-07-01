package org.opencb.opencga.core.models.clinical;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.Map;

public class ExomiserWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Exomiser parameters";

    private String sample;
    private String outdir;

    public ExomiserWrapperParams() {
    }

    public ExomiserWrapperParams(String sample, String outdir) {
        this.sample = sample;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExomiserWrapperParams{");
        sb.append("sample='").append(sample).append('\'');
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

    public String getOutdir() {
        return outdir;
    }

    public ExomiserWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
