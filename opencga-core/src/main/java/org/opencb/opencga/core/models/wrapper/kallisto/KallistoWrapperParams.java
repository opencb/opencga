package org.opencb.opencga.core.models.wrapper.kallisto;

import org.opencb.opencga.core.models.wrapper.hisat2.Hisat2Params;
import org.opencb.opencga.core.tools.ToolParams;

public class KallistoWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "Kallisto parameters.";

    private KallistoParams kallistoParams;
    private String outdir;

    public KallistoWrapperParams() {
        this(new KallistoParams(), "");
    }

    public KallistoWrapperParams(KallistoParams kallistoParams, String outdir) {
        this.kallistoParams = kallistoParams;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KallistoWrapperParams{");
        sb.append("kallistoParams=").append(kallistoParams);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public KallistoParams getKallistoParams() {
        return kallistoParams;
    }

    public KallistoWrapperParams setKallistoParams(KallistoParams kallistoParams) {
        this.kallistoParams = kallistoParams;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public KallistoWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
