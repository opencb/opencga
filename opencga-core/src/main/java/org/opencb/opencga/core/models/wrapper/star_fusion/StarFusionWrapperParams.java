package org.opencb.opencga.core.models.wrapper.star_fusion;

import org.opencb.opencga.core.models.wrapper.star.StarParams;
import org.opencb.opencga.core.tools.ToolParams;

public class StarFusionWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "STAR parameters.";

    private StarFusionParams starFusionParams;
    private String outdir;

    public StarFusionWrapperParams() {
        this(new StarFusionParams(), "");
    }

    public StarFusionWrapperParams(StarFusionParams starFusionParams, String outdir) {
        this.starFusionParams = starFusionParams;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StarFusionWrapperParams{");
        sb.append("starFusionParams=").append(starFusionParams);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public StarFusionParams getStarFusionParams() {
        return starFusionParams;
    }

    public StarFusionWrapperParams setStarFusionParams(StarFusionParams starFusionParams) {
        this.starFusionParams = starFusionParams;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public StarFusionWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
