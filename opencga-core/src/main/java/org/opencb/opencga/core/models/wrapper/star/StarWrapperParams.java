package org.opencb.opencga.core.models.wrapper;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.ToolParams;

public class StarWrapperParams extends ToolParams {
    public static final String DESCRIPTION = "STAR parameters.";

    private ObjectMap starParams;
    private String outdir;

    public StarWrapperParams() {
        this.starParams = new ObjectMap();
    }

    public StarWrapperParams(ObjectMap starParams, String outdir) {
        this.starParams = starParams;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StarWrapperParams{");
        sb.append(", starParams=").append(starParams);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public ObjectMap getStarParams() {
        return starParams;
    }

    public StarWrapperParams setStarParams(ObjectMap starParams) {
        this.starParams = starParams;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public StarWrapperParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
