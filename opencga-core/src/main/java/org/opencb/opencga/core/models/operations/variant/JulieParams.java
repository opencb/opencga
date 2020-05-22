package org.opencb.opencga.core.models.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class JulieParams extends ToolParams {

    public static final String DESCRIPTION = "Julie tool params. Specify list of cohorts from multiple studies with {study}:{cohort}";

    /**
     * List of cohorts from multiple studies with {study}:{cohort}
     */
    private List<String> cohorts;

    private String region;

    /**
     * Overwrite all population frequencies.
     */
    private boolean overwrite;


    public JulieParams() {
    }

    public JulieParams(List<String> cohorts, String region, boolean overwrite) {
        this.cohorts = cohorts;
        this.overwrite = overwrite;
    }

    public List<String> getCohorts() {
        return cohorts;
    }

    public JulieParams setCohorts(List<String> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public boolean getOverwrite() {
        return overwrite;
    }

    public JulieParams setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public String getRegion() {
        return region;
    }

    public JulieParams setRegion(String region) {
        this.region = region;
        return this;
    }
}
