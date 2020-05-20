package org.opencb.opencga.core.models.operations.variant;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class JulieParams extends ToolParams {

    public static final String DESCRIPTION = "Julie tool params. Specify list of cohorts from multiple studies with {study}:{cohort}";
    private List<String> cohorts;

    public JulieParams() {
    }

    public JulieParams(List<String> cohorts) {
        this.cohorts = cohorts;
    }

    public List<String> getCohorts() {
        return cohorts;
    }

    public JulieParams setCohorts(List<String> cohorts) {
        this.cohorts = cohorts;
        return this;
    }
}
