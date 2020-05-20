package org.opencb.opencga.analysis.variant.julie;

import org.opencb.opencga.core.tools.ToolParams;

import java.util.List;

public class JulieParams extends ToolParams {

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
