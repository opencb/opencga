package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.util.List;
import java.util.Map;

public abstract class JulieToolExecutor extends OpenCgaToolExecutor {

    private Map<String, List<String>> cohorts;

    public Map<String, List<String>> getCohorts() {
        return cohorts;
    }

    public JulieToolExecutor setCohorts(Map<String, List<String>> cohorts) {
        this.cohorts = cohorts;
        return this;
    }
}
