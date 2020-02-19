package org.opencb.opencga.core.tools.variant;

import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class RelatednessAnalysisExecutor extends OpenCgaToolExecutor {

    private String study;
    private List<String> samples;

    public RelatednessAnalysisExecutor() {
    }

    public String getStudy() {
        return study;
    }

    public RelatednessAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public RelatednessAnalysisExecutor setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }
}
