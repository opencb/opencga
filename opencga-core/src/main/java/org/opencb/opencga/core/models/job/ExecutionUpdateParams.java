package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.models.file.File;

import java.util.Map;

public class ExecutionUpdateParams {

    protected Pipeline pipeline;
    protected Map<String, Object> params;
    protected Boolean visited;
    protected File outDir;

    public ExecutionUpdateParams() {
    }

    public ExecutionUpdateParams(Pipeline pipeline, Map<String, Object> params, Boolean visited, File outDir) {
        this.pipeline = pipeline;
        this.params = params;
        this.visited = visited;
        this.outDir = outDir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionUpdateParams{");
        sb.append("pipeline=").append(pipeline);
        sb.append(", params=").append(params);
        sb.append(", visited=").append(visited);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public ExecutionUpdateParams setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public ExecutionUpdateParams setParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public Boolean getVisited() {
        return visited;
    }

    public ExecutionUpdateParams setVisited(Boolean visited) {
        this.visited = visited;
        return this;
    }

    public File getOutDir() {
        return outDir;
    }

    public ExecutionUpdateParams setOutDir(File outDir) {
        this.outDir = outDir;
        return this;
    }

}

