package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.models.file.File;

public class ExecutionUpdateParams {

    private Pipeline pipeline;
    private Boolean visited;
    private File outDir;
    private ExecutionInternal internal;

    public ExecutionUpdateParams() {
    }

    public ExecutionUpdateParams(Pipeline pipeline, Boolean visited, File outDir, ExecutionInternal internal) {
        this.pipeline = pipeline;
        this.visited = visited;
        this.outDir = outDir;
        this.internal = internal;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionUpdateParams{");
        sb.append("pipeline=").append(pipeline);
        sb.append(", visited=").append(visited);
        sb.append(", outDir=").append(outDir);
        sb.append(", internal=").append(internal);
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

    public ExecutionInternal getInternal() {
        return internal;
    }

    public ExecutionUpdateParams setInternal(ExecutionInternal internal) {
        this.internal = internal;
        return this;
    }
}

