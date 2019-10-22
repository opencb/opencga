package org.opencb.opencga.core.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.annotations.AnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.core.analysis.result.AnalysisResultManager;

import java.nio.file.Path;

/**
 * Helper interface to be used by opencga local analysis executors.
 */
public abstract class OpenCgaAnalysisExecutor {


    protected ObjectMap executorParams;
    protected Path outDir;
    protected AnalysisResultManager arm;

    protected OpenCgaAnalysisExecutor() {
    }

    protected OpenCgaAnalysisExecutor(ObjectMap executorParams, Path outDir) {
        setUp(executorParams, outDir);
    }

    public final void init(AnalysisResultManager arm) {
        this.arm = arm;
    }

    public final String getAnalysisId() {
        return this.getClass().getAnnotation(AnalysisExecutor.class).analysis();
    }

    public final String getId() {
        return this.getClass().getAnnotation(AnalysisExecutor.class).id();
    }

    public final void setUp(ObjectMap executorParams, Path outDir) {
        this.executorParams = executorParams;
        this.outDir = outDir;
    }

    public abstract void exec() throws AnalysisException;

    public final ObjectMap getExecutorParams() {
        return executorParams;
    }

    public final Path getOutDir() {
        return outDir;
    }

    protected final String getSessionId() throws AnalysisExecutorException {
        return getExecutorParams().getString("sessionId");
    }

}
