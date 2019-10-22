package org.opencb.opencga.core.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.annotations.AnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.core.analysis.result.AnalysisResultManager;

import java.nio.file.Path;

public abstract class OpenCgaAnalysisExecutor {

    public static final String EXECUTOR_ID = "executorId";
    protected ObjectMap executorParams;
    protected Path outDir;
    private AnalysisResultManager arm;

    protected OpenCgaAnalysisExecutor() {
    }

    public final String getAnalysisId() {
        return this.getClass().getAnnotation(AnalysisExecutor.class).analysis();
    }

    public final String getId() {
        return this.getClass().getAnnotation(AnalysisExecutor.class).id();
    }

    public final AnalysisExecutor.Framework getFramework() {
        return this.getClass().getAnnotation(AnalysisExecutor.class).framework();
    }

    public final AnalysisExecutor.Source getSource() {
        return this.getClass().getAnnotation(AnalysisExecutor.class).source();
    }

    public final void setUp(AnalysisResultManager arm, ObjectMap executorParams, Path outDir) {
        this.arm = arm;
        this.executorParams = executorParams;
        this.outDir = outDir;
    }

    public final void execute() throws AnalysisException {
        run();
    }

    protected abstract void run() throws AnalysisException;

    public final ObjectMap getExecutorParams() {
        return executorParams;
    }

    public final Path getOutDir() {
        return outDir;
    }

    protected final String getSessionId() throws AnalysisExecutorException {
        return getExecutorParams().getString("sessionId");
    }

    protected final void addWarning(String warning) throws AnalysisException {
        arm.addWarning(warning);
    }

    protected final void addAttribute(String key, Object value) throws AnalysisException {
        arm.addStepAttribute(key, value);
    }

}
