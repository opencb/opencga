package org.opencb.opencga.core.tools;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.annotations.ToolExecutor;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.exception.ToolExecutorException;
import org.opencb.opencga.core.tools.result.ExecutorResultManager;

import java.nio.file.Path;

public abstract class OpenCgaToolExecutor {

    public static final String EXECUTOR_ID = "executorId";
    protected ObjectMap executorParams;
    protected Path outDir;
    private ExecutorResultManager arm;

    protected OpenCgaToolExecutor() {
    }

    public final String getAnalysisId() {
        return this.getClass().getAnnotation(ToolExecutor.class).tool();
    }

    public final String getId() {
        return this.getClass().getAnnotation(ToolExecutor.class).id();
    }

    public final ToolExecutor.Framework getFramework() {
        return this.getClass().getAnnotation(ToolExecutor.class).framework();
    }

    public final ToolExecutor.Source getSource() {
        return this.getClass().getAnnotation(ToolExecutor.class).source();
    }

    public final void setUp(ExecutorResultManager arm, ObjectMap executorParams, Path outDir) {
        this.arm = arm;
        this.executorParams = executorParams;
        this.outDir = outDir;
    }

    public final void execute() throws ToolException {
        run();
    }

    protected abstract void run() throws ToolException;

    public final ObjectMap getExecutorParams() {
        return executorParams;
    }

    public final Path getOutDir() {
        return outDir;
    }

    protected final String getToken() throws ToolExecutorException {
        return getExecutorParams().getString("token");
    }

    protected final void addWarning(String warning) throws ToolException {
        arm.addWarning(warning);
    }

    protected final void addAttribute(String key, Object value) throws ToolException {
        arm.addStepAttribute(key, value);
    }

}
