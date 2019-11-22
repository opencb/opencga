package org.opencb.opencga.master.tasks;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.master.exceptions.TaskException;
import org.opencb.opencga.master.tasks.result.Result;
import org.opencb.opencga.master.tasks.result.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

public abstract class OpenCgaTask {

    protected CatalogManager catalogManager;
    protected Configuration configuration;

    protected String opencgaToken;

    protected ObjectMap params;
    protected ObjectMap executorParams;
    protected Logger logger;
    private Path outDir;
    private String taskId;
    private final Logger privateLogger = LoggerFactory.getLogger(OpenCgaTask.class);

    private TaskManager arm;

    public OpenCgaTask() {
    }

    public final OpenCgaTask setUp(CatalogManager catalogManager, ObjectMap params, String taskId, String token) {
        this.catalogManager = catalogManager;
        this.configuration = catalogManager.getConfiguration();
        this.opencgaToken = token;
        this.params = params == null ? new ObjectMap() : new ObjectMap(params);
        this.executorParams = new ObjectMap();
        this.outDir = outDir;
        this.params.put("outDir", outDir.toAbsolutePath().toString());

        logger = LoggerFactory.getLogger(this.getClass().toString());

        return this;
    }

    public final OpenCgaTask setUp(Configuration configuration, ObjectMap params, String taskId, String token) throws TaskException {
        this.configuration = configuration;
        this.opencgaToken = token;
        this.params = params == null ? new ObjectMap() : new ObjectMap(params);
        this.executorParams = new ObjectMap();
        this.outDir = outDir;
        this.params.put("outDir", outDir.toAbsolutePath().toString());

        try {
            this.catalogManager = new CatalogManager(configuration);
        } catch (CatalogException e) {
            throw new TaskException(e);
        }

        logger = LoggerFactory.getLogger(this.getClass().toString());

        return this;
    }

    /**
     * Execute the task. The task should have been properly setUp before being executed.
     *
     * @return Result
     * @throws TaskException on error
     */
    public final Result start() throws TaskException {
        arm = new TaskManager(getId(), outDir);
        arm.init();
        Thread hook = new Thread(() -> {
            Exception exception = null;
            try {
                onShutdown();
            } catch (Exception e) {
                exception = e;
            }
            if (!arm.isClosed()) {
                privateLogger.error("Unexpected system shutdown!");
                try {
                    if (exception == null) {
                        exception = new RuntimeException("Unexpected system shutdown");
                    }
                    arm.close(exception);
                } catch (TaskException e) {
                    privateLogger.error("Error closing Result", e);
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);

        try {
            check();
            arm.setSteps(getSteps());
            run();
        } catch (TaskException e) {
            throw e;
        } catch (Exception e) {
            throw new TaskException(e);
        }

        try {
            return arm.close();
        } catch (RuntimeException | TaskException e) {
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    /**
     * Check that the given parameters are correct.
     * This method will be called before the {@link #run()}.
     *
     * @throws TaskException if the parameters are not correct
     */
    protected abstract void check() throws TaskException;

    /**
     * Method to be implemented by subclasses with the actual execution of the analysis.
     * @throws TaskException on error
     */
    protected abstract void run() throws TaskException;

    /**
     * Method to be called by the Runtime shutdownHook in case of an unexpected system shutdown.
     */
    protected void onShutdown() {
    }

    /**
     * @return the task id
     */
    public final String getId() {
        return taskId;
    }

    /**
     * @return the analysis steps
     */
    protected abstract List<String> getSteps();

    @FunctionalInterface
    protected interface StepRunnable {
        void run() throws Exception;
    }

    protected final void step(StepRunnable step) throws TaskException {
        step(getId(), step);
    }

    protected final void step(String stepId, StepRunnable step) throws TaskException {
        if (checkStep(stepId)) {
            try {
                step.run();
            } catch (TaskException e) {
                throw e;
            } catch (Exception e) {
                throw new TaskException("Exception from step " + stepId, e);
            }
        }
    }

    protected final boolean checkStep(String stepId) throws TaskException {
        return arm.checkStep(stepId);
    }

    protected final void errorStep() throws TaskException {
        arm.errorStep();
    }

    protected final void addWarning(String warning) throws TaskException {
        arm.addWarning(warning);
    }

    protected final void addError(Exception e) throws TaskException {
        arm.addError(e);
    }

}
