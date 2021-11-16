package org.opencb.opencga.core.tools;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.ExceptionUtils;
import org.opencb.opencga.core.common.MemoryUsageMonitor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.ConfigurationUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
import org.opencb.opencga.core.tools.result.ExecutorInfo;
import org.opencb.opencga.core.tools.result.JobResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

public abstract class OpenCgaTool {

    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;

    private String jobId;
    protected String opencgaHome;
    protected String token;

    protected final ObjectMap params;
    protected ObjectMap executorParams;
    private List<ToolExecutor.Source> sourceTypes;
    private List<ToolExecutor.Framework> availableFrameworks;
    protected Logger logger;
    private Path outDir;
    private Path scratchDir;

    private final ToolExecutorFactory toolExecutorFactory;
    private final Logger privateLogger;

    private MemoryUsageMonitor memoryUsageMonitor;

    private ExecutionResultManager erm;
    private String currentStep;

    public OpenCgaTool() {
        privateLogger = LoggerFactory.getLogger(this.getClass());
        toolExecutorFactory = new ToolExecutorFactory();
        params = new ObjectMap();
    }

    protected final OpenCgaTool setUp(String opencgaHome, Configuration configuration, StorageConfiguration storageConfiguration,
                                      ObjectMap params, Path outDir, String jobId, String token) {
        this.opencgaHome = opencgaHome;
        this.configuration = configuration;
        this.storageConfiguration = storageConfiguration;
        this.jobId = jobId;
        this.token = token;
        if (params != null) {
            this.params.putAll(params);
        }
        this.executorParams = new ObjectMap();
        this.outDir = outDir;

        setUpFrameworksAndSource();
        return this;
    }

    private void setUpFrameworksAndSource() {
        logger = LoggerFactory.getLogger(this.getClass().toString());

        availableFrameworks = new ArrayList<>();
        sourceTypes = new ArrayList<>();
        if (storageConfiguration.getVariant().getDefaultEngine().equals("mongodb")) {
            if (getToolResource().equals(Enums.Resource.VARIANT)) {
                sourceTypes.add(ToolExecutor.Source.MONGODB);
            }
        } else if (storageConfiguration.getVariant().getDefaultEngine().equals("hadoop")) {
            availableFrameworks.add(ToolExecutor.Framework.MAP_REDUCE);
            // TODO: Check from configuration if spark is available
//            availableFrameworks.add(ToolExecutor.Framework.SPARK);
            if (getToolResource().equals(Enums.Resource.VARIANT)) {
                sourceTypes.add(ToolExecutor.Source.HBASE);
            }
        }

        availableFrameworks.add(ToolExecutor.Framework.LOCAL);
        sourceTypes.add(ToolExecutor.Source.STORAGE);
//        return this;
    }

    /**
     * Execute the tool. The tool should have been properly setUp before being executed.
     *
     * @return ExecutionResult
     * @throws ToolException on error
     */
    public final JobResult start() throws ToolException {
        if (this.getClass().getAnnotation(Tool.class) == null) {
            throw new ToolException("Missing @" + Tool.class.getSimpleName() + " annotation in " + this.getClass());
        }
        if (configuration.getAnalysis().getExecution().getOptions().getBoolean("memoryMonitor", false)) {
            startMemoryMonitor();
        }
        erm = new ExecutionResultManager(getId(), outDir);
        erm.init(params, executorParams);
        Thread hook = new Thread(() -> {
            Exception exception = null;
            try {
                onShutdown();
            } catch (Exception e) {
                exception = e;
            }
            if (!erm.isClosed()) {
                privateLogger.error("Unexpected system shutdown!");
                try {
                    if (scratchDir != null) {
                        deleteScratchDirectory();
                    }
                    if (exception == null) {
                        exception = new RuntimeException("Unexpected system shutdown");
                    }
                    logException(exception);
                    JobResult result = erm.close(exception);
                    privateLogger.info("------- Tool '" + getId() + "' executed in "
                            + TimeUtils.durationToString(result.getEnd().getTime() - result.getStart().getTime()) + " -------");
                } catch (ToolException e) {
                    privateLogger.error("Error closing ExecutionResult", e);
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        Exception exception = null;
        JobResult result;
        try {
            if (scratchDir == null) {
                Path baseScratchDir = this.outDir;
                if (StringUtils.isNotEmpty(configuration.getAnalysis().getScratchDir())) {
                    try {
                        Path scratch = Paths.get(configuration.getAnalysis().getScratchDir());
                        if (scratch.toFile().isDirectory() && scratch.toFile().canWrite()) {
                            baseScratchDir = scratch;
                        } else {
                            try {
                                FileUtils.forceMkdir(scratch.toFile());
                                baseScratchDir = scratch;
                            } catch (IOException e) {
                                String warn = "Unable to access scratch folder '" + scratch + "'. " + e.getMessage();
                                privateLogger.warn(warn);
                                addWarning(warn);
                            }
                        }
                    } catch (InvalidPathException e) {
                        String warn = "Unable to access scratch folder '"
                                + configuration.getAnalysis().getScratchDir() + "'. " + e.getMessage();
                        privateLogger.warn(warn);
                        addWarning(warn);
                    }
                }

                try {
                    scratchDir = Files.createDirectory(baseScratchDir.resolve("scratch_" + getId() + RandomStringUtils.randomAlphanumeric(10)));
                } catch (IOException e) {
                    throw new ToolException(e);
                }
            }
            try {
                currentStep = "check";
                privateCheck();
                check();
                currentStep = null;
                erm.setSteps(getSteps());
                run();
            } catch (ToolException e) {
                throw e;
            } catch (Exception e) {
                throw new ToolException(e);
            }
        } catch (RuntimeException | ToolException e) {
            exception = e;
            throw e;
        } finally {
            deleteScratchDirectory();
            Runtime.getRuntime().removeShutdownHook(hook);
            stopMemoryMonitor();
            result = erm.close(exception);
            logException(exception);
            privateLogger.info("------- Tool '" + getId() + "' executed in "
                    + TimeUtils.durationToString(result.getEnd().getTime() - result.getStart().getTime()) + " -------");
        }
        return result;
    }

    private void logException(Throwable throwable) {
        if (throwable == null) {
            return;
        }
        privateLogger.error("## -----------------------------------------------------");
//        privateLogger.error("## Catch exception [" + throwable.getClass().getSimpleName() + "] at step '" + getCurrentStep() + "'");
        for (String line : ExceptionUtils.prettyExceptionMessage(throwable, true, true).split("\n")) {
            privateLogger.error("## - " + line);
        }
        privateLogger.error("## -----------------------------------------------------");
    }

    private void deleteScratchDirectory() throws ToolException {
        try {
            FileUtils.deleteDirectory(scratchDir.toFile());
        } catch (IOException e) {
            String warningMessage = "Error deleting scratch folder " + scratchDir + " : " + e.getMessage();
            privateLogger.warn(warningMessage, e);
            erm.addWarning(warningMessage);
        }
    }

    private void startMemoryMonitor() {
        if (memoryUsageMonitor == null) {
            memoryUsageMonitor = new MemoryUsageMonitor();
            memoryUsageMonitor.start();
        }
    }

    private void stopMemoryMonitor() {
        if (memoryUsageMonitor != null) {
            memoryUsageMonitor.stop();
            memoryUsageMonitor = null;
        }
    }

    private void privateCheck() throws Exception {
        ToolParams toolParams = findToolParams();
        if (toolParams != null) {
            toolParams.updateParams(getParams());
        }
    }

    /**
     * Check that the given parameters are correct.
     * This method will be called before the {@link #run()}.
     *
     * @throws Exception if the parameters are not correct
     */
    protected void check() throws Exception {
    }

    /**
     * Method to be implemented by subclasses with the actual execution of the tool.
     *
     * @throws Exception on error
     */
    protected abstract void run() throws Exception;

    /**
     * Method to be called by the Runtime shutdownHook in case of an unexpected system shutdown.
     */
    protected void onShutdown() {
    }

    /**
     * @return the tool id
     */
    public final String getId() {
        return this.getClass().getAnnotation(Tool.class).id();
    }

    /**
     * @return the tool id
     */
    protected final Enums.Resource getToolResource() {
        return this.getClass().getAnnotation(Tool.class).resource();
    }

    /**
     * Method called internally to obtain the list of steps.
     * <p>
     * Will be executed after calling to the {@link #check()} method.
     *
     * @return the tool steps
     */
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add(getId());
        return steps;
    }

    protected final String getCurrentStep() {
        if (currentStep == null) {
            return getSteps().get(0);
        } else {
            return currentStep;
        }
    }

    /**
     * @return Output directory of the job.
     */
    public final Path getOutDir() {
        return outDir;
    }

    /**
     * @return Temporary scratch directory. Files generated in this folder will be deleted.
     */
    public final Path getScratchDir() {
        return scratchDir;
    }

    public final String getToken() {
        return token;
    }

    public final Path getOpencgaHome() {
        return Paths.get(opencgaHome);
    }

    public final String getJobId() {
        return jobId;
    }

    public final ObjectMap getParams() {
        return params;
    }

    private ToolParams findToolParams() throws ToolException {
        for (Field field : getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(org.opencb.opencga.core.tools.annotations.ToolParams.class)
                    && ToolParams.class.isAssignableFrom(field.getType())) {
                try {
                    field.setAccessible(true);
                    ToolParams toolParams = (ToolParams) field.get(this);
                    if (toolParams == null) {
                        toolParams = (ToolParams) field.getType().newInstance();
                        field.set(this, toolParams);
                    }
                    return toolParams;
                } catch (IllegalAccessException | InstantiationException e) {
                    throw new ToolException("Unexpected error reading ToolParams");
                }
            }
        }
        return null;
    }

    public final OpenCgaTool addSource(ToolExecutor.Source source) {
        if (sourceTypes == null) {
            sourceTypes = new ArrayList<>();
        }
        sourceTypes.add(source);
        return this;
    }

    public final OpenCgaTool addFramework(ToolExecutor.Framework framework) {
        if (availableFrameworks == null) {
            availableFrameworks = new ArrayList<>();
        }
        availableFrameworks.add(framework);
        return this;
    }

    @FunctionalInterface
    protected interface StepRunnable {
        void run() throws Exception;
    }

    protected final void step(StepRunnable step) throws ToolException {
        step(getId(), step);
    }

    protected final void step(String stepId, StepRunnable step) throws ToolException {
        if (checkStep(stepId)) {
            try {
                StopWatch stopWatch = StopWatch.createStarted();
                privateLogger.info("");
                privateLogger.info("------- Executing step '" + stepId + "' -------");
                currentStep = stepId;
                step.run();
                privateLogger.info("------- Step '" + stepId + "' executed in " + TimeUtils.durationToString(stopWatch) + " -------");
            } catch (ToolException e) {
                throw e;
            } catch (Exception e) {
                throw new ToolException("Exception from step " + stepId, e);
            }
        } else {
            privateLogger.info("------- Skip step " + stepId + " -------");
        }
    }

    protected final boolean checkStep(String stepId) throws ToolException {
        return erm.checkStep(stepId);
    }

    protected final void errorStep() throws ToolException {
        erm.errorStep();
    }

    protected final void addEvent(Event.Type type, String message) throws ToolException {
        erm.addEvent(type, message);
    }

    protected final void addInfo(String message) throws ToolException {
        erm.addEvent(Event.Type.INFO, message);
    }

    protected final void addWarning(String warning) throws ToolException {
        erm.addWarning(warning);
    }

    protected final void addError(Exception e) throws ToolException {
        erm.addError(e);
    }

    protected final void addAttribute(String key, Object value) throws ToolException {
        erm.addAttribute(key, value);
    }

    protected final void addGeneratedFile(File file) throws ToolException {
        erm.addExternalFile(file.getUri());
    }

    protected final OpenCgaToolExecutor getToolExecutor() throws ToolException {
        return getToolExecutor(OpenCgaToolExecutor.class);
    }

    protected final <T extends OpenCgaToolExecutor> T getToolExecutor(Class<T> clazz) throws ToolException {
        String executorId = executorParams == null ? null : executorParams.getString(EXECUTOR_ID);
        if (StringUtils.isEmpty(executorId) && params != null) {
            executorId = params.getString(EXECUTOR_ID);
        }
        return getToolExecutor(clazz, executorId);
    }

    protected final <T extends OpenCgaToolExecutor> T getToolExecutor(Class<T> clazz, String toolExecutorId) throws ToolException {
        T toolExecutor = toolExecutorFactory.getToolExecutor(getId(), toolExecutorId, clazz, sourceTypes, availableFrameworks);
        String executorId = toolExecutor.getId();
        if (executorParams == null) {
            executorParams = new ObjectMap();
        }
        executorParams.put(EXECUTOR_ID, executorId);

        // Update executor ID
        erm.setExecutorInfo(new ExecutorInfo(executorId,
                toolExecutor.getClass(),
                executorParams,
                toolExecutor.getSource(),
                toolExecutor.getFramework()));

        toolExecutor.setUp(erm, executorParams, outDir);
        return toolExecutor;
    }

    /**
     * This method attempts to load general configuration from OpenCGA installation folder, if not exists then loads JAR configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    private void loadConfiguration() throws IOException {
        this.configuration = ConfigurationUtils.loadConfiguration(opencgaHome);
    }

    /**
     * This method attempts to load storage configuration from OpenCGA installation folder, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    private void loadStorageConfiguration() throws IOException {
        this.storageConfiguration = ConfigurationUtils.loadStorageConfiguration(opencgaHome);
    }

    // TODO can this method be removed?
//    protected final Analyst getAnalyst(String token) throws ToolException {
//        try {
//            String userId = catalogManager.getUserManager().getUserId(token);
//            DataResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
//                    Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);
//
//            return new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization());
//        } catch (CatalogException e) {
//            throw new ToolException(e.getMessage(), e);
//        }
//    }
}
