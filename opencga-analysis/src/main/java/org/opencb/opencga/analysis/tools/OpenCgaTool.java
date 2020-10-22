/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.tools;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.MemoryUsageMonitor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.project.DataStore;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.ExecutionResultManager;
import org.opencb.opencga.core.tools.result.ExecutorInfo;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

public abstract class OpenCgaTool {

    protected CatalogManager catalogManager;
    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected VariantStorageManager variantStorageManager;

    private String jobId;
    private String opencgaHome;
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

    public OpenCgaTool() {
        privateLogger = LoggerFactory.getLogger(OpenCgaTool.class);
        toolExecutorFactory = new ToolExecutorFactory();
        params = new ObjectMap();
    }

    public final OpenCgaTool setUp(String opencgaHome, CatalogManager catalogManager, StorageEngineFactory engineFactory,
                                   ObjectMap params, Path outDir, String jobId, String token) {
        VariantStorageManager manager = new VariantStorageManager(catalogManager, engineFactory);
        return setUp(opencgaHome, catalogManager, manager, params, outDir, jobId, token);
    }

    public final OpenCgaTool setUp(String opencgaHome, CatalogManager catalogManager, VariantStorageManager variantStorageManager,
                                   ObjectMap params, Path outDir, String jobId, String token) {
        this.opencgaHome = opencgaHome;
        this.catalogManager = catalogManager;
        this.configuration = catalogManager.getConfiguration();
        this.variantStorageManager = variantStorageManager;
        this.storageConfiguration = variantStorageManager.getStorageConfiguration();
        this.jobId = jobId;
        this.token = token;
        if (params != null) {
            this.params.putAll(params);
        }
        this.executorParams = new ObjectMap();
        this.outDir = outDir;
        //this.params.put("outDir", outDir.toAbsolutePath().toString());

        setUpFrameworksAndSource();
        return this;
    }

    public final OpenCgaTool setUp(String opencgaHome, ObjectMap params, Path outDir, String token)
            throws ToolException {
        this.opencgaHome = opencgaHome;
        this.token = token;
        if (params != null) {
            this.params.putAll(params);
        }
        this.executorParams = new ObjectMap();
        this.outDir = outDir;
        //this.params.put("outDir", outDir.toAbsolutePath().toString());

        try {
            loadConfiguration();
            loadStorageConfiguration();

            this.catalogManager = new CatalogManager(configuration);
            this.variantStorageManager = new VariantStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
        } catch (IOException | CatalogException e) {
            throw new ToolException(e);
        }

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
    public final ExecutionResult start() throws ToolException {
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
                    erm.close(exception);
                } catch (ToolException e) {
                    privateLogger.error("Error closing ExecutionResult", e);
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        Exception exception = null;
        ExecutionResult result;
        try {
            if (scratchDir == null) {
                Path baseScratchDir = this.outDir;
                if (StringUtils.isNotEmpty(configuration.getAnalysis().getScratchDir())) {
                    Path scratch;
                    try {
                        scratch = Paths.get(configuration.getAnalysis().getScratchDir());
                        if (scratch.toFile().isDirectory() && scratch.toFile().canWrite()) {
                            baseScratchDir = scratch;
                        } else {
                            try {
                                FileUtils.forceMkdir(scratch.toFile());
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
                check();
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
            result = erm.close(exception);
            stopMemoryMonitor();
            privateLogger.info("------- Tool '" + getId() + "' executed in "
                    + TimeUtils.durationToString(result.getEnd().getTime() - result.getStart().getTime()) + " -------");
        }
        return result;
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
     * @return the tool steps
     */
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add(getId());
        return steps;
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

    public Path getOpencgaHome() {
        return Paths.get(opencgaHome);
    }

    public String getJobId() {
        return jobId;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public VariantStorageManager getVariantStorageManager() {
        return variantStorageManager;
    }

    public ObjectMap getParams() {
        return params;
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

    protected final void addWarning(String warning) throws ToolException {
        erm.addWarning(warning);
    }

    protected final void addError(Exception e) throws ToolException {
        erm.addError(e);
    }

    protected final void addAttribute(String key, Object value) throws ToolException {
        erm.addAttribute(key, value);
    }

    protected final void moveFile(String study, Path source, Path destiny, String catalogDirectoryPath, String token) throws ToolException {
        File file;
        try {
            file = catalogManager.getFileManager().moveAndRegister(study, source, destiny, catalogDirectoryPath, token).first();
        } catch (Exception e) {
            throw new ToolException("Error moving file from " + source + " to " + destiny, e);
        }
        // Add only if move is successful
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

    protected final void setUpStorageEngineExecutor(String study) throws ToolException {
        setUpStorageEngineExecutor(null, study);
    }

    protected final void setUpStorageEngineExecutorByProjectId(String projectId) throws ToolException {
        setUpStorageEngineExecutor(projectId, null);
    }

    private final void setUpStorageEngineExecutor(String projectId, String study) throws ToolException {
        executorParams.put("opencgaHome", opencgaHome);
        executorParams.put(ParamConstants.TOKEN, token);
        try {
            DataStore dataStore;
            if (study == null) {
                dataStore = variantStorageManager.getDataStoreByProjectId(projectId, token);
            } else {
                dataStore = variantStorageManager.getDataStore(study, token);
            }

            executorParams.put("storageEngineId", dataStore.getStorageEngine());
            executorParams.put("dbName", dataStore.getDbName());
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
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

    public ExecutionResultManager getErm() {
        return erm;
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
