/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.analysis;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.core.annotations.ToolExecutor;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exception.ToolException;
import org.opencb.opencga.core.exception.ToolExecutorException;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.tools.result.ExecutionResult;
import org.opencb.opencga.core.tools.result.ExecutorInfo;
import org.opencb.opencga.core.tools.result.ExecutorResultManager;
import org.opencb.opencga.core.tools.result.FileResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.core.tools.OpenCgaToolExecutor.EXECUTOR_ID;

public abstract class OpenCgaTool {

    protected CatalogManager catalogManager;
    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected VariantStorageManager variantStorageManager;

    protected String opencgaHome;
    protected String token;

    protected ObjectMap params;
    protected ObjectMap executorParams;
    protected List<ToolExecutor.Source> sourceTypes;
    protected List<ToolExecutor.Framework> availableFrameworks;
    protected Logger logger;
    private Path outDir;
    private Path scratchDir;
    private final Logger privateLogger = LoggerFactory.getLogger(OpenCgaTool.class);

    private ExecutorResultManager erm;

    public OpenCgaTool() {
    }

    public final OpenCgaTool setUp(String opencgaHome, CatalogManager catalogManager, StorageEngineFactory engineFactory,
                                   ObjectMap params, Path outDir, String token) {
        VariantStorageManager manager = new VariantStorageManager(catalogManager, engineFactory);
        return setUp(opencgaHome, catalogManager, manager, params, outDir, token);
    }

    public final OpenCgaTool setUp(VariantStorageManager variantStorageManager, ObjectMap params, Path outDir, String token) {
        return setUp(null, variantStorageManager.getCatalogManager(), variantStorageManager, params, outDir, token);
    }

    public final OpenCgaTool setUp(String opencgaHome, CatalogManager catalogManager, VariantStorageManager variantStorageManager,
                                   ObjectMap params, Path outDir, String token) {
        this.opencgaHome = opencgaHome;
        this.catalogManager = catalogManager;
        this.configuration = catalogManager.getConfiguration();
        this.variantStorageManager = variantStorageManager;
        this.storageConfiguration = variantStorageManager.getStorageConfiguration();
        this.token = token;
        this.params = params == null ? new ObjectMap() : new ObjectMap(params);
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
        this.params = params == null ? new ObjectMap() : new ObjectMap(params);
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
            if (getToolType().equals(Tool.ToolType.VARIANT)) {
                sourceTypes.add(ToolExecutor.Source.MONGODB);
            }
        } else if (storageConfiguration.getVariant().getDefaultEngine().equals("hadoop")) {
            availableFrameworks.add(ToolExecutor.Framework.MAP_REDUCE);
            // TODO: Check from configuration if spark is available
//            availableFrameworks.add(ToolExecutor.Framework.SPARK);
            if (getToolType().equals(Tool.ToolType.VARIANT)) {
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
        erm = new ExecutorResultManager(getId(), outDir);
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
                            String warn = "Unable to access scratch folder '" + scratch + "'";
                            privateLogger.warn(warn);
                            addWarning(warn);
                        }
                    } catch (InvalidPathException e) {
                        String warn = "Unable to access scratch folder '"
                                + configuration.getAnalysis().getScratchDir() + "'";
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

                erm.setParams(params); // params may be modified after check method

                run();
            } catch (ToolException e) {
                throw e;
            } catch (Exception e) {
                throw new ToolException(e);
            }
            try {
                FileUtils.deleteDirectory(scratchDir.toFile());
            } catch (IOException e) {
                String warningMessage = "Error deleting scratch folder " + scratchDir + " : " + e.getMessage();
                privateLogger.warn(warningMessage, e);
                erm.addWarning(warningMessage);
            }
            erm.setParams(params);
            return erm.close();
        } catch (RuntimeException | ToolException e) {
            erm.setParams(params);
            erm.close(e);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
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
     * @return the tool steps
     */
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add(getId());
        return steps;
    }

    /**
     * @return the tool id
     */
    public final Tool.ToolType getToolType() {
        return this.getClass().getAnnotation(Tool.class).type();
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

    protected final String getToken() {
        return token;
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
                step.run();
            } catch (ToolException e) {
                throw e;
            } catch (Exception e) {
                throw new ToolException("Exception from step " + stepId, e);
            }
        }
    }

    protected final boolean checkStep(String stepId) throws ToolException {
        return erm.checkStep(stepId);
    }

    protected final void errorStep() throws ToolException {
        erm.errorStep();
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

    protected final void addFile(Path file) throws ToolException {
        erm.addFile(file, FileResult.FileType.fromName(file.getFileName().toString()));
    }

    protected final void addFile(Path file, FileResult.FileType fileType) throws ToolException {
        erm.addFile(file, fileType);
    }

    protected final List<FileResult> getOutputFiles() throws ToolException {
        return erm.read().getOutputFiles();
    }

    protected final Class<? extends OpenCgaToolExecutor> getToolExecutorClass(String toolExecutorId) {
        return getToolExecutorClass(OpenCgaToolExecutor.class, toolExecutorId);
    }

    protected final <T extends OpenCgaToolExecutor> Class<? extends T> getToolExecutorClass(Class<T> clazz, String toolExecutorId) {
        Objects.requireNonNull(clazz);
        String toolId = getId();

        List<Class<? extends T>> candidateClasses = new ArrayList<>();
        // If the given class is not abstract, check if matches the criteria.
        if (!Modifier.isAbstract(clazz.getModifiers())) {
            if (isValidClass(toolId, toolExecutorId, clazz)) {
                if (StringUtils.isNotEmpty(toolExecutorId) || Modifier.isFinal(clazz.getModifiers())) {
                    // Shortcut to skip reflection
                    return clazz;
                }
                candidateClasses.add(clazz);
            }
        }

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, ToolExecutor.class.getName())))
                .addUrls(ClasspathHelper.forJavaClassPath())
                .filterInputsBy(input -> input.endsWith(".class"))
        );

        Set<Class<? extends T>> typesAnnotatedWith = reflections.getSubTypesOf(clazz);
        for (Class<? extends T> aClass : typesAnnotatedWith) {
            if (isValidClass(toolId, toolExecutorId, aClass)) {
                candidateClasses.add(aClass);
            }
        }
        if (candidateClasses.isEmpty()) {
            return null;
        } else if (candidateClasses.size() == 1) {
            return candidateClasses.get(0);
        } else {
            privateLogger.info("Found multiple " + OpenCgaToolExecutor.class.getName() + " candidates.");
            for (Class<? extends T> matchedClass : candidateClasses) {
                privateLogger.info(" - " + matchedClass);
            }
            privateLogger.info("Sort by framework and source preference.");

            // Prefer the executor that matches better with the source
            // Prefer the executor that matches better with the framework
            List<ToolExecutor.Framework> finalAvailableFrameworks =
                    availableFrameworks == null ? Collections.emptyList() : availableFrameworks;
            List<ToolExecutor.Source> finalSourceTypes =
                    sourceTypes == null ? Collections.emptyList() : sourceTypes;

            Comparator<Class<? extends T>> comparator = Comparator.<Class<? extends T>>comparingInt(c1 -> {
                ToolExecutor annot1 = c1.getAnnotation(ToolExecutor.class);
                return finalAvailableFrameworks.indexOf(annot1.framework());
            }).thenComparingInt(c -> {
                ToolExecutor annot = c.getAnnotation(ToolExecutor.class);
                return finalSourceTypes.indexOf(annot.source());
            }).thenComparing(Class::getName);

            candidateClasses.sort(comparator);

            return candidateClasses.get(0);
        }
    }

    private <T> boolean isValidClass(String toolId, String toolExecutorId, Class<T> aClass) {
        ToolExecutor annotation = aClass.getAnnotation(ToolExecutor.class);
        if (annotation != null) {
            if (annotation.tool().equals(toolId)) {
                if (StringUtils.isEmpty(toolExecutorId) || toolExecutorId.equals(annotation.id())) {
                    if (CollectionUtils.isEmpty(sourceTypes) || sourceTypes.contains(annotation.source())) {
                        if (CollectionUtils.isEmpty(availableFrameworks) || availableFrameworks.contains(annotation.framework())) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    protected final OpenCgaToolExecutor getToolExecutor()
            throws ToolExecutorException {
        return getToolExecutor(OpenCgaToolExecutor.class);
    }

    protected final <T extends OpenCgaToolExecutor> T getToolExecutor(Class<T> clazz)
            throws ToolExecutorException {
        String executorId = executorParams == null ? null : executorParams.getString(EXECUTOR_ID);
        if (StringUtils.isEmpty(executorId) && params != null) {
            executorId = params.getString(EXECUTOR_ID);
        }
        return getToolExecutor(clazz, executorId);
    }

    protected final <T extends OpenCgaToolExecutor> T getToolExecutor(Class<T> clazz, String toolExecutorId)
            throws ToolExecutorException {
        Class<? extends T> executorClass = getToolExecutorClass(clazz, toolExecutorId);
        if (executorClass == null) {
            throw ToolExecutorException.executorNotFound(clazz, getId(), toolExecutorId, sourceTypes, availableFrameworks);
        }
        try {
            T t = executorClass.newInstance();
            privateLogger.info("Using " + clazz.getName() + " '" + t.getId() + "' : " + executorClass);

            String executorId = t.getId();
            if (executorParams == null) {
                executorParams = new ObjectMap();
            }
            executorParams.put(EXECUTOR_ID, executorId);

            // Update executor ID
            if (erm != null) {
                erm.setExecutorInfo(new ExecutorInfo(executorId, executorClass, executorParams, t.getSource(), t.getFramework()));
            }
            t.setUp(erm, executorParams, outDir);

            return t;
        } catch (InstantiationException | IllegalAccessException | ToolException e) {
            throw ToolExecutorException.cantInstantiate(executorClass, e);
        }
    }

    protected final void setUpStorageEngineExecutor(String study) throws ToolException {
        executorParams.put("opencgaHome", opencgaHome);
        executorParams.put("token", token);
        try {
            DataStore dataStore = variantStorageManager.getDataStore(study, token);

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


    protected final Analyst getAnalyst(String token) throws ToolException {
        try {
            String userId = catalogManager.getUserManager().getUserId(token);
            DataResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);

            return new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization());
        } catch (CatalogException e) {
            throw new ToolException(e.getMessage(), e);
        }
    }
}
