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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.core.analysis.result.AnalysisResult;
import org.opencb.opencga.core.analysis.result.AnalysisResultManager;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.annotations.AnalysisExecutor;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public abstract class OpenCgaAnalysis {

    public static final String EXECUTOR_ID = "ID";
    protected CatalogManager catalogManager;
    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;
    protected VariantStorageManager variantStorageManager;

    protected String opencgaHome;
    protected String sessionId;

    protected ObjectMap executorParams;
    protected Path outDir;
    protected List<AnalysisExecutor.Source> sourceTypes;
    protected List<AnalysisExecutor.Framework> availableFrameworks;
    protected final Logger logger = LoggerFactory.getLogger(OpenCgaAnalysis.class);

    protected AnalysisResultManager arm;

    public OpenCgaAnalysis() {
    }

    public OpenCgaAnalysis(String opencgaHome, String sessionId) {
        this.opencgaHome = opencgaHome;
        this.sessionId = sessionId;
    }


    public final OpenCgaAnalysis setUp(String opencgaHome, CatalogManager catalogManager, VariantStorageManager variantStorageManager,
                                       ObjectMap executorParams, Path outDir, String sessionId) {
        this.opencgaHome = opencgaHome;
        this.catalogManager = catalogManager;
        this.configuration = catalogManager.getConfiguration();
        this.variantStorageManager = variantStorageManager;
        this.storageConfiguration = variantStorageManager.getStorageConfiguration();
        this.sessionId = sessionId;

        return setUp(executorParams, outDir);
    }

    public final OpenCgaAnalysis setUp(String opencgaHome, ObjectMap executorParams, Path outDir, String sessionId)
            throws AnalysisException {
        this.opencgaHome = opencgaHome;
        this.sessionId = sessionId;
        this.executorParams = executorParams;
        this.outDir = outDir;

        try {
            loadConfiguration();
            loadStorageConfiguration();

            this.catalogManager = new CatalogManager(configuration);
            this.variantStorageManager = new VariantStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
        } catch (IOException | CatalogException e) {
            throw new AnalysisException(e);
        }

        return setUp(executorParams, outDir);
    }

    private OpenCgaAnalysis setUp(ObjectMap executorParams, Path outDir) {
//        logger = LoggerFactory.getLogger(this.getClass().toString());

        availableFrameworks = new ArrayList<>();
        sourceTypes = new ArrayList<>();
        if (storageConfiguration.getDefaultStorageEngineId().equals("mongodb")) {
            if (getAnalysisData().equals(Analysis.AnalysisData.VARIANT)) {
                sourceTypes.add(AnalysisExecutor.Source.MONGODB);
            }
        } else if (storageConfiguration.getDefaultStorageEngineId().equals("hadoop")) {
            availableFrameworks.add(AnalysisExecutor.Framework.MAP_REDUCE);
            // TODO: Check from configuration if spark is available
//            availableFrameworks.add(AnalysisExecutor.Framework.SPARK);
            if (getAnalysisData().equals(Analysis.AnalysisData.VARIANT)) {
                sourceTypes.add(AnalysisExecutor.Source.HBASE);
            }
        }

        availableFrameworks.add(AnalysisExecutor.Framework.ITERATOR);
        sourceTypes.add(AnalysisExecutor.Source.OPENCGA);

        setUp(executorParams, outDir, sourceTypes, availableFrameworks);
        return this;
    }

    /**
     * Setup the analysis providing the parameters required for the execution.
     * @param executorParams        Params to be provided to the Executor
     * @param outDir                Output directory
     * @param inputDataSourceTypes  Input data source types
     * @param availableFrameworks   Available frameworks in this environment
     */
    private final void setUp(ObjectMap executorParams, Path outDir,
                             List<AnalysisExecutor.Source> inputDataSourceTypes,
                             List<AnalysisExecutor.Framework> availableFrameworks) {
        this.executorParams = executorParams;
        this.outDir = outDir;
        this.sourceTypes = inputDataSourceTypes;
        this.availableFrameworks = availableFrameworks == null ? null : new ArrayList<>(availableFrameworks);
    }

    /**
     * Execute the analysis. The analysis should have been properly setUp before being executed.
     *
     * @return AnalysisResult
     * @throws AnalysisException on error
     */
    public final AnalysisResult execute() throws AnalysisException {
        arm = new AnalysisResultManager(outDir);
        arm.init(getId(), executorParams);
        try {
            execute(arm);
            return arm.close();
        } catch (Exception e) {
            arm.close(e);
            throw e;
        }
    }

    public final void execute(AnalysisResultManager arm) throws AnalysisException {
        this.arm = arm;
        check();
        exec();
    }

    /**
     * Check that the given parameters are correct.
     * This method will be called before the {@link #exec()}.
     *
     * @throws AnalysisException if the parameters are not correct
     */
    protected void check() throws AnalysisException {
    }

    /**
     * Method to be implemented by subclasses with the actual execution of the analysis.
     * @throws AnalysisException on error
     */
    protected abstract void exec() throws AnalysisException;

    /**
     * @return the analysis id
     */
    public final String getId() {
        return this.getClass().getAnnotation(Analysis.class).id();
    }

    /**
     * @return the analysis id
     */
    public final Analysis.AnalysisData getAnalysisData() {
        return this.getClass().getAnnotation(Analysis.class).data();
    }

    public final OpenCgaAnalysis addSource(AnalysisExecutor.Source source) {
        if (sourceTypes == null) {
            sourceTypes = new ArrayList<>();
        }
        sourceTypes.add(source);
        return this;
    }

    public final OpenCgaAnalysis addFramework(AnalysisExecutor.Framework framework) {
        if (availableFrameworks == null) {
            availableFrameworks = new ArrayList<>();
        }
        availableFrameworks.add(framework);
        return this;
    }

    protected final Class<? extends OpenCgaAnalysisExecutor> getAnalysisExecutorClass(String analysisExecutorId) {
        return getAnalysisExecutorClass(OpenCgaAnalysisExecutor.class, analysisExecutorId, null, null);
    }

    protected final <T extends OpenCgaAnalysisExecutor> Class<? extends T> getAnalysisExecutorClass(
            Class<T> clazz, String analysisExecutorId, List<AnalysisExecutor.Source> sourceTypes,
            List<AnalysisExecutor.Framework> availableFrameworks) {
        Objects.requireNonNull(clazz);
        String analysisId = getId();

        if (sourceTypes == null) {
            sourceTypes = this.sourceTypes;
        }
        if (CollectionUtils.isEmpty(availableFrameworks)) {
            availableFrameworks = this.availableFrameworks;
        }

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(
                        new SubTypesScanner(),
                        new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, AnalysisExecutor.class.getName())))
                .addUrls(ClasspathHelper.forJavaClassPath())
                .filterInputsBy(input -> input.endsWith(".class"))
        );

        Set<Class<? extends T>> typesAnnotatedWith = reflections.getSubTypesOf(clazz);
        List<Class<? extends T>> matchedClasses = new ArrayList<>();
        for (Class<? extends T> aClass : typesAnnotatedWith) {
            AnalysisExecutor annotation = aClass.getAnnotation(AnalysisExecutor.class);
            if (annotation != null) {
                if (annotation.analysis().equals(analysisId)) {
                    if (StringUtils.isEmpty(analysisExecutorId) || analysisExecutorId.equals(annotation.id())) {
                        if (sourceTypes == null || sourceTypes.contains(annotation.source())) {
                            if (CollectionUtils.isEmpty(availableFrameworks) || availableFrameworks.contains(annotation.framework())) {
                                matchedClasses.add(aClass);
                            }
                        }
                    }
                }
            }
        }
        if (matchedClasses.isEmpty()) {
            return null;
        } else if (matchedClasses.size() == 1) {
            return matchedClasses.get(0);
        } else {
            logger.info("Found multiple OpenCgaAnalysisExecutor candidates.");
            for (Class<? extends T> matchedClass : matchedClasses) {
                logger.info(" - " + matchedClass);
            }
            logger.info("Sort by framework and source preference.");

            // Prefer the executor that matches better with the source
            // Prefer the executor that matches better with the framework
            List<AnalysisExecutor.Framework> finalAvailableFrameworks =
                    availableFrameworks == null ? Collections.emptyList() : availableFrameworks;
            List<AnalysisExecutor.Source> finalSourceTypes =
                    sourceTypes == null ? Collections.emptyList() : sourceTypes;

            Comparator<Class<? extends T>> comparator = Comparator.<Class<? extends T>>comparingInt(c1 -> {
                AnalysisExecutor annot1 = c1.getAnnotation(AnalysisExecutor.class);
                return finalAvailableFrameworks.indexOf(annot1.framework());
            }).thenComparingInt(c -> {
                AnalysisExecutor annot = c.getAnnotation(AnalysisExecutor.class);
                return finalSourceTypes.indexOf(annot.source());
            }).thenComparing(Class::getName);

            matchedClasses.sort(comparator);

            return matchedClasses.get(0);
        }
    }

    protected final OpenCgaAnalysisExecutor getAnalysisExecutor()
            throws AnalysisExecutorException {
        return getAnalysisExecutor(OpenCgaAnalysisExecutor.class, null, null, null);
    }

    protected final <T extends OpenCgaAnalysisExecutor> T getAnalysisExecutor(Class<T> clazz)
            throws AnalysisExecutorException {
        return getAnalysisExecutor(clazz, executorParams.getString(OpenCgaAnalysis.EXECUTOR_ID), null, null);
    }

    protected final <T extends OpenCgaAnalysisExecutor> T getAnalysisExecutor(Class<T> clazz, String analysisExecutorId)
            throws AnalysisExecutorException {
        return getAnalysisExecutor(clazz, analysisExecutorId, null, null);
    }

    protected final <T extends OpenCgaAnalysisExecutor> T getAnalysisExecutor(
            Class<T> clazz, String analysisExecutorId, List<AnalysisExecutor.Source> source,
            List<AnalysisExecutor.Framework> availableFrameworks)
            throws AnalysisExecutorException {
        Class<? extends T> executorClass = getAnalysisExecutorClass(clazz, analysisExecutorId, source, availableFrameworks);
        if (executorClass == null) {
            throw AnalysisExecutorException.executorNotFound(clazz, getId(), analysisExecutorId, source, availableFrameworks);
        }
        try {
            T t = executorClass.newInstance();
            logger.info("Using OpenCgaAnalysisExecutor '" + t.getId() + "' : " + executorClass);
            t.init(arm);

            // Update executor ID
            if (arm != null) {
                arm.updateResult(analysisResult -> {
                    String executorId = t.getId();
                    analysisResult.setExecutorId(executorId);
                    analysisResult.getExecutorParams().put(EXECUTOR_ID, executorId);
                });

            }
            t.setUp(executorParams, outDir);

            return t;
        } catch (InstantiationException | IllegalAccessException | AnalysisException e) {
            throw AnalysisExecutorException.cantInstantiate(executorClass, e);
        }
    }

    protected final void setUpStorageEngineExecutor(String study) throws AnalysisException {
        executorParams.put("opencgaHome", opencgaHome);
        executorParams.put("sessionId", sessionId);
        try {
            DataStore dataStore = variantStorageManager.getDataStore(study, sessionId);

            executorParams.put("storageEngineId", dataStore.getStorageEngine());
            executorParams.put("dbName", dataStore.getDbName());
        } catch (CatalogException e) {
            throw new AnalysisException(e);
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


    protected final Analyst getAnalyst(String token) throws AnalysisException {
        try {
            String userId = catalogManager.getUserManager().getUserId(token);
            DataResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);

            return new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization());
        } catch (CatalogException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

}
