/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.app.cli.analysis;


import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.analysis.AnalysisOutputRecorder;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends AnalysisCommandExecutor {

    private AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions;
    private VariantStorageManager variantStorageManager;
    private CatalogManager catalogManager;

    public VariantCommandExecutor(AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonOptions);
        this.variantCommandOptions = variantCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = variantCommandOptions.getParsedSubCommand();
        configure();
        switch (subCommandString) {
            case "ibs":
                ibs();
                break;
            case "delete":
                delete();
                break;
            case "query":
                query();
                break;
            case "index":
                index();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void configure()
            throws IllegalAccessException, ClassNotFoundException, InstantiationException, CatalogException {

        //  Creating CatalogManager
        catalogManager = new CatalogManager(catalogConfiguration);

        // Creating StorageManagerFactory
        storageManagerFactory = new StorageManagerFactory(storageConfiguration);

    }

    private void initVariantStorageManager(DataStore dataStore)
            throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        String storageEngine = dataStore.getStorageEngine();
        if (StringUtils.isEmpty(storageEngine)) {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager();
        } else {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        }
    }


    private void ibs() {
        throw new UnsupportedOperationException();
    }

    private void install() {
        throw new UnsupportedOperationException();
    }

    private void query() {
        throw new UnsupportedOperationException();
    }

    private void delete() {
        throw new UnsupportedOperationException();
    }

    private void index() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException,
            StorageManagerException, InstantiationException, IllegalAccessException {
        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;


        String sessionId = variantCommandOptions.commonOptions.sessionId;
        long inputFileId = catalogManager.getFileId(cliOptions.fileId);

        // 1) Create, if not provided, an indexation job
        if (cliOptions.jobId < 0) {
            Job job;
            long outDirId = catalogManager.getFileId(cliOptions.outdirId);

            AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);

            List<String> extraParams = cliOptions.commonOptions.params.entrySet()
                    .stream()
                    .map(entry -> "-D" + entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.toList());

            QueryOptions options = new QueryOptions()
                    .append(AnalysisJobExecutor.EXECUTE, false)
                    .append(AnalysisJobExecutor.SIMULATE, false)
                    .append(AnalysisFileIndexer.TRANSFORM, cliOptions.transform)
                    .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                    .append(AnalysisFileIndexer.PARAMETERS, extraParams)
                    .append(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats)
                    .append(VariantStorageManager.Options.ANNOTATE.key(), cliOptions.annotate)
                    .append(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated)
                    .append(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields)
                    .append(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype)
                    .append(AnalysisFileIndexer.LOG_LEVEL, cliOptions.commonOptions.logLevel);

            QueryResult<Job> result = analysisFileIndexer.index(inputFileId, outDirId, sessionId, options);

            job = result.first();
            ObjectMap update = new ObjectMap("commandLine", job.getCommandLine() + " --job-id " + job.getId());
            job = (Job) catalogManager.modifyJob(job.getId(), update, sessionId).first();

            //Execute locally in a new process. This call will also record the job output files.
            AnalysisJobExecutor.executeLocal(catalogManager, job, sessionId);

//            // Execute in the same thread. Then, record the job output files.
//            try {
//                index(job);
//            } finally {
//                // Record job output
//                AnalysisOutputRecorder outputRecorder = new AnalysisOutputRecorder(catalogManager, sessionId);
//                outputRecorder.recordJobOutput(job);
//            }
        } else {
            index(catalogManager.getJob(cliOptions.jobId, null, sessionId).first());
        }
    }

    /**
     * Index a variant file.
     *
     * steps:
     * 1) Initialize VariantStorageManager
     * 2) Read and validate cli args. Configure options
     * 3) Execute indexation
     * 4) Post process job. Update indexation status
     *
     * @throws CatalogException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws StorageManagerException
     */
    private void index(Job job)
            throws CatalogException, IllegalAccessException, ClassNotFoundException,
            InstantiationException, StorageManagerException {
        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;


        String sessionId = variantCommandOptions.commonOptions.sessionId;
        long inputFileId = catalogManager.getFileId(cliOptions.fileId);



        // 1) Initialize VariantStorageManager
        long studyId = catalogManager.getStudyIdByFileId(inputFileId);
        Study study = catalogManager.getStudy(studyId, sessionId).first();

        /*
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used from Catalog
         */
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        initVariantStorageManager(dataStore);

        // 2) Read and validate cli args. Configure options
        ObjectMap options = storageConfiguration.getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(VariantStorageManager.Options.DB_NAME.key(), dataStore.getDbName());
        options.put(VariantStorageManager.Options.STUDY_NAME.key(), study.getAlias());
        options.put(VariantStorageManager.Options.STUDY_ID.key(), studyId);
        options.put(VariantStorageManager.Options.FILE_ID.key(), inputFileId);
        options.put(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats);
        options.put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields);
        options.put(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype);
        options.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated);

        options.put(VariantStorageManager.Options.ANNOTATE.key(), cliOptions.annotate);
        if (cliOptions.annotator != null) {
            options.put(VariantAnnotationManager.ANNOTATION_SOURCE, cliOptions.annotator);
        }
        options.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations);

//        if (cliOptions.aggregationMappingFile != null) {
//            // TODO move this options to new configuration.yml
//            Properties aggregationMappingProperties = new Properties();
//            try {
//                aggregationMappingProperties.load(new FileInputStream(cliOptions.aggregationMappingFile));
//                options.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
//            } catch (FileNotFoundException e) {
//                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", cliOptions
//                        .aggregationMappingFile);
//            }
//        }

        if (cliOptions.commonOptions.params != null) {
            options.putAll(cliOptions.commonOptions.params);
        }

        final boolean doExtract;
        final boolean doTransform;
        final boolean doLoad;
        StorageETLResult storageETLResult = null;
        Exception exception = null;

        if (!cliOptions.load && !cliOptions.transform) {
            doExtract = true;
            doTransform = true;
            doLoad = true;
        } else {
            doExtract = cliOptions.transform;
            doTransform = cliOptions.transform;
            doLoad = cliOptions.load;
        }

        // 3) Execute indexation
        try {
            File file = catalogManager.getFile(inputFileId, sessionId).first();
            URI fileUri = catalogManager.getFileUri(file);
            storageETLResult = variantStorageManager.index(Collections.singletonList(fileUri), job.getTmpOutDirUri(),
                    doExtract, doTransform, doLoad).get(0);
        } catch (StorageETLException e) {
            storageETLResult = e.getResults().get(0);
            exception = e;
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            exception = e;
            e.printStackTrace();
            throw e;
        } finally {
            // 4) Post process job. Update indexation status
//            new AnalysisOutputRecorder(catalogManager, sessionId).postProcessIndexJob(job, storageETLResult, e, sessionId);
            new AnalysisOutputRecorder(catalogManager, sessionId).saveStorageResult(job, storageETLResult);
        }
    }

}
