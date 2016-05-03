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
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecutor;
import org.opencb.opencga.analysis.AnalysisOutputRecorder;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.api.CatalogCohortDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;

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

    /**
     * Index a variant file.
     *
     * steps:
     * 1) Create, if not provided, an indexation job
     * 2) Initialize VariantStorageManager
     * 3) Read and validate cli args. Configure options
     * 4) Execute indexation
     * 5) Post process job. Update indexation status
     * 6) Record job output. (Only if the job was not provided)
     *
     * @throws CatalogException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws StorageManagerException
     * @throws AnalysisExecutionException
     * @throws IOException
     */
    private void index() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException,
            StorageManagerException, InstantiationException, IllegalAccessException {
        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;


        String sessionId = variantCommandOptions.commonOptions.sessionId;
        long inputFileId = catalogManager.getFileId(cliOptions.fileId);

        // 1) Create, if not provided, an indexation job
        final Job job;
        if (cliOptions.jobId < 0) {
            long outDirId = catalogManager.getFileId(cliOptions.outdirId);
            job = createIndexationJob(cliOptions, inputFileId, outDirId, sessionId);
            try {
                index(job);
            } finally {
                // 6) Record job output. (Only if the job was not provided)
                AnalysisOutputRecorder outputRecorder = new AnalysisOutputRecorder(catalogManager, sessionId);
                outputRecorder.recordJobOutput(job);
            }
        } else {
            job = catalogManager.getJob(cliOptions.jobId, null, sessionId).first();
            index(job);
        }
    }

    private void index(Job job)
            throws CatalogException, IllegalAccessException, ClassNotFoundException,
            InstantiationException, StorageManagerException {
        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;


        String sessionId = variantCommandOptions.commonOptions.sessionId;
        long inputFileId = catalogManager.getFileId(cliOptions.fileId);



        // 2) Initialize VariantStorageManager
        long studyId = catalogManager.getStudyIdByFileId(inputFileId);
        Study study = catalogManager.getStudy(studyId, sessionId).first();

        /*
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used from Catalog
         */
        DataStore dataStore = AnalysisFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.VARIANT, sessionId);
        initVariantStorageManager(dataStore);

        // 3) Read and validate cli args. Configure options
        ObjectMap options = storageConfiguration.getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getOptions();
        options.put(VariantStorageManager.Options.DB_NAME.key(), dataStore.getDbName());
        options.put(VariantStorageManager.Options.STUDY_NAME.key(), study.getAlias());
        options.put(VariantStorageManager.Options.STUDY_ID.key(), studyId);
        options.put(VariantStorageManager.Options.FILE_ID.key(), inputFileId);
        options.put(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats);
        options.put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields);
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

        // 4) Execute indexation
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
            // 5) Post process job. Update indexation status
            postProcessJob(job, storageETLResult, exception, sessionId);
        }
    }


    private void postProcessJob(Job job, StorageETLResult storageETLResult, Exception e, String sessionId) throws CatalogException {
        boolean jobFailed = e != null;

        Integer indexedFileId = (Integer) job.getAttributes().get(Job.INDEXED_FILE_ID);
        File indexedFile = catalogManager.getFile(indexedFileId, sessionId).first();
        final Index index;

        boolean transformedSuccess = storageETLResult != null && storageETLResult.isTransformExecuted() && storageETLResult.getTransformError() == null;
        boolean loadedSuccess = storageETLResult != null && storageETLResult.isLoadExecuted() && storageETLResult.getLoadError() == null;

        if (indexedFile.getIndex() != null) {
            index = indexedFile.getIndex();
            switch (index.getStatus().getStatus()) {
                case Index.IndexStatus.NONE:
                case Index.IndexStatus.TRANSFORMED:
                    logger.warn("Unexpected index status. Expected "
                            + Index.IndexStatus.TRANSFORMING + ", "
                            + Index.IndexStatus.LOADING + " or "
                            + Index.IndexStatus.INDEXING
                            + " and got " + index.getStatus());
                case Index.IndexStatus.READY: //Do not show warn message when index status is READY.
                    break;
                case Index.IndexStatus.TRANSFORMING:
                    if (jobFailed) {
                        logger.warn("Job failed. Restoring status from " +
                                Index.IndexStatus.TRANSFORMING + " to " + Index.IndexStatus.NONE);
                        index.getStatus().setStatus(Index.IndexStatus.NONE);
                    } else {
                        index.getStatus().setStatus(Index.IndexStatus.TRANSFORMED);
                    }
                    break;
                case Index.IndexStatus.LOADING:
                    if (jobFailed) {
                        logger.warn("Job failed. Restoring status from " +
                                Index.IndexStatus.LOADING + " to " + Index.IndexStatus.TRANSFORMED);
                        index.getStatus().setStatus(Index.IndexStatus.TRANSFORMED);
                    } else {
                        index.getStatus().setStatus(Index.IndexStatus.READY);
                    }
                    break;
                case Index.IndexStatus.INDEXING:
                    if (jobFailed) {
                        String newStatus;
                        // If transform was executed, restore status to Transformed.
                        if (transformedSuccess) {
                            newStatus = Index.IndexStatus.TRANSFORMED;
                        } else {
                            newStatus = Index.IndexStatus.NONE;
                        }
                        logger.warn("Job failed. Restoring status from " +
                                Index.IndexStatus.INDEXING + " to " + newStatus);
                        index.getStatus().setStatus(newStatus);
                    } else {
                        index.getStatus().setStatus(Index.IndexStatus.READY);
                    }
                    break;
            }
        } else {
            index = new Index(job.getUserId(), job.getDate(), new Index.IndexStatus(Index.IndexStatus.READY), job.getId(),
                    new HashMap<>());
            logger.warn("Expected INDEX object on the indexed file " +
                    "{ id:" + indexedFile.getId() + ", path:\"" + indexedFile.getPath() + "\"}");
        }

        if (transformedSuccess) {
            FileMetadataReader.get(catalogManager).updateVariantFileStats(job, sessionId);
        }

        catalogManager.modifyFile(indexedFileId, new ObjectMap("index", index), sessionId); //Modify status
        if (index.getStatus().getStatus().equals(Index.IndexStatus.READY) && Boolean.parseBoolean(job.getAttributes().getOrDefault(VariantStorageManager.Options.CALCULATE_STATS.key(), VariantStorageManager.Options.CALCULATE_STATS.defaultValue()).toString())) {
            QueryResult<Cohort> queryResult = catalogManager.getAllCohorts(catalogManager.getStudyIdByJobId(job.getId()), new Query(CatalogCohortDBAdaptor.QueryParams.NAME.key(), StudyEntry.DEFAULT_COHORT), new QueryOptions(), sessionId);
            if (queryResult.getNumResults() != 0) {
                logger.debug("Default cohort status set to READY");
                Cohort defaultCohort = queryResult.first();
                catalogManager.modifyCohort(defaultCohort.getId(),
                        new ObjectMap(CatalogCohortDBAdaptor.QueryParams.STATUS_STATUS.key(), Cohort.CohortStatus.READY),
                        new QueryOptions(), sessionId);
            }
        }
    }

    private Job createIndexationJob(AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions, long fileId, long outdirId, String sessionId) throws CatalogException, AnalysisExecutionException, IOException {

        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);

        QueryOptions options = new QueryOptions()
                .append(AnalysisJobExecutor.EXECUTE, false)
                .append(AnalysisJobExecutor.SIMULATE, false)
                .append(AnalysisFileIndexer.TRANSFORM, cliOptions.transform)
                .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats);

        QueryResult<Job> result = analysisFileIndexer.index(fileId, outdirId, sessionId, options);

        return result.first();
    }

}
