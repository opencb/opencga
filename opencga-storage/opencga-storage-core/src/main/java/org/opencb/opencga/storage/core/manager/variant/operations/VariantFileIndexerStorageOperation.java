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

package org.opencb.opencga.storage.core.manager.variant.operations;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.stats.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.manager.models.FileInfo;
import org.opencb.opencga.storage.core.manager.models.StudyInfo;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.ID;
import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.UID;
import static org.opencb.opencga.catalog.utils.FileMetadataReader.VARIANT_FILE_STATS;

/**
 * Created by imedina on 17/08/16.
 */
public class VariantFileIndexerStorageOperation extends StorageOperation {

    public static final String DEFAULT_COHORT_DESCRIPTION = "Default cohort with almost all indexed samples";
    public static final QueryOptions FILE_GET_QUERY_OPTIONS = new QueryOptions()
            .append(QueryOptions.EXCLUDE, Arrays.asList(
                    FileDBAdaptor.QueryParams.ATTRIBUTES.key(),
                    FileDBAdaptor.QueryParams.STATS.key()))
            .append(QueryOptions.SORT, FileDBAdaptor.QueryParams.NAME.key())
            .append(QueryOptions.ORDER, QueryOptions.ASCENDING);
    private final FileManager fileManager;


    public static final String TRANSFORM = "transform";
    public static final String LOAD = "load";

    // FIXME : Needed?
    public static final String TRANSFORMED_FILES = "transformedFiles";

    private enum Type {
        TRANSFORM,
        LOAD,
        INDEX
    }

    public VariantFileIndexerStorageOperation(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, StorageEngineFactory.get(storageConfiguration),
                LoggerFactory.getLogger(VariantFileIndexerStorageOperation.class));
        this.fileManager = catalogManager.getFileManager();
    }

    public VariantFileIndexerStorageOperation(Configuration configuration, StorageConfiguration storageConfiguration)
            throws CatalogException {
        super(new CatalogManager(configuration), StorageEngineFactory.get(storageConfiguration),
                LoggerFactory.getLogger(VariantFileIndexerStorageOperation.class));
        this.fileManager = catalogManager.getFileManager();
    }

    public List<StoragePipelineResult> index(StudyInfo studyInfo, String outdirString, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageEngineException, URISyntaxException {

        URI outdirUri = UriUtils.createDirectoryUri(outdirString);
        Path outdir = Paths.get(outdirUri);
        FileUtils.checkDirectory(outdir, true);

        // Check the output directory does not correspond with a catalog directory
        Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), outdir.toUri().toString());
        QueryResult<File> count = fileManager.count(studyInfo.getStudyFQN(), query, sessionId);
        if (count.getNumTotalResults() > 0) {
            throw new CatalogException("The output directory is pointing to one in catalog. Please, choose other out of catalog "
                    + "boundaries.");
        }

        // Outdir must be empty
        outdirMustBeEmpty(outdir, options);

        writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.RUNNING, "Job has just started"));

        // TODO: This hook should #updateFileInfo
        Thread hook = buildHook(outdir);
        Runtime.getRuntime().addShutdownHook(hook);

        if (options == null) {
            options = new QueryOptions();
        }

        boolean transform = true;
        boolean load = true;
        // If any of them is defined and at least one is different from false then we set the values
        if (options.getBoolean(TRANSFORM, false) || options.getBoolean(LOAD, false)) {
            transform = options.getBoolean(TRANSFORM, false);
            load = options.getBoolean(LOAD, false);
        }
        boolean resume = options.getBoolean(VariantStorageEngine.Options.RESUME.key());

        // Obtain the type of analysis (transform, load or index)
        Type step = getType(load, transform);


        // We read all input files from fileId. This can either be a single file and then we just use it,
        // or this can be a directory, in that case we use all VCF files in that directory or subdirectory
        String studyFQNByInputFileId = studyInfo.getStudyFQN();

        options.put(VariantStorageEngine.Options.STUDY.key(), studyFQNByInputFileId);
        options.put(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), getAggregation(studyFQNByInputFileId, options, sessionId));

//        Study study = catalogManager.getStudyManager().get(studyUidByInputFileId, new QueryOptions(), sessionId).getResult().get(0);
        Study study = studyInfo.getStudy();

        // We get the credentials of the Datastore to insert the variants
//        DataStore dataStore = getDataStore(catalogManager, studyFQNByInputFileId, File.Bioformat.VARIANT, sessionId);
        DataStore dataStore = studyInfo.getDataStores().get(File.Bioformat.VARIANT);

        // Create default cohort if needed.
        if (step.equals(Type.INDEX) || step.equals(Type.LOAD)) {
            createDefaultCohortIfNeeded(study, sessionId);
        }

        // Update Catalog from the study configuration BEFORE executing the index and fetching files from Catalog
        updateCatalogFromStudyConfiguration(sessionId, studyFQNByInputFileId, dataStore);

        List<File> inputFiles = new ArrayList<>();
//        for (Long fileIdLong : fileIds) {
        for (FileInfo fileInfo : studyInfo.getFileInfos()) {
//            long fileIdLong = fileInfo.getFileId();
            File inputFile = fileManager.get(studyInfo.getStudyFQN(), fileInfo.getPath(), FILE_GET_QUERY_OPTIONS, sessionId)
                    .first();

            if (inputFile.getType() == File.Type.FILE) {
                inputFiles.add(inputFile);
            } else {
                if (inputFile.getType() == File.Type.DIRECTORY) {
//                    if (inputFile.getPath().isEmpty()) {
//                        query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + inputFile.getPath() + "*");
//                    } else {
//                        query = new Query();
//                    }
                    query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), inputFile.getPath());
                    query.append(FileDBAdaptor.QueryParams.FORMAT.key(),
//                            Arrays.asList(File.Format.VCF, File.Format.GVCF, File.Format.AVRO));
                            Arrays.asList(File.Format.VCF, File.Format.GVCF));
                    QueryResult<File> fileQueryResult = fileManager.get(studyFQNByInputFileId, query,
                            FILE_GET_QUERY_OPTIONS, sessionId);
//                    fileQueryResult.getResult().sort(Comparator.comparing(File::getName));
                    inputFiles.addAll(fileQueryResult.getResult());
                } else {
                    throw new CatalogException(String.format("Expected file type %s or %s instead of %s",
                            File.Type.FILE, File.Type.DIRECTORY, inputFile.getType()));
                }
            }
        }

        // Check catalog path
        String catalogOutDirId = getCatalogOutdirId(studyFQNByInputFileId, options, sessionId);

        logger.debug("Index - Number of files to be indexed: {}, list of files: {}", inputFiles.size(),
                inputFiles.stream().map(File::getName).collect(Collectors.toList()));

        VariantStorageEngine variantStorageEngine = getVariantStorageEngine(dataStore);


        QueryResult<Project> projectQueryResult = catalogManager
                .getProjectManager()
                .get(studyInfo.getProjectId(),
                        new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.CURRENT_RELEASE.key()), sessionId);
        int release = projectQueryResult.first().getCurrentRelease();

        // Add species, assembly and release
        updateProjectMetadata(variantStorageEngine.getStudyConfigurationManager(), studyInfo.getOrganism(), release);

        variantStorageEngine.getOptions().putAll(options);
        boolean calculateStats = options.getBoolean(VariantStorageEngine.Options.CALCULATE_STATS.key())
                && (step.equals(Type.LOAD) || step.equals(Type.INDEX));

        String fileStatus;
        String fileStatusMessage;
        List<File> filesToIndex;
        List<URI> fileUris = new ArrayList<>(inputFiles.size());
        switch (step) {
            case INDEX:
                fileStatus = FileIndex.IndexStatus.INDEXING;
                fileStatusMessage = "Start indexing file";
                filesToIndex = filterTransformFiles(inputFiles, resume);
                for (File file : filesToIndex) {
                    fileUris.add(file.getUri());
                }
                break;
            case TRANSFORM:
                fileStatus = FileIndex.IndexStatus.TRANSFORMING;
                fileStatusMessage = "Start transforming file";
                filesToIndex = filterTransformFiles(inputFiles, resume);
                for (File file : filesToIndex) {
                    fileUris.add(file.getUri());
                }
                break;
            case LOAD:
                filesToIndex = filterLoadFiles(study.getFqn(), inputFiles, options, fileUris, resume, sessionId);
                fileStatus = FileIndex.IndexStatus.LOADING;
                fileStatusMessage = "Start loading file";
                break;
            default:
                throw new IllegalArgumentException("Unknown step '" + step + "'.");
        }

        if (filesToIndex.size() == 0) {
            logger.warn("Nothing to do.");
            return Collections.emptyList();
        }

        // Check that we are not indexing two or more files with the same name at the same time
        Set<String> fileNamesToIndexSet = new HashSet<>();
        for (File fileToIndex : filesToIndex) {
            if (!fileNamesToIndexSet.add(fileToIndex.getName())) {
                throw new CatalogException("Unable to " + step + " multiple files with the same name");
            }
        }

        String prevDefaultCohortStatus = Cohort.CohortStatus.NONE;
        if (step.equals(Type.INDEX) || step.equals(Type.LOAD)) {
            if (calculateStats) {
                prevDefaultCohortStatus = updateDefaultCohortStatus(study, Cohort.CohortStatus.CALCULATING, sessionId);
            }
        }
        // Only if we are not transforming or if a path has been passed, we will update catalog information
        if (!step.equals(Type.TRANSFORM) || catalogOutDirId != null) {
            for (File file : filesToIndex) {
                QueryResult<FileIndex> fileIndexQueryResult = fileManager.updateFileIndexStatus(file, fileStatus,
                        fileStatusMessage, release, sessionId);
                file.setIndex(fileIndexQueryResult.first());
            }
        }

        logger.info("Starting to {}", step);
        List<StoragePipelineResult> storagePipelineResults;

        // Save exception to throw at the end
        StorageEngineException exception = null;
        try {
            storagePipelineResults = variantStorageEngine.index(fileUris, outdir.toUri(), false, transform, load);
        } catch (StoragePipelineException e) {
            logger.error("Error executing " + step, e);
            storagePipelineResults = e.getResults();
            exception = e;
        } catch (StorageEngineException e) {
            logger.error("Error executing " + step, e);
            storagePipelineResults = Collections.emptyList();
            exception = e;
        } catch (RuntimeException e) {
            logger.error("Error executing " + step, e);
            storagePipelineResults = Collections.emptyList();
            exception = new StorageEngineException("Error executing " + step, e);
        }

//        logger.debug("Writing storagePipelineResults to file {}", outdir.resolve("storageETLresults"));
//        objectMapper.writer().writeValue(outdir.resolve("storageETLresults").toFile(), storagePipelineResults);

        // Only if we are not transforming or if a path has been passed, we will update catalog information
        if (!step.equals(Type.TRANSFORM) || catalogOutDirId != null) {
            boolean saveIntermediateFiles = catalogOutDirId != null;
            if (saveIntermediateFiles) {
                // Copy results to catalog
                copyResults(outdir, studyFQNByInputFileId, catalogOutDirId, sessionId);
            }
            updateFileInfo(study, filesToIndex, storagePipelineResults, outdir, release, saveIntermediateFiles, options, sessionId);
            // Restore previous cohort status. Cohort status will be read from StudyConfiguration.
            if (calculateStats && exception != null) {
                updateDefaultCohortStatus(study, prevDefaultCohortStatus, sessionId);
            }
            updateCatalogFromStudyConfiguration(sessionId, study.getFqn(), dataStore);
        }

        if (exception == null) {
            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.DONE, "Job completed"));
        } else {
            writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job with errors: " + exception.getMessage()));
        }
        Runtime.getRuntime().removeShutdownHook(hook);

        // Throw the exception!
        if (exception != null) {
            throw exception;
        }
        return storagePipelineResults;
    }

    @Override
    protected List<File> copyResults(Path tmpOutdirPath, String study, String catalogPathOutDir, String sessionId)
            throws CatalogException, IOException {
        List<File> files = super.copyResults(tmpOutdirPath, study, catalogPathOutDir, sessionId);

//        List<File> avroFiles = new ArrayList<>(files.size());
//        for (File file : files) {
//            if (File.Format.AVRO.equals(file.getFormat())) {
//                avroFiles.add(file);
//            }
//        }

        logger.info("{} files found", files.size());
        if (files.size() > 0) {
            fileManager.matchUpVariantFiles(study, files, sessionId);
        }
//
//        if (!ioManager.exists(tmpOutdirPath.toUri())) {
//            logger.warn("Output folder doesn't exist");
//            return;
//        }
//        List<URI> uriList;
//        try {
//            uriList = ioManager.listFiles(tmpOutdirPath.toUri());
//        } catch (CatalogIOException e) {
//            logger.warn("Could not obtain the URI of the files within the directory {}", tmpOutdirPath.toUri());
//            logger.error(e.getMessage());
//            throw e;
//        }
//        if (uriList.isEmpty()) {
//            try {
//                ioManager.deleteDirectory(tmpOutdirPath.toUri());
//            } catch (CatalogIOException e) {
//                if (ioManager.exists(tmpOutdirPath.toUri())) {
//                    logger.error("Could not delete empty directory {}. Error: {}", tmpOutdirPath.toUri(), e.getMessage());
//                    throw e;
//                }
//            }
//        } else {
//            logger.error("Error processing job output. Temporal job out dir is not empty. " + uriList);
//        }
        return files;
    }

    private Type getType(Boolean load, Boolean transform) {
        if (transform && load) {
            return Type.INDEX;
        }
        if (transform) {
            return Type.TRANSFORM;
        }
        return Type.LOAD;
    }

    private void updateFileInfo(Study study, List<File> filesToIndex, List<StoragePipelineResult> storagePipelineResults, Path outdir,
                                Integer release, boolean saveIntermediateFiles, QueryOptions options, String sessionId)
            throws CatalogException, IOException {

        Map<String, StoragePipelineResult> map;
        try {
            map = storagePipelineResults
                    .stream()
                    .collect(Collectors.toMap(s -> {
                        String input = s.getInput().getPath();
                        String inputFileName = Paths.get(input).getFileName().toString();
                        // Input file may be the transformed one. Convert into original file.
                        return VariantReaderUtils.getOriginalFromTransformedFile(inputFileName);
                    }, i -> i));
        } catch (IllegalStateException e) {
            throw e;
        }

        for (File indexedFile : filesToIndex) {
            // Fetch from catalog. {@link #copyResult} may modify the content
            indexedFile = catalogManager.getFileManager().get(study.getFqn(), indexedFile.getId(), null, sessionId).first();
            // Suppose that the missing results are due to errors, and those files were not indexed.
            StoragePipelineResult storagePipelineResult = map.get(indexedFile.getName());

            boolean jobFailed = storagePipelineResult == null || storagePipelineResult.getLoadError() != null
                    || storagePipelineResult.getTransformError() != null;

            boolean transformedSuccess = storagePipelineResult != null && storagePipelineResult.isTransformExecuted()
                    && storagePipelineResult.getTransformError() == null;
            boolean loadedSuccess = storagePipelineResult != null && storagePipelineResult.isLoadExecuted()
                    && storagePipelineResult.getLoadError() == null;

            String indexStatusName;
            String indexStatusMessage = null;

            if (indexedFile.getIndex() != null) {
                FileIndex index = indexedFile.getIndex();
                switch (index.getStatus().getName()) {
                    case FileIndex.IndexStatus.NONE:
                    case FileIndex.IndexStatus.TRANSFORMED:
                        indexStatusMessage = "Unexpected index status. Expected " + FileIndex.IndexStatus.TRANSFORMING + ", "
                                + FileIndex.IndexStatus.LOADING + " or " + FileIndex.IndexStatus.INDEXING + " and got "
                                + index.getStatus();
                        logger.warn(indexStatusMessage);
                    case FileIndex.IndexStatus.READY: //Do not show warn message when index status is READY.
                        indexStatusName = index.getStatus().getName();
                        break;
                    case FileIndex.IndexStatus.TRANSFORMING:
                        if (jobFailed) {
                            indexStatusMessage = "Job failed. Restoring status from " + FileIndex.IndexStatus.TRANSFORMING + " to "
                                    + FileIndex.IndexStatus.NONE;
                            logger.warn(indexStatusMessage);
                            indexStatusName = FileIndex.IndexStatus.NONE;
                        } else {
                            indexStatusMessage = "Job finished. File transformed";
                            indexStatusName = FileIndex.IndexStatus.TRANSFORMED;
                        }
                        break;
                    case FileIndex.IndexStatus.LOADING:
                        if (jobFailed) {
                            indexStatusMessage = "Job failed. Restoring status from " + FileIndex.IndexStatus.LOADING + " to "
                                    + FileIndex.IndexStatus.TRANSFORMED;
                            logger.warn(indexStatusMessage);
                            indexStatusName = FileIndex.IndexStatus.TRANSFORMED;
                        } else {
                            indexStatusMessage = "Job finished. File index ready";
                            indexStatusName = FileIndex.IndexStatus.READY;
                        }
                        break;
                    case FileIndex.IndexStatus.INDEXING:
                        if (jobFailed) {
                            // If transform was executed, restore status to Transformed.
                            if (transformedSuccess && saveIntermediateFiles) {
//                            if (transformedSuccess) {
                                indexStatusName = FileIndex.IndexStatus.TRANSFORMED;
                            } else {
                                indexStatusName = FileIndex.IndexStatus.NONE;
                            }
                            indexStatusMessage = "Job failed. Restoring status from " + FileIndex.IndexStatus.INDEXING
                                    + " to " + indexStatusName;
                            logger.warn(indexStatusMessage);
                        } else {
                            indexStatusName = FileIndex.IndexStatus.READY;
                            indexStatusMessage = "Job finished. File index ready";
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unknown Index Status " + index.getStatus().getName());
                }
            } else {
                logger.error("The execution should never get into this condition. Critical error.");
                throw new CatalogException("Critical error. Empty index parameter in file " + indexedFile.getUid());
            }

            if (transformedSuccess) {
                updateVariantFileStats(study.getFqn(), indexedFile, outdir, sessionId);
            }

            // Update storagePipelineResult
            Map<String, Object> attributes = indexedFile.getAttributes();
            attributes.put("storagePipelineResult", storagePipelineResult);
            ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
            fileManager.update(study.getFqn(), indexedFile.getPath(), params, new QueryOptions(), sessionId);

            // Update index status
            fileManager.updateFileIndexStatus(indexedFile, indexStatusName, indexStatusMessage, release, sessionId);

            boolean calculateStats = options.getBoolean(VariantStorageEngine.Options.CALCULATE_STATS.key());
            if (indexStatusName.equals(FileIndex.IndexStatus.READY) && calculateStats) {
                Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT);
                QueryResult<Cohort> queryResult = catalogManager.getCohortManager().get(study.getFqn(), query, new QueryOptions(),
                        sessionId);
                if (queryResult.getNumResults() != 0) {
                    logger.debug("Default cohort status set to READY");
                    Cohort defaultCohort = queryResult.first();
                    catalogManager.getCohortManager().setStatus(study.getFqn(), defaultCohort.getId(), Cohort.CohortStatus.READY, null,
                            sessionId);
//                    params = new ObjectMap(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), Cohort.CohortStatus.READY);
//                    catalogManager.getCohortManager().update(defaultCohort.getId(), params, new QueryOptions(), sessionId);
                }
            }
        }
    }

    /**
     * Updates the file stats from a transformed variant file.
     * Reads the stats generated on the transform step.
     *
     *
     * @param studyFqn
     * @param inputFile
     * @param outdir
     * @param sessionId
     * @throws CatalogException if a Catalog error occurs.
     */
    private void updateVariantFileStats(String studyFqn, File inputFile, Path outdir, String sessionId)
            throws CatalogException, IOException {
        if (inputFile.getBioformat().equals(File.Bioformat.VARIANT)) {
            Path metaFile = outdir.resolve(inputFile.getName() + "." + VariantReaderUtils.METADATA_FILE_FORMAT_GZ);
            if (!metaFile.toFile().exists()) {
                throw new IOException("Stats file not found.");
            }
            VariantSetStats stats;
            try {
                VariantFileMetadata fileMetadata = VariantReaderUtils.readVariantFileMetadata(metaFile, null);
                stats = fileMetadata.getStats();
            } catch (StorageEngineException e) {
                throw new CatalogException("Error reading file \"" + metaFile + "\"", e);
            }
            ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.STATS.key(), new ObjectMap(VARIANT_FILE_STATS, stats));
            fileManager.update(studyFqn, inputFile.getPath(), params, new QueryOptions(), sessionId);
        }
//        long studyId = catalogManager.getStudyIdByJobId(job.getId());
//        Query query = new Query()
//                .append(CatalogFileDBAdaptor.QueryParams.ID.key(), job.getInput())
//                .append(CatalogFileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT);
//        QueryResult<File> fileQueryResult = catalogManager.getAllFiles(studyId, query, new QueryOptions(), sessionId);
//        if (fileQueryResult.getResult().isEmpty()) {
//            return;
//        }
//        File inputFile = fileQueryResult.first();
//        if (inputFile.getBioformat().equals(File.Bioformat.VARIANT)) {
//            query = new Query()
//                    .append(CatalogFileDBAdaptor.QueryParams.ID.key(), job.getOutput())
//                    .append(CatalogFileDBAdaptor.QueryParams.NAME.key(), "~" + inputFile.getName() + ".file");
//            fileQueryResult = catalogManager.getAllFiles(studyId, query, new QueryOptions(), sessionId);
//            if (fileQueryResult.getResult().isEmpty()) {
//                return;
//            }
//
//            File variantsFile = fileQueryResult.first();
//            URI fileUri = catalogManager.getFileUri(variantsFile);
//            try (InputStream is = FileUtils.newInputStream(Paths.get(fileUri.getPath()))) {
//                VariantSource variantSource = new com.fasterxml.jackson.databind.ObjectMapper().readValue(is, VariantSource.class);
//                VariantGlobalStats stats = variantSource.stats();
//                catalogManager.modifyFile(inputFile.getId(), new ObjectMap("stats", new ObjectMap(VARIANT_STATS, stats)), sessionId);
//            } catch (IOException e) {
//                throw new CatalogException("Error reading file \"" + fileUri + "\"", e);
//            }
//        }
    }

    private Cohort createDefaultCohortIfNeeded(Study study, String sessionId) throws CatalogException {
        Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT);
        Cohort cohort = catalogManager.getCohortManager().get(study.getFqn(), query, null, sessionId).first();
        if (cohort == null) {
            return createDefaultCohort(study, sessionId);
        } else {
            return cohort;
        }
    }

    private Cohort createDefaultCohort(Study study, String sessionId) throws CatalogException {
        return catalogManager.getCohortManager().create(study.getId(), StudyEntry.DEFAULT_COHORT, Study.Type.COLLECTION,
                DEFAULT_COHORT_DESCRIPTION, Collections.emptyList(), null, null, sessionId).first();
    }

    private void updateDefaultCohortStatus(String sessionId, Study study, StorageEngineException exception) throws CatalogException {
        if (exception == null) {
            updateDefaultCohortStatus(study, Cohort.CohortStatus.READY, sessionId);
        } else {
            updateDefaultCohortStatus(study, Cohort.CohortStatus.INVALID, sessionId);
        }
    }

    private String updateDefaultCohortStatus(Study study, String status, String sessionId) throws CatalogException {
        Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT);
        Cohort defaultCohort = catalogManager.getCohortManager().get(study.getFqn(), query, new QueryOptions(), sessionId).first();
        String prevStatus = defaultCohort.getStatus().getName();

        catalogManager.getCohortManager().setStatus(study.getFqn(), defaultCohort.getId(), status, null,
                sessionId);

        return prevStatus;
    }

    /**
     * Get non transformed files.
     *
     * @param fileList Files to filter
     * @param resume If resume, get also TRANSFORMING and INDEXING files.
     * @return List of non transformed files
     */
    private List<File> filterTransformFiles(List<File> fileList, boolean resume) {
        if (fileList == null || fileList.isEmpty()) {
            return new ArrayList<>();
        }

        List<File> filteredFiles = new ArrayList<>(fileList.size());
        for (File file : fileList) {
            if (file.getStatus().getName().equals(File.FileStatus.READY) && file.getFormat().equals(File.Format.VCF)) {
                String indexStatus;
                if (file.getIndex() != null && file.getIndex().getStatus() != null && file.getIndex().getStatus().getName() != null) {
                    indexStatus = file.getIndex().getStatus().getName();
                } else {
                    indexStatus = FileIndex.IndexStatus.NONE;
                }
                switch (indexStatus) {
                    case FileIndex.IndexStatus.NONE:
                        filteredFiles.add(file);
                        break;
                    case FileIndex.IndexStatus.INDEXING:
                    case FileIndex.IndexStatus.TRANSFORMING:
                        if (!resume) {
                            logger.warn("File already being transformed. "
                                            + "We can only transform VCF files not transformed, the status is {}. "
                                            + "Do '" + VariantStorageEngine.Options.RESUME.key() + "' to continue.",
                                    indexStatus);
                        } else {
                            filteredFiles.add(file);
                        }
                        break;
                    case FileIndex.IndexStatus.TRANSFORMED:
                    case FileIndex.IndexStatus.LOADING:
                    case FileIndex.IndexStatus.READY:
                    default:
                        logger.warn("We can only transform VCF files not transformed, the status is {}",
                                indexStatus);
                        break;
                }
            }
        }
        return filteredFiles;
    }

    private List<File> filterLoadFiles(String studyFQN, List<File> fileList, QueryOptions options, List<URI> fileUris,
                                       boolean resume, String sessionId)
            throws CatalogException, URISyntaxException {
        if (fileList == null || fileList.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> transformedFiles;
        if (options.get(TRANSFORMED_FILES) != null) {
            transformedFiles = options.getAsStringList(TRANSFORMED_FILES);
            if (transformedFiles.size() != fileList.size()) {
                throw new CatalogException("The number of transformed files (" + transformedFiles.size() + ") passed does not match the"
                        + " number of vcf files (" + fileList.size() + ").");
            }
            // Check all the files are accessible and are actually files
            for (String transformedFile : transformedFiles) {
                java.io.File file = Paths.get(transformedFile).toFile();
                if (!file.exists() || !file.isFile()) {
                    throw new CatalogException("File " + file + " does not exist or it is not an avro file");
                }
            }
        } else {
            transformedFiles = null;
        }

        List<File> filteredFiles = new ArrayList<>(fileList.size());
        Map<Long, Long> transformedToOrigFileIdsMap = new HashMap<>();
        for (int i = 0; i < fileList.size(); i++) {
            File file = fileList.get(i);
            File transformed = null;

            // If is a transformed file, get the related VCF file
            if (VariantReaderUtils.isTransformedVariants(file.getName())) {
                if (transformedFiles != null) {
                    // Check if the uri is different
                    URI avroUri = UriUtils.createUri(transformedFiles.get(i));
                    if (!avroUri.equals(file.getUri())) {
                        // This case should never happen. If we are here, it means that the input file was already an avro file and that
                        // a list of avro uris have been found. However, the uri of the avro does not match the uri we have for the avro
                        // file. Nonsense.
                        throw new CatalogException("The avro file " + file.getName() + " in " + file.getUri() + " has been associated a "
                                + "different uri " + avroUri + " by the user.");
                    }
                }
                transformed = file;
                file = getOriginalFromTransformed(studyFQN, file, sessionId);
            }

            if (file.getFormat().equals(File.Format.VCF) || file.getFormat().equals(File.Format.GVCF)) {
                String status = file.getIndex() == null || file.getIndex().getStatus() == null ? FileIndex.IndexStatus.NONE
                        : file.getIndex().getStatus().getName();
                switch (status) {
                    case FileIndex.IndexStatus.NONE:
                        if (transformedFiles != null) {
                            filteredFiles.add(file);
                            fileUris.add(UriUtils.createUri(transformedFiles.get(i)));
                        } else {
                            logger.warn("Cannot load vcf file " + file.getName() + " if no avro file is provided.");
                        }
                        break;
                    case FileIndex.IndexStatus.INDEXING:
                    case FileIndex.IndexStatus.LOADING:
                        if (!resume) {
                            logger.warn("Unable to load this file. Already being loaded. Skipping file {}", file.getName());
                            break;
                        }
                    case FileIndex.IndexStatus.TRANSFORMED:
                        // We will attempt to use the avro file registered in catalog
                        if (transformed == null) {
                            // Don't query file by file. Make one single call at the end
                            transformedToOrigFileIdsMap.put(getTransformedFileIdFromOriginal(file), file.getUid());
                        } else {
                            fileUris.add(transformed.getUri());
                        }
                        filteredFiles.add(file);
                        break;
                    case FileIndex.IndexStatus.TRANSFORMING:
                        logger.warn("We can only load files previously transformed. Skipping file {}", file.getName());
                        break;
                    case FileIndex.IndexStatus.READY:
                        logger.warn("Already loaded file. Skipping file {}", file.getName());
                        break;
                    default:
                        logger.warn("We can only load files previously transformed, File {} with status is {}", file.getName(), status);
                        break;
                }
            } else {
                logger.warn("The input file is not a variant file. Format {}", file.getFormat());
            }

        }
        if (!transformedToOrigFileIdsMap.isEmpty()) {
            Query query = new Query(UID.key(), new ArrayList<>(transformedToOrigFileIdsMap.keySet()));
            Set<Long> foundTransformedFiles = new HashSet<>();
            fileManager.iterator(studyFQN, query, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(UID.key(), FileDBAdaptor.QueryParams.URI.key())), sessionId)
                    .forEachRemaining(transformed -> {
                        foundTransformedFiles.add(transformed.getUid());
                        fileUris.add(transformed.getUri());
                        //if (transformedFiles != null) {
                        //    // Check that the uri from the avro file obtained from catalog is the same the user has put as input
                        //    URI uri = UriUtils.createUri(transformedFiles.get(i));
                        //    if (!uri.equals(transformed.getUri())) {
                        //        throw new CatalogException("A transformed file was found for file " + file.getId() + " in "
                        //                + transformed.getUri() + ". However, the user selected a different one in " + uri);
                        //    }
                        //}
                    });
            if (foundTransformedFiles.size() != transformedToOrigFileIdsMap.size()) {
                for (Long foundTransformedFile : foundTransformedFiles) {
                    transformedToOrigFileIdsMap.remove(foundTransformedFile);
                }
                throw new CatalogException("Internal error. No transformed file could be found for files "
                        + transformedToOrigFileIdsMap.values());
            }
        }


        return filteredFiles;
    }

    private File getOriginalFromTransformed(String study, File file, String sessionId)
            throws URISyntaxException, CatalogException {
        // Look for the vcf file
        String vcfId = null;
        // Matchup variant files, if missing
        if (file.getRelatedFiles() == null || file.getRelatedFiles().isEmpty()) {
            catalogManager.getFileManager().matchUpVariantFiles(null, Collections.singletonList(file), sessionId);
        }
        for (File.RelatedFile relatedFile : file.getRelatedFiles()) {
            if (File.RelatedFile.Relation.PRODUCED_FROM.equals(relatedFile.getRelation())) {
                long fileUid = relatedFile.getFileId();
                // FIXME!!!
                vcfId = catalogManager.getFileManager().get(study,
                        new Query(UID.key(), fileUid),
                        new QueryOptions(QueryOptions.INCLUDE, ID.key()), sessionId).first().getId();
                break;
            }
        }
        if (vcfId == null) {
            logger.error("This code should never be executed. Every transformed avro file should come from a registered vcf file");
            throw new CatalogException("Internal error. No vcf file could be found for file " + file.getPath());
        }
        QueryResult<File> vcfQueryResult = fileManager.get(study, vcfId, FILE_GET_QUERY_OPTIONS, sessionId);
        if (vcfQueryResult.getNumResults() != 1) {
            logger.error("This code should never be executed. No vcf file could be found for vcf id " + vcfId);
            throw new CatalogException("Internal error. No vcf file could be found under id " + vcfId);
        }
        file = vcfQueryResult.first();
        return file;
    }

    private File getTransformedFromOriginal(String sessionId, File file)
            throws CatalogException, URISyntaxException {
        long transformedFileId = getTransformedFileIdFromOriginal(file);
        QueryResult<File> queryResult = fileManager.get(transformedFileId, FILE_GET_QUERY_OPTIONS, sessionId);
        if (queryResult.getNumResults() != 1) {
            logger.error("This code should never be executed. No transformed file could be found under ");
            throw new CatalogException("Internal error. No transformed file could be found under id " + transformedFileId);
        }

        return queryResult.first();
    }

    private long getTransformedFileIdFromOriginal(File file) throws CatalogException {
        long transformedFile = file.getIndex() != null && file.getIndex().getTransformedFile() != null
                ? file.getIndex().getTransformedFile().getId()
                : -1;
        if (transformedFile == -1) {
            logger.error("This code should never be executed. Every vcf file containing the transformed status should have"
                    + " a registered transformed file");
            throw new CatalogException("Internal error. No transformed file could be found for file " + file.getUid());
        }
        return transformedFile;
    }


}
