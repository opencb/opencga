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

package org.opencb.opencga.analysis.variant.manager.operations;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.analysis.variant.operations.OperationTool;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortStatus;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.ANNOTATION_SETS;
import static org.opencb.opencga.catalog.db.api.FileDBAdaptor.QueryParams.UID;
import static org.opencb.opencga.catalog.db.api.ProjectDBAdaptor.QueryParams.CURRENT_RELEASE;
import static org.opencb.opencga.catalog.db.api.ProjectDBAdaptor.QueryParams.ORGANISM;
import static org.opencb.opencga.catalog.utils.FileMetadataReader.FILE_VARIANT_STATS_VARIABLE_SET;

/**
 * Created by imedina on 17/08/16.
 */
public class VariantFileIndexerOperationManager extends OperationManager {

    public static final String DEFAULT_COHORT_DESCRIPTION = "Default cohort with almost all indexed samples";
    public static final QueryOptions FILE_GET_QUERY_OPTIONS = new QueryOptions()
            .append(QueryOptions.EXCLUDE, Arrays.asList(
                    FileDBAdaptor.QueryParams.ATTRIBUTES.key(),
                    FileDBAdaptor.QueryParams.STATS.key()))
            .append(QueryOptions.SORT, FileDBAdaptor.QueryParams.NAME.key())
            .append(QueryOptions.ORDER, QueryOptions.ASCENDING);

    public static final String TRANSFORM = "transform";
    public static final String LOAD = "load";
    // FIXME : Needed?
    public static final String TRANSFORMED_FILES = "transformedFiles";

    private final Logger logger;

    private String studyFqn;
    private List<String> files;

    private boolean calculateStats;
    private boolean transform;
    private boolean load;
    private boolean resume;
    private boolean keepIntermediateFiles;
    private Type step;
    private URI outDirUri;
    private int release;
    private List<File> filesToIndex;
    private CatalogStorageMetadataSynchronizer synchronizer;

    public VariantFileIndexerOperationManager(VariantStorageManager variantStorageManager, VariantStorageEngine engine) {
        super(variantStorageManager, engine);
        logger = LoggerFactory.getLogger(VariantFileIndexerOperationManager.class);
    }

    private enum Type {
        // AUTO, // TODO
        TRANSFORM,
        LOAD,
        INDEX
    }


    public List<StoragePipelineResult> index(String study, List<String> files, URI outDirUri, ObjectMap params, String token)
            throws Exception {
        this.files = files;
        this.outDirUri = outDirUri;
        check(study, params, token);

        updateProject(studyFqn, token);

        List<URI> fileUris = findFilesToIndex(params, token);
        if (fileUris.size() == 0) {
            logger.warn("Nothing to do.");
            return Collections.emptyList();
        }
        
        return indexFiles(fileUris, token, params);
    }

    private void check(String study, ObjectMap params, String token) throws Exception {
        studyFqn = getStudyFqn(study, token);

        this.keepIntermediateFiles = params.getBoolean(OperationTool.KEEP_INTERMEDIATE_FILES);

        transform = true;
        load = true;
        // If any of them is defined and at least one is different from false then we set the values
        if (params.getBoolean(TRANSFORM, false) || params.getBoolean(LOAD, false)) {
            transform = params.getBoolean(TRANSFORM, false);
            load = params.getBoolean(LOAD, false);
        }
        resume = params.getBoolean(VariantStorageOptions.RESUME.key());

        // Obtain the type of analysis (transform, load or index)
        step = getType(load, transform);

        params.put(VariantStorageOptions.STUDY.key(), studyFqn);
        Aggregation aggregation = VariantStatsAnalysis.getAggregation(catalogManager, studyFqn, params, token);
        params.putIfAbsent(VariantStorageOptions.STATS_AGGREGATION.key(), aggregation);
        calculateStats = params.getBoolean(VariantStorageOptions.STATS_CALCULATE.key())
                && (step.equals(Type.LOAD) || step.equals(Type.INDEX));

        // Create default cohort if needed.
        if (step.equals(Type.INDEX) || step.equals(Type.LOAD)) {
            createDefaultCohortIfNeeded(studyFqn, token);
        }

        toFileSystemPath(studyFqn, params, VariantStorageOptions.NORMALIZATION_REFERENCE_GENOME.key(), token);
        toFileSystemPath(studyFqn, params, VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), token);

        variantStorageEngine.getOptions().putAll(params);
    }

    private void updateProject(String studyFqn, String token) throws CatalogException, StorageEngineException {
        String projectFqn = catalogManager.getStudyManager().getProjectFqn(studyFqn);
        Project project = catalogManager
                .getProjectManager()
                .get(projectFqn,
                        new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(CURRENT_RELEASE.key(), ORGANISM.key())), token).first();
        release = project.getCurrentRelease();

        // Add species, assembly and release
        CatalogStorageMetadataSynchronizer.updateProjectMetadata(variantStorageEngine.getMetadataManager(), project.getOrganism(), release);
    }

    /**
     * We read all input files from fileId. This can either be a single file and then we just use it,
     * or this can be a directory, in that case we use all VCF files in that directory or subdirectory
     * @param token
     * @return
     * @throws CatalogException
     * @throws URISyntaxException
     * @throws StorageEngineException
     */
    private List<URI> findFilesToIndex(ObjectMap params, String token)
            throws CatalogException, URISyntaxException, StorageEngineException {
        synchronizer = new CatalogStorageMetadataSynchronizer(catalogManager, variantStorageEngine.getMetadataManager());

        List<File> inputFiles = new ArrayList<>();
        for (String file : files) {
            File inputFile = catalogManager.getFileManager().get(studyFqn, file, FILE_GET_QUERY_OPTIONS, token).first();

            if (inputFile.getType() == File.Type.FILE) {
                // If is a transformed file, get the related VCF file
                if (VariantReaderUtils.isTransformedVariants(inputFile.getName())) {
                    inputFiles.add(getOriginalFromTransformed(studyFqn, inputFile, token));
                } else {
                    inputFiles.add(inputFile);
                }
            } else {
                if (inputFile.getType() == File.Type.DIRECTORY) {
                    Query query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), inputFile.getPath());
                    query.append(FileDBAdaptor.QueryParams.FORMAT.key(),
//                            Arrays.asList(File.Format.VCF, File.Format.GVCF, File.Format.AVRO));
                            Arrays.asList(File.Format.VCF, File.Format.GVCF));
                    DataResult<File> fileDataResult = catalogManager.getFileManager().search(studyFqn, query, FILE_GET_QUERY_OPTIONS, token);
//                    fileDataResult.getResults().sort(Comparator.comparing(File::getName));
                    inputFiles.addAll(fileDataResult.getResults());
                } else {
                    throw new CatalogException(String.format("Expected file type %s or %s instead of %s",
                            File.Type.FILE, File.Type.DIRECTORY, inputFile.getType()));
                }
            }
        }


        // Update Catalog from the storage metadata. This may change the index status of the inputFiles .
        synchronizer.synchronizeCatalogFilesFromStorage(studyFqn, inputFiles, token, FILE_GET_QUERY_OPTIONS);

        logger.debug("Index - Number of files to be indexed: {}, list of files: {}", inputFiles.size(),
                inputFiles.stream().map(File::getName).collect(Collectors.toList()));

        String fileStatus;
        String fileStatusMessage;
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
                filesToIndex = filterLoadFiles(studyFqn, inputFiles, params, fileUris, resume, token);
                fileStatus = FileIndex.IndexStatus.LOADING;
                fileStatusMessage = "Start loading file";
                break;
            default:
                throw new IllegalArgumentException("Unknown step '" + step + "'.");
        }


        // Check that we are not indexing two or more files with the same name at the same time
        Set<String> fileNamesToIndexSet = new HashSet<>();
        for (File fileToIndex : filesToIndex) {
            if (!fileNamesToIndexSet.add(fileToIndex.getName())) {
                throw new CatalogException("Unable to " + step + " multiple files with the same name");
            }
        }

        // Only if we are not transforming or if a path has been passed, we will update catalog information
        if (!step.equals(Type.TRANSFORM) || keepIntermediateFiles) {
            for (File file : filesToIndex) {
                DataResult<FileIndex> fileIndexDataResult = catalogManager.getFileManager().updateFileIndexStatus(file, fileStatus,
                        fileStatusMessage, release, token);
                file.getInternal().setIndex(fileIndexDataResult.first());
            }
        }
        return fileUris;
    }

    private List<StoragePipelineResult> indexFiles(List<URI> fileUris, String token, ObjectMap params) throws Exception {

        String prevDefaultCohortStatus = CohortStatus.NONE;
        if (step.equals(Type.INDEX) || step.equals(Type.LOAD)) {
            if (calculateStats) {
                prevDefaultCohortStatus = updateDefaultCohortStatus(studyFqn, CohortStatus.CALCULATING, token);
            }
        }

        logger.info("Starting to {}", step);

        // Save exception to throw at the end
        StorageEngineException exception = null;
        List<StoragePipelineResult> storagePipelineResults = Collections.emptyList();
        try {
            storagePipelineResults = variantStorageEngine.index(fileUris, outDirUri, false, transform, load);
        } catch (StoragePipelineException e) {
            logger.error("Error executing " + step, e);
            storagePipelineResults = e.getResults();
            exception = e;
            throw e;
        } catch (StorageEngineException e) {
            logger.error("Error executing " + step, e);
            storagePipelineResults = Collections.emptyList();
            exception = e;
            throw e;
        } catch (RuntimeException e) {
            logger.error("Error executing " + step, e);
            storagePipelineResults = Collections.emptyList();
            exception = new StorageEngineException("Error executing " + step, e);
            throw e;
        } finally {
            // Only if we are not transforming or if a path has been passed, we will update catalog information
            if (!step.equals(Type.TRANSFORM) || keepIntermediateFiles) {

                updateFileInfo(studyFqn, filesToIndex, variantStorageEngine.getVariantReaderUtils(),
                        storagePipelineResults, Paths.get(outDirUri), release, keepIntermediateFiles, params, token);

                // Restore previous cohort status. Cohort status will be read from StudyConfiguration.
                if (calculateStats && exception != null) {
                    updateDefaultCohortStatus(studyFqn, prevDefaultCohortStatus, token);
                }
                synchronizer.synchronizeCatalogStudyFromStorage(studyFqn, token);
            }
            variantStorageEngine.close();
        }
        return storagePipelineResults;
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

    private void updateFileInfo(String study, List<File> filesToIndex, VariantReaderUtils variantReaderUtils,
                                List<StoragePipelineResult> storagePipelineResults, Path outdir,
                                Integer release, boolean saveIntermediateFiles, ObjectMap options, String sessionId)
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
            indexedFile = catalogManager.getFileManager().get(study, indexedFile.getId(), null, sessionId).first();
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

            if (indexedFile.getInternal().getIndex() != null) {
                FileIndex index = indexedFile.getInternal().getIndex();
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
                            if (indexedFile.getInternal().getIndex().getTransformedFile() == null) {
                                indexStatusName = FileIndex.IndexStatus.NONE;
                            } else {
                                indexStatusName = FileIndex.IndexStatus.TRANSFORMED;
                            }
                            indexStatusMessage = "Job failed. Restoring status from " + FileIndex.IndexStatus.LOADING + " to "
                                    + indexStatusName;
                            logger.warn(indexStatusMessage);
                        } else {
                            indexStatusMessage = "Job finished. File index ready";
                            indexStatusName = FileIndex.IndexStatus.READY;
                        }
                        break;
                    case FileIndex.IndexStatus.INDEXING:
                        if (jobFailed) {
                            // If transform was executed, restore status to Transformed.
                            if (transformedSuccess && saveIntermediateFiles) {
                                indexStatusName = FileIndex.IndexStatus.TRANSFORMED;
                            } else if (indexedFile.getInternal().getIndex().getTransformedFile() != null) {
                                // If transform file already exists, restore to Transformed
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
                updateVariantFileStats(study, variantReaderUtils, indexedFile, outdir, sessionId);
            }

            // Update storagePipelineResult
            Map<String, Object> attributes = indexedFile.getAttributes();
            attributes.put("storagePipelineResult", storagePipelineResult);
            FileUpdateParams updateParams = new FileUpdateParams().setAttributes(attributes);
            catalogManager.getFileManager().update(study, indexedFile.getPath(), updateParams, new QueryOptions(), sessionId);

            // Update index status
            catalogManager.getFileManager().updateFileIndexStatus(indexedFile, indexStatusName, indexStatusMessage, release, sessionId);

            boolean calculateStats = options.getBoolean(VariantStorageOptions.STATS_CALCULATE.key());
            if (indexStatusName.equals(FileIndex.IndexStatus.READY) && calculateStats) {
                Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT);
                DataResult<Cohort> queryResult = catalogManager.getCohortManager()
                        .search(study, query, new QueryOptions(), sessionId);
                if (queryResult.getNumResults() != 0) {
                    logger.debug("Default cohort status set to READY");
                    Cohort defaultCohort = queryResult.first();
                    catalogManager.getCohortManager().setStatus(study, defaultCohort.getId(), CohortStatus.READY, null,
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
     * @param variantReaderUtils
     * @param inputFile
     * @param outdir
     * @param token
     * @throws CatalogException if a Catalog error occurs.
     */
    private void updateVariantFileStats(String studyFqn, VariantReaderUtils variantReaderUtils, File inputFile, Path outdir,
                                        String token)
            throws CatalogException, IOException {
        Path metaFile = outdir.resolve(inputFile.getName() + "." + VariantReaderUtils.METADATA_FILE_FORMAT_GZ);
        if (!metaFile.toFile().exists()) {
            throw new IOException("Stats file not found.");
        }
        VariantSetStats stats;
        try {
            VariantFileMetadata fileMetadata = variantReaderUtils.readVariantFileMetadata(metaFile, null);
            stats = fileMetadata.getStats();
        } catch (StorageEngineException e) {
            throw new CatalogException("Error reading file \"" + metaFile + "\"", e);
        }

        try {
            catalogManager.getStudyManager().getVariableSet(studyFqn, FILE_VARIANT_STATS_VARIABLE_SET, new QueryOptions(), token);
        } catch (CatalogException e) {
            // Variable set not found. Try to create
            catalogManager.getStudyManager().createDefaultVariableSets(studyFqn, token);
        }

        AnnotationSet annotationSet = AvroToAnnotationConverter.convertToAnnotationSet(stats, FILE_VARIANT_STATS_VARIABLE_SET);
        catalogManager.getFileManager()
                .update(studyFqn, inputFile.getPath(), new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet)),
                        new QueryOptions(Constants.ACTIONS, Collections.singletonMap(ANNOTATION_SETS.key(), ParamUtils.BasicUpdateAction.SET)),
                        token);

    }

    private Cohort createDefaultCohortIfNeeded(String studyFqn, String sessionId) throws CatalogException {
        Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT);
        Cohort cohort = catalogManager.getCohortManager().search(studyFqn, query, null, sessionId).first();
        if (cohort == null) {
            try {
                return createDefaultCohort(studyFqn, sessionId);
            } catch (CatalogDBException e) {
                if (e.getMessage().contains("already exists")) {
                    cohort = catalogManager.getCohortManager().search(studyFqn, query, null, sessionId).first();
                    if (cohort == null) {
                        throw e;
                    } else {
                        // Do not fail when concurrent cohort creation.
                        return cohort;
                    }
                } else {
                    throw e;
                }
            }
        } else {
            return cohort;
        }
    }

    private Cohort createDefaultCohort(String studyFqn, String sessionId) throws CatalogException {
        return catalogManager.getCohortManager().create(studyFqn, StudyEntry.DEFAULT_COHORT, Enums.CohortType.COLLECTION,
                DEFAULT_COHORT_DESCRIPTION, Collections.emptyList(), null, null, sessionId).first();
    }

    private void updateDefaultCohortStatus(Study study, StorageEngineException exception, String sessionId) throws CatalogException {
        if (exception == null) {
            updateDefaultCohortStatus(study.getFqn(), CohortStatus.READY, sessionId);
        } else {
            updateDefaultCohortStatus(study.getFqn(), CohortStatus.INVALID, sessionId);
        }
    }

    private String updateDefaultCohortStatus(String study, String status, String sessionId) throws CatalogException {
        Query query = new Query(CohortDBAdaptor.QueryParams.ID.key(), StudyEntry.DEFAULT_COHORT);
        Cohort defaultCohort = catalogManager.getCohortManager().search(study, query, new QueryOptions(), sessionId).first();
        String prevStatus = defaultCohort.getInternal().getStatus().getName();

        catalogManager.getCohortManager().setStatus(study, defaultCohort.getId(), status, null,
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
            if (file.getInternal().getStatus().getName().equals(FileStatus.READY) && OperationManager.isVcfFormat(file)) {
                String indexStatus;
                if (file.getInternal().getIndex() != null && file.getInternal().getIndex().getStatus() != null && file.getInternal().getIndex().getStatus().getName() != null) {
                    indexStatus = file.getInternal().getIndex().getStatus().getName();
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
                                            + "Do '" + VariantStorageOptions.RESUME.key() + "' to continue.",
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
            } else {
                logger.warn("Skip file " + file.getName() + " with format " + file.getFormat() + " and status "
                        + file.getInternal().getStatus().getName());
            }
        }
        return filteredFiles;
    }

    private List<File> filterLoadFiles(String studyFQN, List<File> fileList, ObjectMap options, List<URI> fileUris,
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

            if (OperationManager.isVcfFormat(file)) {
                String status = file.getInternal().getIndex() == null || file.getInternal().getIndex().getStatus() == null ? FileIndex.IndexStatus.NONE
                        : file.getInternal().getIndex().getStatus().getName();
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
            catalogManager.getFileManager().iterator(studyFQN, query, new QueryOptions(QueryOptions.INCLUDE,
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
            throws CatalogException {
        // Look for the vcf file
        String vcfId = null;
        // Matchup variant files, if missing
        if (file.getRelatedFiles() == null || file.getRelatedFiles().isEmpty()) {
            catalogManager.getFileManager().matchUpVariantFiles(study, Collections.singletonList(file), sessionId);
        }
        for (FileRelatedFile relatedFile : file.getRelatedFiles()) {
            if (FileRelatedFile.Relation.PRODUCED_FROM.equals(relatedFile.getRelation())) {
                long fileUid = relatedFile.getFile().getUid();
                // FIXME!!!
                vcfId = catalogManager.getFileManager().search(study, new Query(UID.key(), fileUid),
                        new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key()), sessionId).first().getId();
                break;
            }
        }
        if (vcfId == null) {
            logger.error("This code should never be executed. Every transformed avro file should come from a registered vcf file");
            throw new CatalogException("Internal error. No vcf file could be found for file " + file.getPath());
        }
        DataResult<File> vcfDataResult = catalogManager.getFileManager().get(study, vcfId, FILE_GET_QUERY_OPTIONS, sessionId);
        if (vcfDataResult.getNumResults() != 1) {
            logger.error("This code should never be executed. No vcf file could be found for vcf id " + vcfId);
            throw new CatalogException("Internal error. No vcf file could be found under id " + vcfId);
        }
        file = vcfDataResult.first();
        return file;
    }

    private File getTransformedFromOriginal(String sessionId, File file)
            throws CatalogException {
        long transformedFileId = getTransformedFileIdFromOriginal(file);
        DataResult<File> queryResult = catalogManager.getFileManager().get(transformedFileId, FILE_GET_QUERY_OPTIONS, sessionId);
        if (queryResult.getNumResults() != 1) {
            logger.error("This code should never be executed. No transformed file could be found under ");
            throw new CatalogException("Internal error. No transformed file could be found under id " + transformedFileId);
        }

        return queryResult.first();
    }

    private long getTransformedFileIdFromOriginal(File file) throws CatalogException {
        long transformedFile = file.getInternal().getIndex() != null && file.getInternal().getIndex().getTransformedFile() != null
                ? file.getInternal().getIndex().getTransformedFile().getId()
                : -1;
        if (transformedFile == -1) {
            logger.error("This code should never be executed. Every vcf file containing the transformed status should have"
                    + " a registered transformed file");
            throw new CatalogException("Internal error. No transformed file could be found for file " + file.getUid());
        }
        return transformedFile;
    }


}
