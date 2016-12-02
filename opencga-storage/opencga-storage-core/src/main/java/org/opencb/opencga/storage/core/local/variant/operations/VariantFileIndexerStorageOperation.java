/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.local.variant.operations;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.api.IFileManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.local.models.FileInfo;
import org.opencb.opencga.storage.core.local.models.StudyInfo;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.utils.FileMetadataReader.VARIANT_STATS;

/**
 * Created by imedina on 17/08/16.
 */
public class VariantFileIndexerStorageOperation extends StorageOperation {

    private final IFileManager fileManager;


    public static final String TRANSFORM = "transform";
    public static final String LOAD = "load";

    public static final String CATALOG_PATH = "catalogPath";
    // FIXME : Needed?
    public static final String TRANSFORMED_FILES = "transformedFiles";

    private enum Type {
        TRANSFORM,
        LOAD,
        INDEX
    }

    public VariantFileIndexerStorageOperation(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, StorageManagerFactory.get(storageConfiguration),
                LoggerFactory.getLogger(VariantFileIndexerStorageOperation.class));
        this.fileManager = catalogManager.getFileManager();
    }

    public VariantFileIndexerStorageOperation(CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration)
            throws CatalogException {
        super(new CatalogManager(catalogConfiguration), StorageManagerFactory.get(storageConfiguration),
                LoggerFactory.getLogger(VariantFileIndexerStorageOperation.class));
        this.fileManager = catalogManager.getFileManager();
    }

    public List<StorageETLResult> index(StudyInfo studyInfo, String outdirString, QueryOptions options, String sessionId)
            throws CatalogException, IOException, StorageManagerException, URISyntaxException {

        URI outdirUri = UriUtils.createDirectoryUri(outdirString);
        Path outdir = Paths.get(outdirUri);
        FileUtils.checkDirectory(outdir, true);

        // Check the output directory does not correspond with a catalog directory
        Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), outdir.toUri().toString());
        QueryResult<Long> count = fileManager.count(query, sessionId);
        if (count.first() > 0) {
            throw new CatalogException("The output directory is pointing to one in catalog. Please, choose other out of catalog "
                    + "boundaries.");
        }

        // Outdir must be empty
        outdirMustBeEmpty(outdir);

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

        // Obtain the type of analysis (transform, load or index)
        Type step = getType(load, transform);


        // We read all input files from fileId. This can either be a single file and then we just use it,
        // or this can be a directory, in that case we use all VCF files in that directory or subdirectory
//        long studyIdByInputFileId = getStudyId(fileIds);
        long studyIdByInputFileId = studyInfo.getStudyId();

//        Study study = catalogManager.getStudyManager().get(studyIdByInputFileId, new QueryOptions(), sessionId).getResult().get(0);
        Study study = studyInfo.getStudy();

        // We get the credentials of the Datastore to insert the variants
//        DataStore dataStore = getDataStore(catalogManager, studyIdByInputFileId, File.Bioformat.VARIANT, sessionId);
        DataStore dataStore = studyInfo.getDataStores().get(File.Bioformat.VARIANT);

        // Update study configuration BEFORE executing the index and fetching files from Catalog
        updateStudyConfiguration(sessionId, studyIdByInputFileId, dataStore);

        List<File> inputFiles = new ArrayList<>();
//        for (Long fileIdLong : fileIds) {
        for (FileInfo fileInfo : studyInfo.getFileInfos()) {
            long fileIdLong = fileInfo.getFileId();
            File inputFile = fileManager.get(fileIdLong, new QueryOptions(), sessionId).first();

            if (inputFile.getType() == File.Type.FILE) {
                inputFiles.add(inputFile);
            } else {
                if (inputFile.getType() == File.Type.DIRECTORY) {
                    query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + inputFile.getPath() + "*");
                    query.append(FileDBAdaptor.QueryParams.FORMAT.key(),
//                            Arrays.asList(File.Format.VCF, File.Format.GVCF, File.Format.AVRO));
                            Arrays.asList(File.Format.VCF, File.Format.GVCF));
                    QueryResult<File> fileQueryResult = fileManager.get(studyIdByInputFileId, query, options, sessionId);
                    inputFiles.addAll(fileQueryResult.getResult());
                } else {
                    throw new CatalogException(String.format("Expected file type %s or %s instead of %s",
                            File.Type.FILE, File.Type.DIRECTORY, inputFile.getType()));
                }
            }
        }

        // Check catalog path
        Long catalogOutDirId = getCatalogOutdirId(studyIdByInputFileId, options.getString(CATALOG_PATH), sessionId);

        logger.debug("Index - Number of files to be indexed: {}, list of files: {}", inputFiles.size(),
                inputFiles.stream().map(File::getName).collect(Collectors.toList()));

        options.put(VariantStorageManager.Options.DB_NAME.key(), dataStore.getDbName());
        options.put(VariantStorageManager.Options.STUDY_ID.key(), studyIdByInputFileId);

        VariantStorageManager variantStorageManager;
        try {
            variantStorageManager = storageManagerFactory.getVariantStorageManager(dataStore.getStorageEngine());
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new StorageManagerException("Unable to create StorageManager", e);
        }
        variantStorageManager.getOptions().putAll(options);
        boolean calculateStats = options.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key())
                && (step.equals(Type.LOAD) || step.equals(Type.INDEX));

        String fileStatus;
        String fileStatusMessage;
        List<File> filesToIndex;
        List<URI> fileUris = new ArrayList<>(inputFiles.size());
        if (step.equals(Type.INDEX)) {
            fileStatus = FileIndex.IndexStatus.INDEXING;
            fileStatusMessage = "Start indexing file";
            filesToIndex = filterTransformFiles(inputFiles);
            for (File file : filesToIndex) {
                fileUris.add(file.getUri());
            }
        } else if (step.equals(Type.TRANSFORM)) {
            fileStatus = FileIndex.IndexStatus.TRANSFORMING;
            fileStatusMessage = "Start transforming file";
            filesToIndex = filterTransformFiles(inputFiles);
            for (File file : filesToIndex) {
                fileUris.add(file.getUri());
            }
        } else {
            filesToIndex = filterLoadFiles(inputFiles, options, fileUris, sessionId);
            fileStatus = FileIndex.IndexStatus.LOADING;
            fileStatusMessage = "Start loading file";
        }

        if (filesToIndex.size() == 0) {
            logger.warn("Nothing to do.");
            return Collections.emptyList();
        }

        if (step.equals(Type.INDEX) || step.equals(Type.LOAD)) {
            boolean modified = false;
            for (File file : filesToIndex) {
                modified |= updateDefaultCohort(file, study, options, sessionId);
            }
            if (calculateStats) {
                updateDefaultCohortStatus(study, Cohort.CohortStatus.CALCULATING, sessionId);
            }
            if (modified) {
                // Update again the StudyConfiguration.
                updateStudyConfiguration(sessionId, study.getId(), dataStore);
            }
        }
        // Only if we are not transforming or if a path has been passed, we will update catalog information
        List<String> previousFileStatus = new ArrayList<>(filesToIndex.size());
        if (!step.equals(Type.TRANSFORM) || catalogOutDirId != null) {
            for (File file : filesToIndex) {
                previousFileStatus.add(file.getIndex().getStatus().getName());
                QueryResult<FileIndex> fileIndexQueryResult = fileManager.updateFileIndexStatus(file, fileStatus,
                        fileStatusMessage, sessionId);
                file.setIndex(fileIndexQueryResult.first());
            }
        }


        logger.info("Starting to {}", step);
        List<StorageETLResult> storageETLResults;

//        try {
//            storageETLResults = variantStorageManager.index(fileUris, outdir.toUri(), false, transform, load);
//        } catch(StorageManagerException e) {
//            // Restore previous status
//            if (!step.equals(Type.TRANSFORM) || options.get(CATALOG_PATH) != null) {
//                for (int i = 0; i < filesToIndex.size(); i++) {
//                    File file = filesToIndex.get(i);
//                    fileManager.updateFileIndexStatus(file, previousFileStatus.get(i), sessionId);
//                }
//            }
//            throw e;
//        }

        // Save exception to throw at the end
        StorageManagerException exception = null;
        try {
            storageETLResults = variantStorageManager.index(fileUris, outdir.toUri(), false, transform, load);
        } catch (StorageETLException e) {
            logger.error("Error executing " + step, e);
            storageETLResults = e.getResults();
            exception = e;
        } catch (StorageManagerException e) {
            logger.error("Error executing " + step, e);
            storageETLResults = Collections.emptyList();
            exception = e;
        } catch (RuntimeException e) {
            logger.error("Error executing " + step, e);
            storageETLResults = Collections.emptyList();
            exception = new StorageManagerException("Error executing " + step, e);
        }

//        logger.debug("Writing storageETLResults to file {}", outdir.resolve("storageETLresults"));
//        objectMapper.writer().writeValue(outdir.resolve("storageETLresults").toFile(), storageETLResults);

        // Only if we are not transforming or if a path has been passed, we will update catalog information
        if (!step.equals(Type.TRANSFORM) || catalogOutDirId != null) {
            if (catalogOutDirId != null) {
                // Copy results to catalog
                copyResults(outdir, catalogOutDirId, sessionId);
            }
            updateFileInfo(study, filesToIndex, storageETLResults, outdir, options, sessionId);
            if (calculateStats) {
                updateDefaultCohortStatus(sessionId, study, exception);
            }
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
        return storageETLResults;
    }

    private long getStudyId(List<Long> fileIds) throws CatalogException {
        long studyIdByInputFileId = -1;
        for (Long fileIdLong : fileIds) {
            long studyId = fileManager.getStudyId(fileIdLong);
            if (studyId == -1) {
                // Skip the file. Something strange occurred.
                logger.error("Could not obtain study of the file {}", fileIdLong);
                throw new CatalogException("Could not obtain the study of the file " + fileIdLong + ". Is it a correct file id?.");
//                continue;
            }

            // Check that the study of all the files is the same
            if (studyIdByInputFileId == -1) {
                // First iteration
                studyIdByInputFileId = studyId;
            } else if (studyId != studyIdByInputFileId) {
                throw new CatalogException("Cannot index files coming from different studies.");
            }
        }
        return studyIdByInputFileId;
    }

    @Override
    protected List<File> copyResults(Path tmpOutdirPath, long catalogPathOutDir, String sessionId) throws CatalogException, IOException {
        List<File> files = super.copyResults(tmpOutdirPath, catalogPathOutDir, sessionId);

//        List<File> avroFiles = new ArrayList<>(files.size());
//        for (File file : files) {
//            if (File.Format.AVRO.equals(file.getFormat())) {
//                avroFiles.add(file);
//            }
//        }

        logger.info("{} files found", files.size());
        if (files.size() > 0) {
            fileManager.matchUpVariantFiles(files, sessionId);
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

    private void updateFileInfo(Study study, List<File> filesToIndex, List<StorageETLResult> storageETLResults, Path outdir,
                                QueryOptions options, String sessionId) throws CatalogException, IOException {

        Map<String, StorageETLResult> map;
        try {
            map = storageETLResults
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
            indexedFile = catalogManager.getFile(indexedFile.getId(), sessionId).first();
            // Suppose that the missing results are due to errors, and those files were not indexed.
            StorageETLResult storageETLResult = map.get(indexedFile.getName());

            boolean jobFailed = storageETLResult == null || storageETLResult.getLoadError() != null
                    || storageETLResult.getTransformError() != null;

            boolean transformedSuccess = storageETLResult != null && storageETLResult.isTransformExecuted()
                    && storageETLResult.getTransformError() == null;
            boolean loadedSuccess = storageETLResult != null && storageETLResult.isLoadExecuted()
                    && storageETLResult.getLoadError() == null;

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
                            if (transformedSuccess) {
                                indexStatusName = FileIndex.IndexStatus.TRANSFORMED;
                            } else {
                                indexStatusName = FileIndex.IndexStatus.NONE;
                            }
                            indexStatusMessage = "Job failed. Restoring status from " + FileIndex.IndexStatus.INDEXING
                                    + " to " + indexStatusName;
                            logger.warn(indexStatusName);
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
                throw new CatalogException("Critical error. Empty index parameter in file " + indexedFile.getId());
            }

            if (transformedSuccess) {
                updateVariantFileStats(indexedFile, outdir, sessionId);
            }

            // Update storageETLResult
            Map<String, Object> attributes = indexedFile.getAttributes();
            attributes.put("storageETLResult", storageETLResult);
            ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
            fileManager.update(indexedFile.getId(), params, new QueryOptions(), sessionId);

            // Update index status
            fileManager.updateFileIndexStatus(indexedFile, indexStatusName, indexStatusMessage, sessionId);

            boolean calculateStats = options.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key());
            if (indexStatusName.equals(FileIndex.IndexStatus.READY) && calculateStats) {
                Query query = new Query(CohortDBAdaptor.QueryParams.NAME.key(), StudyEntry.DEFAULT_COHORT);
                QueryResult<Cohort> queryResult = catalogManager.getCohortManager().get(study.getId(), query, new QueryOptions(),
                        sessionId);
                if (queryResult.getNumResults() != 0) {
                    logger.debug("Default cohort status set to READY");
                    Cohort defaultCohort = queryResult.first();
                    catalogManager.getCohortManager().setStatus(Long.toString(defaultCohort.getId()), Cohort.CohortStatus.READY, null,
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
     * @param inputFile
     * @param outdir
     * @param sessionId
     * @throws CatalogException if a Catalog error occurs.
     */
    private void updateVariantFileStats(File inputFile, Path outdir, String sessionId) throws CatalogException, IOException {
        if (inputFile.getBioformat().equals(File.Bioformat.VARIANT)) {
            Path metaFile = outdir.resolve(inputFile.getName() + "." + VariantReaderUtils.METADATA_FILE_FORMAT_GZ);
            if (!metaFile.toFile().exists()) {
                throw new IOException("Stats file not found.");
            }
            VariantGlobalStats stats;
            try {
                VariantSource variantSource = VariantReaderUtils.readVariantSource(metaFile, null);
                stats = variantSource.getStats();
            } catch (StorageManagerException e) {
                throw new CatalogException("Error reading file \"" + metaFile + "\"", e);
            }
            ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.STATS.key(), new ObjectMap(VARIANT_STATS, stats));
            fileManager.update(inputFile.getId(), params, new QueryOptions(), sessionId);
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

    private boolean updateDefaultCohort(File file, Study study, QueryOptions options, String sessionId) throws CatalogException {
        /* Get file samples */
        boolean modified = false;
        List<Sample> sampleList;
        if (file.getSampleIds() == null || file.getSampleIds().isEmpty()) {
            final ObjectMap fileModifyParams = new ObjectMap(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), new ObjectMap());
            sampleList = FileMetadataReader.get(catalogManager).getFileSamples(study, file,
                    catalogManager.getFileManager().getUri(file), fileModifyParams,
                    options.getBoolean(FileMetadataReader.CREATE_MISSING_SAMPLES, true), false, options, sessionId);
        } else {
            Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), file.getSampleIds());
            sampleList = catalogManager.getSampleManager().get(study.getId(), query, new QueryOptions(), sessionId).getResult();
        }

        Cohort defaultCohort;
        Query query = new Query(CohortDBAdaptor.QueryParams.NAME.key(), StudyEntry.DEFAULT_COHORT);
        QueryResult<Cohort> cohorts = catalogManager.getAllCohorts(study.getId(), query, new QueryOptions(), sessionId);

        if (cohorts.getResult().isEmpty()) {
            defaultCohort = catalogManager.getCohortManager().create(study.getId(), StudyEntry.DEFAULT_COHORT, Study.Type.COLLECTION,
                    "Default cohort with almost all indexed samples", Collections.emptyList(), null, sessionId).first();
            modified = true;
        } else {
            defaultCohort = cohorts.first();
        }

        //Samples are the already indexed plus those that are going to be indexed
        ObjectMap updateParams = new ObjectMap();
        Set<Long> samples = new HashSet<>(defaultCohort.getSamples());
        samples.addAll(sampleList.stream().map(Sample::getId).collect(Collectors.toList()));
        if (samples.size() != defaultCohort.getSamples().size()) {
            logger.debug("Updating \"{}\" cohort", StudyEntry.DEFAULT_COHORT);
            updateParams.append(CohortDBAdaptor.QueryParams.SAMPLES.key(), new ArrayList<>(samples));
        }
        if (!updateParams.isEmpty()) {
            catalogManager.getCohortManager().update(defaultCohort.getId(), updateParams, new QueryOptions(), sessionId);
            modified = true;
        }
        return modified;
    }

    private void updateDefaultCohortStatus(String sessionId, Study study, StorageManagerException exception) throws CatalogException {
        if (exception == null) {
            updateDefaultCohortStatus(study, Cohort.CohortStatus.READY, sessionId);
        } else {
            updateDefaultCohortStatus(study, Cohort.CohortStatus.INVALID, sessionId);
        }
    }

    private void updateDefaultCohortStatus(Study study, String status, String sessionId) throws CatalogException {

        Query query = new Query(CohortDBAdaptor.QueryParams.NAME.key(), StudyEntry.DEFAULT_COHORT);
        Cohort defaultCohort = catalogManager.getAllCohorts(study.getId(), query, new QueryOptions(), sessionId).first();

        catalogManager.getCohortManager().setStatus(Long.toString(defaultCohort.getId()), status, null,
                sessionId);
    }

    private List<File> filterTransformFiles(List<File> fileList) throws CatalogException {
        if (fileList == null || fileList.isEmpty()) {
            return new ArrayList<>();
        }

        List<File> filteredFiles = new ArrayList<>(fileList.size());
        for (File file : fileList) {
            if (file.getStatus().getName().equals(File.FileStatus.READY)
                    && file.getFormat().equals(File.Format.VCF)) {
                if (file.getIndex() != null) {
                    if (file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.NONE)) {
                        filteredFiles.add(file);
                    } else {
                        logger.warn("We can only transform VCF files not transformed, the status is {}",
                                file.getIndex().getStatus().getName());
                    }
                } else {
                    // This block should not happen ever
                    filteredFiles.add(file);
                    logger.warn("This block should not happen ever");
                }
            } else {
                logger.warn("");
            }
        }
        return filteredFiles;
    }

    private List<File> filterLoadFiles(List<File> fileList, QueryOptions options, List<URI> fileUris, String sessionId)
            throws CatalogException, URISyntaxException {
        if (fileList == null || fileList.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> avroFiles = null;
        if (options.get(TRANSFORMED_FILES) != null) {
            avroFiles = options.getAsStringList(TRANSFORMED_FILES);
            if (avroFiles.size() != fileList.size()) {
                throw new CatalogException("The number of avro files (" + avroFiles.size() + ") passed does not match the number of vcf "
                        + "files (" + fileList.size() + ").");
            }
            // Check all the files are accessible and are actually files
            for (String avroFile : avroFiles) {
                java.io.File file = Paths.get(avroFile).toFile();
                if (!file.exists() || !file.isFile()) {
                    throw new CatalogException("File " + file + " does not exist or it is not an avro file");
                }
            }
        }

        List<File> filteredFiles = new ArrayList<>(fileList.size());
        for (int i = 0; i < fileList.size(); i++) {
            File file = fileList.get(i);

            if (file.getFormat().equals(File.Format.VCF) || file.getFormat().equals(File.Format.GVCF)) {
                String status = file.getIndex() == null ? FileIndex.IndexStatus.NONE : file.getIndex().getStatus().getName();
                switch (status) {
                    case FileIndex.IndexStatus.NONE:
                        if (avroFiles != null) {
                            filteredFiles.add(file);
                            fileUris.add(UriUtils.createUri(avroFiles.get(i)));
                        } else {
                            logger.warn("Cannot load vcf file " + file.getId() + " if no avro file is provided.");
                        }
                        break;
                    case FileIndex.IndexStatus.TRANSFORMED:
                        // We will attempt to use the avro file registered in catalog
                        long avroId = file.getIndex().getTransformedFile().getId();
                        if (avroId == -1) {
                            logger.error("This code should never be executed. Every vcf file containing the transformed status should have"
                                    + " a registered avro file");
                            throw new CatalogException("Internal error. No avro file could be found for file " + file.getId());
                        }
                        QueryResult<File> avroQueryResult = fileManager.get(avroId, new QueryOptions(), sessionId);
                        if (avroQueryResult.getNumResults() != 1) {
                            logger.error("This code should never be executed. No avro file could be found under ");
                            throw new CatalogException("Internal error. No avro file could be found under id " + avroId);
                        }

                        if (avroFiles != null) {
                            // Check that the uri from the avro file obtained from catalog is the same the user has put as input
                            URI avroUri = UriUtils.createUri(avroFiles.get(i));
                            if (!avroUri.equals(avroQueryResult.first().getUri())) {
                                throw new CatalogException("An Avro file was found for file " + file.getId() + " in "
                                        + avroQueryResult.first().getUri() + ". However, the user selected a different one in " + avroUri);
                            }
                        }
                        filteredFiles.add(file);
                        fileUris.add(avroQueryResult.first().getUri());
                        break;
                    case FileIndex.IndexStatus.TRANSFORMING:
                        logger.warn("We can only load files previously transformed. Skipping file {}", file.getName());
                        break;
                    case FileIndex.IndexStatus.LOADING:
                    case FileIndex.IndexStatus.INDEXING:
                        logger.warn("Unable to load this file. Already being loaded. Skipping file {}", file.getName());
                        break;
                    case FileIndex.IndexStatus.READY:
                        logger.warn("Already loaded file. Skipping file {}", file.getName());
                        break;
                    default:
                        logger.warn("We can only load files previously transformed, File {} with status is {}", file.getName(), status);
                        break;
                }
            } else if (file.getFormat().equals(File.Format.AVRO)) {
                if (avroFiles != null) {
                    // Check if the uri is different
                    URI avroUri = UriUtils.createUri(avroFiles.get(i));
                    if (!avroUri.equals(file.getUri())) {
                        // This case should never happen. If we are here, it means that the input file was already an avro file and that
                        // a list of avro uris have been found. However, the uri of the avro does not match the uri we have for the avro
                        // file. Nonsense.
                        throw new CatalogException("The avro file " + file.getId() + " in " + file.getUri() + " has been associated a "
                                + "different uri " + avroUri + " by the user.");
                    }
                }
                // Look for the vcf file
                long vcfId = -1;
                // Matchup variant files, if missing
                if (file.getRelatedFiles() == null || file.getRelatedFiles().isEmpty()) {
                    catalogManager.getFileManager().matchUpVariantFiles(Collections.singletonList(file), sessionId);
                }
                for (File.RelatedFile relatedFile : file.getRelatedFiles()) {
                    if (File.RelatedFile.Relation.PRODUCED_FROM.equals(relatedFile.getRelation())) {
                        vcfId = relatedFile.getFileId();
                        break;
                    }
                }
                if (vcfId == -1) {
                    logger.error("This code should never be executed. Every transformed avro file should come from a registered vcf file");
                    throw new CatalogException("Internal error. No vcf file could be found for file " + file.getId());
                }
                QueryResult<File> vcfQueryResult = fileManager.get(vcfId, new QueryOptions(), sessionId);
                if (vcfQueryResult.getNumResults() != 1) {
                    logger.error("This code should never be executed. No vcf file could be found for vcf id " + vcfId);
                    throw new CatalogException("Internal error. No vcf file could be found under id " + vcfId);
                }
                filteredFiles.add(vcfQueryResult.first());
                fileUris.add(file.getUri());
            } else {
                logger.warn("The input file is not a variant file. Format {}", file.getFormat());
            }

        }
        return filteredFiles;
    }


}
