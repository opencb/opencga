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

package org.opencb.opencga.analysis.variant;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.AnalysisExecutionException;
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
public class VariantFileIndexer extends AbstractFileIndexer {

    private final CatalogConfiguration catalogConfiguration;
    private final StorageConfiguration storageConfiguration;
    private final IFileManager fileManager;


    public static final String TRANSFORM = "transform";
    public static final String LOAD = "load";

    public static final String CATALOG_PATH = "catalogPath";
    public static final String TRANSFORMED_FILES = "transformedFiles";

    private final String VCF_EXTENSION = ".vcf";
    private final String AVRO_EXTENSION = ".avro";

    private enum Type {
        TRANSFORM,
        LOAD,
        INDEX
    }

    public VariantFileIndexer(CatalogConfiguration catalogConfiguration, StorageConfiguration storageConfiguration)
            throws CatalogException {
        super(new CatalogManager(catalogConfiguration), LoggerFactory.getLogger(VariantFileIndexer.class));
        this.catalogConfiguration = catalogConfiguration;
        this.storageConfiguration = storageConfiguration;
        this.fileManager = catalogManager.getFileManager();
    }

    public List<StorageETLResult> index(String fileIds, String outdirString, String sessionId, QueryOptions options)
            throws CatalogException, AnalysisExecutionException, IOException, IllegalAccessException, InstantiationException,
            ClassNotFoundException, StorageManagerException, URISyntaxException {

        // Query catalog for user data
        String userId = catalogManager.getUserManager().getId(sessionId);
        List<Long> fileIdsLong = fileManager.getIds(userId, fileIds);
        return index(fileIdsLong, outdirString, sessionId, options);
    }

    public List<StorageETLResult> index(List<Long> fileIds, String outdirString, String sessionId, QueryOptions options)
            throws CatalogException, AnalysisExecutionException, IOException, IllegalAccessException, InstantiationException,
            ClassNotFoundException, StorageManagerException, URISyntaxException {

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
        Thread hook = new Thread(() -> {
            try {
                // If the status has not been changed by the method and is still running, we assume that the execution failed.
                Job.JobStatus status = readJobStatus(outdir);
                if (status.getName().equalsIgnoreCase(Job.JobStatus.RUNNING)) {
                    writeJobStatus(outdir, new Job.JobStatus(Job.JobStatus.ERROR, "Job finished with an error."));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
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
        List<File> inputFiles = new ArrayList<>();
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
        long catalogPathId = -1;
        if (options.get(CATALOG_PATH) != null) {
            catalogPathId = fileManager.getId(options.getString(CATALOG_PATH), studyIdByInputFileId, sessionId);
            if (catalogPathId <= 0) {
                throw new CatalogException("Output directory could not be found within catalog.");
            }
        }

        logger.debug("Index - Number of files to be indexed: {}, list of files: {}", inputFiles.size(),
                inputFiles.stream().map(File::getName).collect(Collectors.toList()));

        Study study = catalogManager.getStudyManager().get(studyIdByInputFileId, new QueryOptions(), sessionId).getResult().get(0);

        // We get the credentials of the Datastore to insert the variants
        DataStore dataStore = getDataStore(catalogManager, studyIdByInputFileId, File.Bioformat.VARIANT, sessionId);

        options.put(VariantStorageManager.Options.DB_NAME.key(), dataStore.getDbName());
        options.put(VariantStorageManager.Options.STUDY_ID.key(), studyIdByInputFileId);

        VariantStorageManager variantStorageManager = StorageManagerFactory.get().getVariantStorageManager(dataStore.getStorageEngine());
        variantStorageManager.getOptions().putAll(options);

        String fileStatus;
        List<File> filesToIndex;
        List<URI> fileUris = new ArrayList<>(inputFiles.size());
        if (step.equals(Type.INDEX)) {
            fileStatus = FileIndex.IndexStatus.INDEXING;
            filesToIndex = filterTransformFiles(inputFiles);
            for (File file : filesToIndex) {
                fileUris.add(file.getUri());
            }
        } else if (step.equals(Type.TRANSFORM)) {
            fileStatus = FileIndex.IndexStatus.TRANSFORMING;
            filesToIndex = filterTransformFiles(inputFiles);
            for (File file : filesToIndex) {
                fileUris.add(file.getUri());
            }
        } else {
            filesToIndex = filterLoadFiles(inputFiles, options, fileUris, sessionId);
            fileStatus = FileIndex.IndexStatus.LOADING;
        }

        if (filesToIndex.size() == 0) {
            logger.warn("Nothing to do.");
            return Collections.emptyList();
        }

        // Only if we are not transforming or if a path has been passed, we will update catalog information
        List<String> previousFileStatus = new ArrayList<>(filesToIndex.size());
        if (!step.equals(Type.TRANSFORM) || options.get(CATALOG_PATH) != null) {
            for (File file : filesToIndex) {
                previousFileStatus.add(file.getIndex().getStatus().getName());
                QueryResult<FileIndex> fileIndexQueryResult = fileManager.updateFileIndexStatus(file, fileStatus, sessionId);
                file.setIndex(fileIndexQueryResult.first());
            }
        }

        // Update study configuration BEFORE executing the index
        updateStudyConfiguration(sessionId, studyIdByInputFileId, dataStore);

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
        }

//        logger.debug("Writing storageETLResults to file {}", outdir.resolve("storageETLresults"));
//        objectMapper.writer().writeValue(outdir.resolve("storageETLresults").toFile(), storageETLResults);

        // Only if we are not transforming or if a path has been passed, we will update catalog information
        if (!step.equals(Type.TRANSFORM) || options.get(CATALOG_PATH) != null) {
            if (!step.equals(Type.LOAD) && options.get(CATALOG_PATH) != null) {
                // Copy results to catalog
                copyResults(outdir, catalogPathId, sessionId);
            }
            updateFileInfo(study, filesToIndex, storageETLResults, outdir, options, sessionId);
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

    @Override
    protected List<File> copyResults(Path tmpOutdirPath, long catalogPathOutDir, String sessionId) throws CatalogException, IOException {
        List<File> files = super.copyResults(tmpOutdirPath, catalogPathOutDir, sessionId);

        List<File> avroFiles = new ArrayList<>(files.size());
        for (File file : files) {
            if (File.Format.AVRO.equals(file.getFormat())) {
                avroFiles.add(file);
            }
        }

        logger.info("{} avro files found", avroFiles.size());
        if (avroFiles.size() > 0) {
            fileManager.matchUpVariantFiles(avroFiles, sessionId);
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

        Map<String, StorageETLResult> map = storageETLResults
                .stream()
                .collect(Collectors.toMap(s -> {
                    String input = s.getInput().getPath();
                    String inputFileName = Paths.get(input).getFileName().toString();
                    // Input file may be the transformed one. Convert into original file.
                    return VariantReaderUtils.getOriginalFromTransformedFile(inputFileName);
                }, i -> i));

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

            FileIndex index;

            if (indexedFile.getIndex() != null) {
                index = indexedFile.getIndex();
                switch (index.getStatus().getName()) {
                    case FileIndex.IndexStatus.NONE:
                    case FileIndex.IndexStatus.TRANSFORMED:
                        logger.warn("Unexpected index status. Expected " + FileIndex.IndexStatus.TRANSFORMING + ", "
                                + FileIndex.IndexStatus.LOADING + " or " + FileIndex.IndexStatus.INDEXING + " and got "
                                + index.getStatus());
                    case FileIndex.IndexStatus.READY: //Do not show warn message when index status is READY.
                        break;
                    case FileIndex.IndexStatus.TRANSFORMING:
                        if (jobFailed) {
                            logger.warn("Job failed. Restoring status from " + FileIndex.IndexStatus.TRANSFORMING + " to "
                                    + FileIndex.IndexStatus.NONE);
                            index.getStatus().setName(FileIndex.IndexStatus.NONE);
                        } else {
                            index.getStatus().setName(FileIndex.IndexStatus.TRANSFORMED);
                        }
                        break;
                    case FileIndex.IndexStatus.LOADING:
                        if (jobFailed) {
                            logger.warn("Job failed. Restoring status from " + FileIndex.IndexStatus.LOADING + " to "
                                    + FileIndex.IndexStatus.TRANSFORMED);
                            index.getStatus().setName(FileIndex.IndexStatus.TRANSFORMED);
                        } else {
                            index.getStatus().setName(FileIndex.IndexStatus.READY);
                        }
                        break;
                    case FileIndex.IndexStatus.INDEXING:
                        if (jobFailed) {
                            String newStatus;
                            // If transform was executed, restore status to Transformed.
                            if (transformedSuccess) {
                                newStatus = FileIndex.IndexStatus.TRANSFORMED;
                            } else {
                                newStatus = FileIndex.IndexStatus.NONE;
                            }
                            logger.warn("Job failed. Restoring status from " + FileIndex.IndexStatus.INDEXING + " to " + newStatus);
                            index.getStatus().setName(newStatus);
                        } else {
                            index.getStatus().setName(FileIndex.IndexStatus.READY);
                        }
                        break;
                }
            } else {
                logger.error("The execution should never get into this condition. Critical error.");
                throw new CatalogException("Critical error. Empty index parameter in file " + indexedFile.getId());
            }

            if (transformedSuccess) {
                updateVariantFileStats(indexedFile, outdir, sessionId);
            }

            if (loadedSuccess) {
                updateDefaultCohorts(indexedFile, study, options, sessionId);
            }

            // Update storageETLResult
            Map<String, Object> attributes = indexedFile.getAttributes();
            attributes.put("storageETLResult", storageETLResult);
            ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
            fileManager.update(indexedFile.getId(), params, new QueryOptions(), sessionId);

            // Update index status
            fileManager.updateFileIndexStatus(indexedFile, index.getStatus().getName(), sessionId);

            boolean calculateStats = options.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key());
            if (index.getStatus().getName().equals(FileIndex.IndexStatus.READY) && calculateStats) {
                Query query = new Query(CohortDBAdaptor.QueryParams.NAME.key(), StudyEntry.DEFAULT_COHORT);
                QueryResult<Cohort> queryResult = catalogManager.getCohortManager().get(study.getId(), query, new QueryOptions(),
                        sessionId);
                if (queryResult.getNumResults() != 0) {
                    logger.debug("Default cohort status set to READY");
                    Cohort defaultCohort = queryResult.first();
                    params = new ObjectMap(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), Cohort.CohortStatus.READY);
                    catalogManager.getCohortManager().update(defaultCohort.getId(), params, new QueryOptions(), sessionId);
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
//                VariantGlobalStats stats = variantSource.getStats();
//                catalogManager.modifyFile(inputFile.getId(), new ObjectMap("stats", new ObjectMap(VARIANT_STATS, stats)), sessionId);
//            } catch (IOException e) {
//                throw new CatalogException("Error reading file \"" + fileUri + "\"", e);
//            }
//        }
    }

    private void updateDefaultCohorts(File file, Study study, QueryOptions options, String sessionId) throws CatalogException {
        /* Get file samples */
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
        } else {
            defaultCohort = cohorts.first();
        }

        ObjectMap updateParams = new ObjectMap();

        if (options.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key())) {
            updateParams.append(CohortDBAdaptor.QueryParams.STATUS_NAME.key(), Cohort.CohortStatus.CALCULATING);
        }

        //Samples are the already indexed plus those that are going to be indexed
        Set<Long> samples = new HashSet<>(defaultCohort.getSamples());
        samples.addAll(sampleList.stream().map(Sample::getId).collect(Collectors.toList()));
        if (samples.size() != defaultCohort.getSamples().size()) {
            logger.debug("Updating \"{}\" cohort", StudyEntry.DEFAULT_COHORT);
            updateParams.append(CohortDBAdaptor.QueryParams.SAMPLES.key(), new ArrayList<>(samples));
        }
        if (!updateParams.isEmpty()) {
            catalogManager.getCohortManager().update(defaultCohort.getId(), updateParams, new QueryOptions(), sessionId);
        }
    }

    private void updateCohorts() {

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
                            logger.error("This code should never be executed. Every vcf file containing the transformed status should have a "
                                    + "registered avro file");
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
                        logger.warn("We can only load files previously transformed. Skipping file ", file.getName());
                        break;
                    case FileIndex.IndexStatus.LOADING:
                    case FileIndex.IndexStatus.INDEXING:
                        logger.warn("Unable to load this file. Already being loaded. Skipping file ", file.getName());
                        break;
                    case FileIndex.IndexStatus.READY:
                        logger.warn("Already loaded file. Skipping file ", file.getName());
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

    @Deprecated
    private List<File> filterLoadFiles(List<File> fileList, long studyId, String sessionId) throws CatalogException {
        if (fileList == null || fileList.isEmpty()) {
            return new ArrayList<>();
        }

        File vcf;
        File avro;
        List<File> filteredFiles = new ArrayList<>(fileList.size());
        for (File file : fileList) {

            // This should use relatedFile in the future
            if (file.getFormat().equals(File.Format.VCF) || file.getFormat().equals(File.Format.GVCF)) {
                vcf = file;
                avro = search(studyId, file, VCF_EXTENSION, AVRO_EXTENSION, sessionId);
            } else {
                avro = file;
                vcf = search(studyId, file, AVRO_EXTENSION, VCF_EXTENSION, sessionId);
            }

            if (vcf == null || avro == null) {
                logger.warn("");
                continue;
            }

            if (vcf.getStatus().getName().equals(File.FileStatus.READY) && avro.getStatus().getName().equals(File.FileStatus.READY)) {
                if (vcf.getIndex() != null) {
                    if (vcf.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.TRANSFORMED)) {
                        filteredFiles.add(avro);
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

    /**
     * Look for the related avro or vcf file in catalog using one of the pairs as a starting point
     *
     * @param studyId study id of the files.
     * @param sourceFile avro or vcf file used to look for the vcf or avro file respectively.
     * @param sourceExtension Extension of the file being passed.
     * @param destinyExtension Extension of the file being looked for.
     * @param sessionId session id of the user that should have permissions for the file being looked for.
     * @return the obtained file or null otherwise.
     */
    @Deprecated
    private File search(long studyId, File sourceFile, String sourceExtension, String destinyExtension, String sessionId)
            throws CatalogException {
        // Look for the destiny file in the same folder where the source file is located
        Path sourcePath = Paths.get(sourceFile.getPath());

        Path parent = sourcePath.getParent();
        String destinyFileName = sourcePath.getFileName().toString().replace(sourceExtension, destinyExtension);

        // TODO: Use relatedFiles to look for the files
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.NAME.key(), destinyFileName)
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult<File> fileQueryResult = fileManager.get(parent.toString(), true, query, new QueryOptions(), sessionId);
        if (fileQueryResult.getNumResults() == 1) {
            return fileQueryResult.first();
        }

        // Look for the file in the same path
        String destinyPath = parent.resolve(destinyFileName).toString();

        for (File file : fileQueryResult.getResult()) {
            if (destinyPath.equals(file.getPath())) {
                return file;
            }
        }

        logger.error("{} files have been found as possible pairs of {} with id {}", fileQueryResult.getNumResults(), sourceFile.getName(),
                sourceFile.getId());
        return null;
    }

}
