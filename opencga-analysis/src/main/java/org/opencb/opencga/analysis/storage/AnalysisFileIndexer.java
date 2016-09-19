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

package org.opencb.opencga.analysis.storage;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.JobFactory;
import org.opencb.opencga.analysis.variant.AbstractFileIndexer;
import org.opencb.opencga.analysis.variant.CatalogStudyConfigurationFactory;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 16/10/14.
 *
 * IndexFile (fileId, backend, outDir)
 * - get Samples data
 * - create temporal outDir (must be a new folder)
 * - Add index information to the file
 * - create command line
 * - create job
 * - update index file
 *
 *
 *  * If only transform, do not add index information
 *  * If only load, take "originalFile" from the "inputFileId" jobId -> job.attributes.indexedFile
 *
 * UnIndexFile (fileId, backend)
 * ?????????????????????????????
 *
 */
@Deprecated
public class AnalysisFileIndexer {

    //Properties
    public static final String OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX = "OPENCGA.ANALYSIS.STORAGE.DATABASE_PREFIX";

    //Options
    public static final String PARAMETERS = "parameters";
    public static final String TRANSFORM = "transform";
    public static final String CREATE = "create";
    public static final String LOAD = "load";
    public static final String LOG_LEVEL = "logLevel";


    //Other
    @Deprecated
    public static final String OPENCGA_STORAGE_BIN_NAME = "opencga-storage.sh";
    public static final String OPENCGA_ANALYSIS_BIN_NAME = "opencga-analysis.sh";

    private final CatalogManager catalogManager;
    protected static Logger logger = LoggerFactory.getLogger(AnalysisFileIndexer.class);

    public AnalysisFileIndexer(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    /**
     *
     * @param fileId            File to index
     * @param outDirId          Place where locate the temporary files
     * @param sessionId         User session Id
     * @param options           Other options
     * @return                  Generated job for the indexation
     * @throws IOException
     * @throws CatalogException
     * @throws AnalysisExecutionException
     */
    public QueryResult<Job> index(long fileId, long outDirId, String sessionId, QueryOptions options)
            throws CatalogException, AnalysisExecutionException {

        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(ExecutorManager.EXECUTE);
        final boolean simulate = options.getBoolean(ExecutorManager.SIMULATE);
        final long start = System.currentTimeMillis();
        final boolean transform;
        final boolean load;

        if (!options.getBoolean(TRANSFORM, false) && !options.getBoolean(LOAD, false)) {  // if not present --transform nor --load, do both
            transform = true;
            load = true;
        } else {
            transform = options.getBoolean(TRANSFORM, false);
            load = options.getBoolean(LOAD, false);
        }


        /** Query catalog for user data. **/
        String userId = catalogManager.getUserIdBySessionId(sessionId);
        File inputFile = catalogManager.getFile(fileId, sessionId).first();
        File originalFile;
        File outDir = catalogManager.getFile(outDirId, sessionId).first();
        long studyIdByOutDirId = catalogManager.getStudyIdByFileId(outDirId);
        long studyIdByInputFileId = catalogManager.getStudyIdByFileId(inputFile.getId());
        Study study = catalogManager.getStudy(studyIdByOutDirId, sessionId).getResult().get(0);

        if (inputFile.getType() != File.Type.FILE) {
            throw new CatalogException("Expected file type = " + File.Type.FILE + " instead of " + inputFile.getType());
        }

        if (studyIdByInputFileId != studyIdByOutDirId) {
            logger.warn("Indexing a file from the study " + studyIdByInputFileId + " in the study " + studyIdByOutDirId);
            // TODO: Raise an exception?
        }

        // The file is already transformed, but the transformation job is not registered in Catalog.
        boolean externalTransformed = false;

        /** Get the original file. **/
        if (!transform && load) { //Don't transform. Just load. Select the original file
            final long indexedFileId;
            // FIXME: What if the original VCF contains a jobId ?
            if (inputFile.getJobId() <= 0) {

                // TODO: Move this code to an external class for relating Transformed and Original files.
                // If the input file is the original file, read from the Index object
                if (inputFile.getIndex() != null) {
                    long jobId = inputFile.getIndex().getJobId();
                    Query query;
                    if (inputFile.getBioformat().equals(File.Bioformat.VARIANT)) {
                        query = new Query(FileDBAdaptor.QueryParams.JOB_ID.key(), jobId)
                                .append(FileDBAdaptor.QueryParams.NAME.key(), "~" + inputFile.getName() + ".variants");
                    } else {
                        throw new CatalogException("Error: can't load this file. Only transformed files can be loaded.");
                    }
                    QueryResult<File> result = catalogManager.searchFile(studyIdByOutDirId, query, sessionId);
                    indexedFileId = inputFile.getId();
                    inputFile = result.first();
                } else if (inputFile.getAttributes().containsKey(Job.INDEXED_FILE_ID)) {
                    indexedFileId = new ObjectMap(inputFile.getAttributes()).getInt(Job.INDEXED_FILE_ID);
                } else {

                    // If the input file is an externally transformed file, find the original file.
                    if (inputFile.getBioformat().equals(File.Bioformat.VARIANT) && !(inputFile.getFormat() == File.Format.VCF)) {
                        VariantReaderUtils utils = new VariantReaderUtils();
                        try {
                            // Read the VariantSource to get the source file
                            VariantSource variantSource = utils.readVariantSource(catalogManager.getFileUri(inputFile));
                            Query query = new Query(FileDBAdaptor.QueryParams.NAME.key(), variantSource.getFileName());
                            QueryResult<File> result = catalogManager.searchFile(studyIdByOutDirId, query, sessionId);
                            if (result.getResult().size() == 0) {
                                // TODO: Continue with the transformed file as indexed file?
                                throw new CatalogException("Unable to find file \"" + variantSource.getFileName() + "\" "
                                        + "as source file from \"" + inputFile.getName() + "\"");
                            } else if (result.getResult().size() > 1) {
                                List<String> foundFilesSummary = result.getResult().stream().map(File::getPath).collect(Collectors.toList());
                                throw new CatalogException("Unable to find single file \"" + variantSource.getFileName() + "\" "
                                        + "as source file from \"" + inputFile.getName() + "\". Got multiple versions: " + foundFilesSummary);
                            } else {
                                externalTransformed = true;
                                indexedFileId = result.first().getId();
//                                inputFile = result.first();
                            }
                        } catch (StorageManagerException e) {
                            throw new CatalogException("Unable to find source file from \"" + inputFile.getName() + "\"", e);
                        }
                    } else {
                        // TODO: Solve for other bioformats
                        throw new CatalogException("Unable to find source file from \"" + inputFile.getName() + "\"");
                    }
                }

//                throw new CatalogException("Error: can't load this file. JobId unknown. Need jobId to know origin file. " +
//                        "Only transformed files can be loaded.");
            } else {
                Job job = catalogManager.getJob(inputFile.getJobId(), null, sessionId).first();
                if (job.getAttributes().containsKey(Job.INDEXED_FILE_ID)) {
                    indexedFileId = new ObjectMap(job.getAttributes()).getInt(Job.INDEXED_FILE_ID);
                } else {
                    logger.warn("INDEXED_FILE_ID missing in job " + job.getId());
                    List<Long> jobInputFiles = job.getInput();
                    if (jobInputFiles.size() != 1) {
                        throw new CatalogException("Error: Job {id: " + job.getId() + "} input is empty");
                    }
                    indexedFileId = jobInputFiles.get(0);
                }
            }
            originalFile = catalogManager.getFile(indexedFileId, null, sessionId).first();
            if (!originalFile.getStatus().getName().equals(File.FileStatus.READY)) {
                throw new CatalogException("Error: Original file status must be \"READY\", not \"" + originalFile.getStatus().getName() + "\"");
            }
        } else {
            originalFile = inputFile;
        }

        final DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, catalogManager.getStudyIdByFileId(originalFile.getId()), originalFile.getBioformat(), sessionId);

        /** Check if file can be indexed **/
        if (originalFile.getIndex() != null) {
            switch (originalFile.getIndex().getStatus().getName()) {
                case FileIndex.IndexStatus.TRANSFORMING:
                    throw new CatalogException("File '" + originalFile.getId() + "' it's being transformed");
                case FileIndex.IndexStatus.TRANSFORMED:
                    if (transform) {
                        throw new CatalogException("File '" + originalFile.getId() + "' is already transformed");
                    }
                    break;
                case FileIndex.IndexStatus.LOADING:
                    throw new CatalogException("File '" + originalFile.getId() + "' it's being loaded");
                case FileIndex.IndexStatus.INDEXING:
                    throw new CatalogException("File '" + originalFile.getId() + "' it's being indexed");
                case FileIndex.IndexStatus.READY:
                    throw new CatalogException("File '" + originalFile.getId() + "' is already indexed");
                case FileIndex.IndexStatus.NONE:
                    break;
            }
        } else {
            if (!transform && load) {
                if (!externalTransformed) {
                    throw new CatalogException("File '" + originalFile.getId() + "' need to be transformed before loading");
                }
            }
        }

        // ObjectMap to fill with modifications over the indexed file (like new attributes or jobId)
        final ObjectMap fileModifyParams = new ObjectMap("attributes", new ObjectMap());
        final ObjectMap indexAttributes;
        if (originalFile.getIndex() == null || originalFile.getIndex().getAttributes() == null) {
            indexAttributes = new ObjectMap();
        } else {
            indexAttributes = new ObjectMap(originalFile.getIndex().getAttributes());
        }

        /** Create temporal Job Outdir **/
        final URI temporalOutDirUri;
        final String randomString = "I_" + RandomStringUtils.randomAlphanumeric(10);
        if (simulate) {
            temporalOutDirUri = createSimulatedOutDirUri(randomString);
        } else {
            temporalOutDirUri = catalogManager.createJobOutDir(studyIdByOutDirId, randomString, sessionId);
        }

        /** Get file samples **/
        List<Sample> sampleList;
        if (originalFile.getSampleIds() == null || originalFile.getSampleIds().isEmpty()) {
            try {
                sampleList = FileMetadataReader.get(catalogManager).getFileSamples(study, originalFile,
                        catalogManager.getFileUri(originalFile), fileModifyParams,
                        options.getBoolean(FileMetadataReader.CREATE_MISSING_SAMPLES, true), simulate, options, sessionId);
            } catch (CatalogException e) {
                throw new AnalysisExecutionException(e);
            }
        } else {
            sampleList = catalogManager.getAllSamples(study.getId(), new Query("id", originalFile.getSampleIds()), new QueryOptions(), sessionId).getResult();
        }
        if (!simulate) {
            Cohort defaultCohort = null;
            QueryResult<Cohort> cohorts = catalogManager.getAllCohorts(studyIdByOutDirId,
                    new Query(CohortDBAdaptor.QueryParams.NAME.key(), StudyEntry.DEFAULT_COHORT), new QueryOptions(), sessionId);
            if (cohorts.getResult().isEmpty()) {
                defaultCohort = catalogManager.createCohort(studyIdByOutDirId, StudyEntry.DEFAULT_COHORT, Study.Type.COLLECTION,
                        "Default cohort with almost all indexed samples", Collections.emptyList(), null, sessionId).first();
            } else {
                defaultCohort = cohorts.first();
            }

            ObjectMap updateParams = new ObjectMap();

            if (options.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key()) && load) {
                updateParams.append("status.name", Cohort.CohortStatus.CALCULATING);
            }
            //Samples are the already indexed plus those that are going to be indexed
            Set<Long> samples = new HashSet<>(defaultCohort.getSamples());
            samples.addAll(sampleList.stream().map(Sample::getId).collect(Collectors.toList()));
            if (samples.size() != defaultCohort.getSamples().size()) {
                logger.debug("Updating \"{}\" cohort", StudyEntry.DEFAULT_COHORT);
                updateParams.append("samples", new ArrayList<>(samples));
            }
            if (!updateParams.isEmpty()) {
                catalogManager.modifyCohort(defaultCohort.getId(), updateParams, new QueryOptions(), sessionId);
            }
        }


        /** Create commandLine **/
        String commandLine = createCommandLine(study, originalFile, inputFile, sampleList,
                outDirId, temporalOutDirUri, randomString, indexAttributes, dataStore, sessionId, options);
        if (options.containsKey(PARAMETERS)) {
            List<String> extraParams = options.getAsStringList(PARAMETERS);
            for (String extraParam : extraParams) {
                commandLine += " " + extraParam;
            }
        }

        /** Update StudyConfiguration **/
        if (!simulate) {
            try {
                if (inputFile.getBioformat().equals(File.Bioformat.VARIANT)) {
                    StudyConfigurationManager studyConfigurationManager = StorageManagerFactory.get().getVariantStorageManager(dataStore.getStorageEngine())
                            .getDBAdaptor(dataStore.getDbName()).getStudyConfigurationManager();
                    new CatalogStudyConfigurationFactory(catalogManager).updateStudyConfigurationFromCatalog(studyIdByOutDirId, studyConfigurationManager, sessionId);
                }
            } catch (StorageManagerException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        /** Create index information **/
        FileIndex indexInformation = originalFile.getIndex();
        if (indexInformation == null) {
            String status = externalTransformed ? FileIndex.IndexStatus.TRANSFORMED : FileIndex.IndexStatus.NONE;
            indexInformation = new FileIndex(userId, TimeUtils.getTime(), new FileIndex.IndexStatus(status), -1, indexAttributes);
        }
        if (transform && !load) {
            indexInformation.getStatus().setName(FileIndex.IndexStatus.TRANSFORMING);
        } else if (!transform && load) {
            indexInformation.getStatus().setName(FileIndex.IndexStatus.LOADING);
        } else if (transform && load) {
            indexInformation.getStatus().setName(FileIndex.IndexStatus.INDEXING);
        }

        if (!simulate) {
            fileModifyParams.put("index", indexInformation);
        }

        /** Modify file with new information **/
        if (!simulate) {
            catalogManager.modifyFile(originalFile.getId(), fileModifyParams, sessionId).getResult();
        }

        /** Create job **/
        ObjectMap jobAttributes = new ObjectMap();
        jobAttributes.put(Job.TYPE, Job.Type.INDEX);
        jobAttributes.put(Job.INDEXED_FILE_ID, originalFile.getId());
        jobAttributes.put(VariantStorageManager.Options.CALCULATE_STATS.key(), options.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key(), VariantStorageManager.Options.CALCULATE_STATS.defaultValue()));
//        jobAttributes.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), options.get(VariantStorageManager.Options.AGGREGATED_TYPE.key(), VariantSource.Aggregation.class, VariantStorageManager.Options.AGGREGATED_TYPE.defaultValue()));

        String jobName;
        String jobDescription;
        switch (indexInformation.getStatus().getName()) {
            default:
//                throw new IllegalStateException("Unexpected state");
            case FileIndex.IndexStatus.INDEXING:
                jobName = "index";
                jobDescription = "Indexing file " + originalFile.getName() + " (" + originalFile.getId() + ")";
                break;
            case FileIndex.IndexStatus.LOADING:
                jobName = "load";
                jobDescription = "Loading file " + originalFile.getName() + " (" + originalFile.getId() + ")";
                break;
            case FileIndex.IndexStatus.TRANSFORMING:
                jobName = "transform";
                jobDescription = "Transforming file " + originalFile.getName() + " (" + originalFile.getId() + ")";
                break;
        }
        JobFactory jobFactory = new JobFactory(catalogManager);
        final Job job = jobFactory.createJob(studyIdByOutDirId, jobName,
                OPENCGA_ANALYSIS_BIN_NAME, jobDescription, outDir, Collections.singletonList(inputFile.getId()),
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate, jobAttributes, null).first();


        if (!simulate) {
            modifyIndexJobId(originalFile.getId(), job.getId(), transform, load, sessionId);
        }

        if (simulate) {
            return new QueryResult<>("indexFile", (int) (System.currentTimeMillis() - start), 1, 1, "", "", Collections.singletonList(job));
        } else {
            return new QueryResult<>("indexFile", (int) (System.currentTimeMillis() - start), 1, 1, "", "",
                    catalogManager.getJob(job.getId(), null, sessionId).getResult());
        }
    }


    private void modifyIndexJobId(long fileId, long jobId, boolean transform, boolean load, String sessionId) throws CatalogException {
        File file = catalogManager.getFile(fileId, sessionId).first();
        FileIndex index = file.getIndex();
        index.setJobId(jobId);
        if (transform && !load) {
            index.getAttributes().put("transformJobId", jobId);
        } else if (!transform && load) {
            index.getAttributes().put("loadJobId", jobId);
        } else if (transform && load) {
            index.getAttributes().put("indexJobId", jobId);
        }
        catalogManager.modifyFile(fileId, new ObjectMap("index", index), sessionId);
    }

    /**
     *
     * @param study                     Study where file is located
     * @param inputFile                 File to be indexed
     * @param sampleList
     * @param outDirId
     * @param outDirUri                 Index outdir
     * @param randomString
     * @param indexAttributes           Attributes of the index object
     * @param dataStore
     * @return                  CommandLine
     *
     * @throws CatalogException
     */
    private String createCommandLine(Study study, File originalFile, File inputFile, List<Sample> sampleList,
                                     long outDirId, URI outDirUri, String randomString, final ObjectMap indexAttributes, final DataStore dataStore,
                                     String sessionId, QueryOptions options)
            throws CatalogException {

        //Create command line
//        String userId = inputFile.getOwnerId();
        String name = originalFile.getName();
        String commandLine;


//        String opencgaStorageBin = Paths.get(Config.getOpenCGAHome(), "bin", OPENCGA_STORAGE_BIN_NAME).toString();
        String opencgaAnalysisBin = Paths.get(Config.getOpenCGAHome(), "bin", OPENCGA_ANALYSIS_BIN_NAME).toString();
        if(originalFile.getBioformat() == File.Bioformat.ALIGNMENT || name.endsWith(".bam") || name.endsWith(".sam")) {
            int chunkSize = 200;    //TODO: Read from properties.
            StringBuilder sb = new StringBuilder(opencgaAnalysisBin)
                    .append(" alignment index ")
                    .append(" --file-id ").append(originalFile.getId())
                    .append(" --outdir-id ").append(outDirId)
                    .append(" --calculate-coverage ")
                    .append(" --mean-coverage ").append(chunkSize)
                    .append(" --session-id ").append(sessionId)
                    .append(" --job-id ").append(randomString);
//                    .append(" --credentials ")
            if (options.containsKey(LOG_LEVEL)) {
                sb.append(" --log-level ").append(options.getString(LOG_LEVEL));
            }
            commandLine = sb.toString();

            indexAttributes.put("chunkSize", chunkSize);

        } else if (name.endsWith(".fasta") || name.endsWith(".fasta.gz")) {
            throw new UnsupportedOperationException();
        } else if (originalFile.getBioformat() == File.Bioformat.VARIANT || name.contains(".vcf") || name.contains(".vcf.gz")) {

            StringBuilder sb = new StringBuilder(opencgaAnalysisBin)
                    .append(" variant index ")
                    .append(" --file-id ").append(inputFile.getId())
                    .append(" --outdir ").append(outDirId)
                    .append(" --session-id ").append(sessionId)
                    .append(" --job-id ").append(randomString);
            if (options.getBoolean(VariantStorageManager.Options.ANNOTATE.key(), VariantStorageManager.Options.ANNOTATE.defaultValue())) {
                sb.append(" --annotate ");
            }
            if (options.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key(), VariantStorageManager.Options.CALCULATE_STATS.defaultValue())) {
                sb.append(" --calculate-stats ");
            }
            if (options.getBoolean(TRANSFORM, false)) {
                sb.append(" --transform ");
            }
            if (StringUtils.isNotEmpty(options.getString(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key()))) {
                sb.append(" --include-extra-fields ").append(options.getString(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key()));
            }
            if (options.getBoolean(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), VariantStorageManager.Options.EXCLUDE_GENOTYPES.defaultValue())) {
                sb.append(" --exclude-genotypes ");
            }
            if (options.getBoolean(LOAD, false)) {
                sb.append(" --load ");
            }
            if (StringUtils.isNotEmpty(options.getString(LOG_LEVEL))) {
                sb.append(" --log-level ").append(options.getString(LOG_LEVEL));
            }
            if (options.containsKey(VariantStorageManager.Options.AGGREGATED_TYPE.key())) {
                sb.append(" --aggregated ").append(options.getString(VariantStorageManager.Options.AGGREGATED_TYPE.key()));
            }
            commandLine = sb.toString();

        } else {
            return null;
        }

        return commandLine;
    }




    ////AUX METHODS


    public static URI createSimulatedOutDirUri() {
        return createSimulatedOutDirUri("J_" + RandomStringUtils.randomAlphanumeric(10));
    }

    public static URI createSimulatedOutDirUri(String randomString) {
        return Paths.get("/tmp","simulatedJobOutdir" , randomString).toUri();
    }
}
