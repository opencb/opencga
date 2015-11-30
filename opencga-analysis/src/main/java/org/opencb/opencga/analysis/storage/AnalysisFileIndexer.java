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

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.ToolManager;
import org.opencb.opencga.analysis.JobFactory;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.catalog.db.api.CatalogSampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.StringUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
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
public class AnalysisFileIndexer {

    //Properties
    public static final String OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX = "OPENCGA.ANALYSIS.STORAGE.DATABASE_PREFIX";

    //Options
    public static final String PARAMETERS = "parameters";
    public static final String TRANSFORM = "transform";
    public static final String LOAD = "load";
    public static final String LOG_LEVEL = "logLevel";


    //Other
    public static final String OPENCGA_STORAGE_BIN_NAME = "opencga-storage.sh";

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
    public QueryResult<Job> index(int fileId, int outDirId, String sessionId, QueryOptions options)
            throws IOException, CatalogException, AnalysisExecutionException {

        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(ToolManager.EXECUTE);
        final boolean simulate = options.getBoolean(ToolManager.SIMULATE);
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
        int studyIdByOutDirId = catalogManager.getStudyIdByFileId(outDirId);
        Study study = catalogManager.getStudy(studyIdByOutDirId, sessionId).getResult().get(0);

        if (inputFile.getType() != File.Type.FILE) {
            throw new CatalogException("Expected file type = " + File.Type.FILE + " instead of " + inputFile.getType());
        }

        /** Get the original file. **/
        if (!transform && load) { //Don't transform. Just load. Select the original file
            if (inputFile.getJobId() <= 0) {
                throw new CatalogException("Error: can't load this file. JobId unknown. Need jobId to know origin file. " +
                        "Only transformed files can be loaded.");
            }
            Job job = catalogManager.getJob(inputFile.getJobId(), null, sessionId).first();
            int indexedFileId;
            if (job.getAttributes().containsKey(Job.INDEXED_FILE_ID)) {
                indexedFileId = new ObjectMap(job.getAttributes()).getInt(Job.INDEXED_FILE_ID);
            } else {
                logger.warn("INDEXED_FILE_ID missing in job " + job.getId());
                List<Integer> jobInputFiles = job.getInput();
                if (jobInputFiles.size() != 1) {
                    throw new CatalogException("Error: Job {id: " + job.getId() + "} input is empty");
                }
                indexedFileId = jobInputFiles.get(0);
            }
            originalFile = catalogManager.getFile(indexedFileId, null, sessionId).first();
            if (originalFile.getStatus() != File.Status.READY) {
                throw new CatalogException("Error: Original file status must be \"READY\", not \"" + originalFile.getStatus() + "\"");
            }
        } else {
            originalFile = inputFile;
        }

        final DataStore dataStore = getDataStore(catalogManager, catalogManager.getStudyIdByFileId(originalFile.getId()), originalFile.getBioformat(), sessionId);

        /** Check if file can be indexed **/
        if (originalFile.getIndex() != null) {
            switch (originalFile.getIndex().getStatus()) {
                case TRANSFORMING:
                    throw new CatalogException("File '" + originalFile.getId() + "' it's being transformed");
                case TRANSFORMED:
                    if (transform) {
                        throw new CatalogException("File '" + originalFile.getId() + "' is already transformed");
                    }
                    break;
                case LOADING:
                    throw new CatalogException("File '" + originalFile.getId() + "' it's being loaded");
                case INDEXING:
                    throw new CatalogException("File '" + originalFile.getId() + "' it's being indexed");
                case READY:
                    throw new CatalogException("File '" + originalFile.getId() + "' is already indexed");
                case NONE:
                    break;
            }
        } else {
            if (!transform && load) {
                throw new CatalogException("File '" + originalFile.getId() + "' need to be transformed before loading");
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
        final String randomString = "I_" + StringUtils.randomString(10);
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
            } catch (CatalogException | StorageManagerException e) {
                throw new AnalysisExecutionException(e);
            }
        } else {
            sampleList = catalogManager.getAllSamples(study.getId(), new QueryOptions("id", originalFile.getSampleIds()), sessionId).getResult();
        }
        if (!simulate) {
            Cohort defaultCohort = null;
            QueryResult<Cohort> cohorts = catalogManager.getAllCohorts(studyIdByOutDirId, new QueryOptions(CatalogSampleDBAdaptor.CohortFilterOption.name.toString(), StudyEntry.DEFAULT_COHORT), sessionId);
            if (cohorts.getResult().isEmpty()) {
                defaultCohort = catalogManager.createCohort(studyIdByOutDirId, StudyEntry.DEFAULT_COHORT, Cohort.Type.COLLECTION, "Default cohort with almost all indexed samples", Collections.<Integer>emptyList(), null, sessionId).first();
            } else {
                defaultCohort = cohorts.first();
            }

            //Samples are the already indexed plus those that are going to be indexed
            Set<Integer> samples = new HashSet<>(defaultCohort.getSamples());
            samples.addAll(sampleList.stream().map(Sample::getId).collect(Collectors.toList()));
            if (samples.size() != defaultCohort.getSamples().size()) {
                logger.debug("Updating \"{}\" cohort", StudyEntry.DEFAULT_COHORT);
                catalogManager.modifyCohort(defaultCohort.getId(), new ObjectMap("samples", new ArrayList<>(samples)), sessionId);
            }
        }


        /** Create commandLine **/
        String commandLine = createCommandLine(study, originalFile, inputFile, sampleList,
                temporalOutDirUri, indexAttributes, dataStore, sessionId, options);
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
        Index indexInformation = originalFile.getIndex();
        if (indexInformation == null) {
            indexInformation = new Index(userId, TimeUtils.getTime(), Index.Status.NONE, -1, indexAttributes);
        }
        if (transform && !load) {
            indexInformation.setStatus(Index.Status.TRANSFORMING);
        } else if (!transform && load) {
            indexInformation.setStatus(Index.Status.LOADING);
        } else if (transform && load) {
            indexInformation.setStatus(Index.Status.INDEXING);
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
        switch (indexInformation.getStatus()) {
            default:
//                throw new IllegalStateException("Unexpected state");
            case INDEXING:
                jobName = "index";
                jobDescription = "Indexing file " + originalFile.getName() + " (" + originalFile.getId() + ")";
                break;
            case LOADING:
                jobName = "load";
                jobDescription = "Loading file " + originalFile.getName() + " (" + originalFile.getId() + ")";
                break;
            case TRANSFORMING:
                jobName = "transform";
                jobDescription = "Transforming file " + originalFile.getName() + " (" + originalFile.getId() + ")";
                break;
        }
        JobFactory jobFactory = new JobFactory(catalogManager);
        final Job job = jobFactory.createJob(studyIdByOutDirId, jobName,
                OPENCGA_STORAGE_BIN_NAME, jobDescription, outDir, Collections.singletonList(inputFile.getId()),
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


    private void modifyIndexJobId(int fileId, int jobId, boolean transform, boolean load, String sessionId) throws CatalogException {
        File file = catalogManager.getFile(fileId, sessionId).first();
        Index index = file.getIndex();
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

    public static DataStore getDataStore(CatalogManager catalogManager, int studyId, File.Bioformat bioformat, String sessionId) throws CatalogException {
        Study study = catalogManager.getStudy(studyId, sessionId).first();
        DataStore dataStore;
        if (study.getDataStores() != null && study.getDataStores().containsKey(bioformat)) {
            dataStore = study.getDataStores().get(bioformat);
        } else {
            int projectId = catalogManager.getProjectIdByStudyId(study.getId());
            Project project = catalogManager.getProject(projectId, new QueryOptions("include", Arrays.asList("projects.alias", "projects.dataStores")), sessionId).first();
            if (project.getDataStores() != null && project.getDataStores().containsKey(bioformat)) {
                dataStore = project.getDataStores().get(bioformat);
            } else { //get default datastore
                String userId = catalogManager.getUserIdByStudyId(studyId); //Must use the UserByStudyId instead of the file owner.
                String alias = project.getAlias();
                dataStore = new DataStore(StorageManagerFactory.get().getDefaultStorageManagerName(), Config.getAnalysisProperties().getProperty(OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX, "opencga_") + userId + "_" + alias);
            }
        }
        return dataStore;
    }

    /**
     *
     * @param study                     Study where file is located
     * @param inputFile                 File to be indexed
     * @param sampleList
     * @param outDirUri                 Index outdir
     * @param indexAttributes           Attributes of the index object
     * @param dataStore
     * @return                  CommandLine
     *
     * @throws org.opencb.opencga.catalog.exceptions.CatalogDBException
     * @throws org.opencb.opencga.catalog.exceptions.CatalogIOException
     */

    private String createCommandLine(Study study, File originalFile, File inputFile, List<Sample> sampleList,
                                     URI outDirUri, final ObjectMap indexAttributes, final DataStore dataStore,
                                     String sessionId, QueryOptions options)
            throws CatalogException {

        //Create command line
//        String userId = inputFile.getOwnerId();
        String name = originalFile.getName();
        String commandLine;

        String opencgaStorageBin = Paths.get(Config.getOpenCGAHome(), "bin", OPENCGA_STORAGE_BIN_NAME).toString();

        if(originalFile.getBioformat() == File.Bioformat.ALIGNMENT || name.endsWith(".bam") || name.endsWith(".sam")) {
            int chunkSize = 200;    //TODO: Read from properties.
            StringBuilder sb = new StringBuilder(opencgaStorageBin)
                    .append(" index-alignments ")
                    .append(" --storage-engine ").append(dataStore.getStorageEngine())
                    .append(" --file-id ").append(originalFile.getId())
                    .append(" --database ").append(dataStore.getDbName())
                    .append(" --input ").append(catalogManager.getFileUri(inputFile))
                    .append(" --calculate-coverage ")
                    .append(" --mean-coverage ").append(chunkSize)
                    .append(" --outdir ").append(outDirUri);
//                    .append(" --credentials ")
            if (options.containsKey(LOG_LEVEL)) {
                sb.append(" --log-level ").append(options.getString(LOG_LEVEL));
            }
            commandLine = sb.toString();

            indexAttributes.put("chunkSize", chunkSize);

        } else if (name.endsWith(".fasta") || name.endsWith(".fasta.gz")) {
            throw new UnsupportedOperationException();
        } else if (originalFile.getBioformat() == File.Bioformat.VARIANT || name.contains(".vcf") || name.contains(".vcf.gz")) {

//            StringBuilder sampleIdsString = new StringBuilder();
//            for (Sample sample : sampleList) {
//                sampleIdsString.append(sample.getName()).append(":").append(sample.getId()).append(",");
//            }

            int projectId = catalogManager.getProjectIdByStudyId(study.getId());
            String projectAlias = catalogManager.getProject(projectId, null, sessionId).first().getAlias();
            String userId = catalogManager.getUserIdByProjectId(projectId);

            StringBuilder sb = new StringBuilder(opencgaStorageBin)
                    .append(" index-variants ")
                    .append(" --storage-engine ").append(dataStore.getStorageEngine())
                    .append(" --file-id ").append(originalFile.getId())
//                    .append(" --study-name \'").append(study.getName()).append("\'")
                    .append(" --study-name \'").append(userId).append("@").append(projectAlias).append(":").append(study.getAlias()).append("\'")
                    .append(" --study-id ").append(study.getId())
//                    .append(" --study-type ").append(study.getType())
                    .append(" --database ").append(dataStore.getDbName())
                    .append(" --input ").append(catalogManager.getFileUri(inputFile))
                    .append(" --outdir ").append(outDirUri)
//                    .append(" -D").append(VariantStorageManager.Options.STUDY_CONFIGURATION_MANAGER_CLASS_NAME.key()).append("=").append(CatalogStudyConfigurationManager.class.getName())
//                    .append(" -D").append("sessionId").append("=").append(sessionId)
//                    .append(" --sample-ids ").append(sampleIdsString)
//                    .append(" --credentials ")
                    ;
            if (options.getBoolean(VariantStorageManager.Options.ANNOTATE.key(), VariantStorageManager.Options.ANNOTATE.defaultValue())) {
                sb.append(" --annotate ");
            }
            if (options.getBoolean(VariantStorageManager.Options.CALCULATE_STATS.key(), VariantStorageManager.Options.CALCULATE_STATS.defaultValue())) {
                sb.append(" --calculate-stats ");
            }
            if (options.getBoolean(VariantStorageManager.Options.INCLUDE_SRC.key(), false)) {
                sb.append(" --include-src ");
            }
            if (options.getBoolean(TRANSFORM, false)) {
                sb.append(" --transform ");
            }
            if (options.getBoolean(VariantStorageManager.Options.INCLUDE_GENOTYPES.key(), VariantStorageManager.Options.INCLUDE_GENOTYPES.defaultValue())) {
                sb.append(" --include-genotypes ");
            }
            if (!options.getString(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), "").isEmpty()) {
                sb.append(" --include-extra-fields ").append(options.getString(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key()));
            }
            if (options.getBoolean(LOAD, false)) {
                sb.append(" --load ");
            }
            if (options.containsKey(LOG_LEVEL)) {
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
        return createSimulatedOutDirUri("J_" + StringUtils.randomString(10));
    }

    public static URI createSimulatedOutDirUri(String randomString) {
        return Paths.get("/tmp","simulatedJobOutdir" , randomString).toUri();
    }
}
