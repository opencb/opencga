package org.opencb.opencga.analysis.storage;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecuter;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.common.TimeUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

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
    public static final String DB_NAME = "dbName";
    public static final String PARAMETERS = "parameters";
    public static final String CREATE_MISSING_SAMPLES = "createMissingSamples";
    public static final String TRANSFORM = "transform";
    public static final String LOAD = "load";


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
     * @param storageEngine     Storage engine name where index the file
     * @param sessionId         User session Id
     * @param options           Other options
     * @return                  Generated job for the indexation
     * @throws IOException
     * @throws CatalogException
     * @throws AnalysisExecutionException
     */
    public QueryResult<Job> index(int fileId, int outDirId, String storageEngine, String sessionId, QueryOptions options)
            throws IOException, CatalogException, AnalysisExecutionException {

        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(AnalysisJobExecuter.EXECUTE);
        final boolean simulate = options.getBoolean(AnalysisJobExecuter.SIMULATE);
        final boolean recordOutput = options.getBoolean(AnalysisJobExecuter.RECORD_OUTPUT);
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

        if (!transform) { //Don't transform. Just load. Select the original file
            if (inputFile.getJobId() <= 0) {
                throw new CatalogException("Error: can't load this file. JobId unknown");
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

        final String dbName;
        if (options.containsKey(DB_NAME)) {
            dbName = options.getString(DB_NAME);
        } else {
            if (study.getAttributes().containsKey(DB_NAME) && study.getAttributes().get(DB_NAME) != null) {
                dbName = study.getAttributes().get(DB_NAME).toString();
            } else {
                int projectId = catalogManager.getProjectIdByStudyId(study.getId());
                String alias = catalogManager.getProject(projectId, new QueryOptions("include", "alias"), sessionId).first().getAlias();
                dbName = Config.getAnalysisProperties().getProperty(OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX, "opencga_") + userId + "_" + alias;
            }
        }

        //TODO: Check if file can be indexed

        // ObjectMap to fill with modifications over the indexed file (like new attributes or jobId)
        ObjectMap fileModifyParams = new ObjectMap("attributes", new ObjectMap());
        ObjectMap indexAttributes = new ObjectMap();

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
            sampleList = getFileSamples(study, originalFile, fileModifyParams, simulate, options, sessionId);
        } else {
            sampleList = catalogManager.getAllSamples(study.getId(), new QueryOptions("id", originalFile.getSampleIds()), sessionId).getResult();
        }


        /** Create commandLine **/
        String commandLine = createCommandLine(study, originalFile, inputFile, sampleList, storageEngine,
                temporalOutDirUri, indexAttributes, dbName, options);
        if (options.containsKey(PARAMETERS)) {
            List<String> extraParams = options.getAsStringList(PARAMETERS);
            for (String extraParam : extraParams) {
                commandLine += " " + extraParam;
            }
        }

        /** Create job **/
        ObjectMap jobResourceManagerAttributes = new ObjectMap();
        jobResourceManagerAttributes.put(Job.TYPE, Job.Type.INDEX);
        jobResourceManagerAttributes.put(Job.INDEXED_FILE_ID, originalFile.getId());

        String jobName = "index";
        String jobDescription = "Indexing file " + originalFile.getName() + " (" + originalFile.getId() + ")";
        final Job job = AnalysisJobExecuter.createJob(catalogManager, studyIdByOutDirId, jobName,
                OPENCGA_STORAGE_BIN_NAME, jobDescription, outDir, Collections.singletonList(inputFile.getId()),
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate, recordOutput, jobResourceManagerAttributes).first();

        /** Create index information only if it's going to be loaded **/
        if (load) {
            Index indexInformation = new Index(userId, storageEngine, job.getId(), Collections.singletonMap(DB_NAME, dbName), Collections.emptyMap());
            fileModifyParams.put("index", indexInformation);
        }

        /** Modify file with new information **/
        catalogManager.modifyFile(inputFile.getId(), fileModifyParams, sessionId).getResult();


        if (simulate) {
            return new QueryResult<>("indexFile", (int) (System.currentTimeMillis() - start), 1, 1, "", "", Collections.singletonList(job));
        } else {
            return new QueryResult<>("indexFile", (int) (System.currentTimeMillis() - start), 1, 1, "", "",
                    catalogManager.getJob(job.getId(), null, sessionId).getResult());
        }
    }

    /**
     *
     * @param study                     Study where file is located
     * @param inputFile                 File to be indexed
     * @param sampleList
     * @param storageEngine             StorageEngine to be used
     * @param outDirUri                 Index outdir
     * @param indexAttributes           Attributes of the index object
     * @param dbName
     * @return                  CommandLine
     *
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     * @throws CatalogIOManagerException
     */
    private String createCommandLine(Study study, File originalFile, File inputFile, List<Sample> sampleList, String storageEngine,
                                     URI outDirUri, final ObjectMap indexAttributes, final String dbName, QueryOptions options)
            throws CatalogException {

        //Create command line
//        String userId = inputFile.getOwnerId();
        String name = originalFile.getName();
        String commandLine;

        String opencgaStorageBin = Paths.get(Config.getOpenCGAHome(), "bin", OPENCGA_STORAGE_BIN_NAME).toString();

        if(originalFile.getBioformat() == File.Bioformat.ALIGNMENT || name.endsWith(".bam") || name.endsWith(".sam")) {
            int chunkSize = 200;    //TODO: Read from properties.
            commandLine = new StringBuilder(opencgaStorageBin)
                    .append(" --storage-engine ").append(storageEngine)
                    .append(" index-alignments ")
                    .append(" --file-id ").append(originalFile.getId())
                    .append(" --database ").append(dbName)
                    .append(" --input ").append(catalogManager.getFileUri(inputFile))
                    .append(" --calculate-coverage ").append(chunkSize)
                    .append(" --mean-coverage ").append(chunkSize)
                    .append(" --outdir ").append(outDirUri)
//                    .append(" --credentials ")
                    .toString();

            indexAttributes.put("chunkSize", chunkSize);

        } else if (name.endsWith(".fasta") || name.endsWith(".fasta.gz")) {
            throw new UnsupportedOperationException();
        } else if (originalFile.getBioformat() == File.Bioformat.VARIANT || name.contains(".vcf") || name.contains(".vcf.gz")) {

            StringBuilder sampleIdsString = new StringBuilder();
            for (Sample sample : sampleList) {
                sampleIdsString.append(sample.getName()).append(":").append(sample.getId()).append(",");
            }

            StringBuilder sb = new StringBuilder(opencgaStorageBin)
                    .append(" --storage-engine ").append(storageEngine)
                    .append(" index-variants ")
                    .append(" --file-id ").append(originalFile.getId())
                    .append(" --study-name \'").append(study.getName()).append("\'")
                    .append(" --study-id ").append(study.getId())
//                    .append(" --study-type ").append(study.getType())
                    .append(" --database ").append(dbName)
                    .append(" --input ").append(catalogManager.getFileUri(inputFile))
                    .append(" --outdir ").append(outDirUri)
                    .append(" --include-genotypes ")
                    .append(" --compress-genotypes ")
                    .append(" --include-stats ")
//                    .append(" --sample-ids ").append(sampleIdsString)
//                    .append(" --credentials ")
                    ;
            if (options.getBoolean(VariantStorageManager.ANNOTATE, true)) {
                sb.append(" --annotate ");
            }
            if (options.getBoolean(VariantStorageManager.INCLUDE_SRC, false)) {
                sb.append(" --include-src ");
            }
            if (options.getBoolean(TRANSFORM, false)) {
                sb.append(" --transform ");
            }
            if (options.getBoolean(LOAD, false)) {
                sb.append(" --load ");
            }
            commandLine = sb.toString();

        } else {
            return null;
        }

        return commandLine;
    }




    ////AUX METHODS

    private List<Sample> getFileSamples(Study study, File file, ObjectMap fileModifyParams, boolean simulate, QueryOptions options, String sessionId)
            throws CatalogException {
        List<Sample> sampleList;
        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("projects.studies.samples.id","projects.studies.samples.name"));

        if (file.getSampleIds() == null || file.getSampleIds().isEmpty()) {
            //Read samples from file
            List<String> sampleNames = null;
            switch (file.getBioformat()) {
                case VARIANT: {
                    if (file.getAttributes().containsKey("variantSource")) {
                        Object variantSource = file.getAttributes().get("variantSource");
                        if (variantSource instanceof VariantSource) {
                            sampleNames = ((VariantSource) variantSource).getSamples();
                        } else if (variantSource instanceof Map) {
                            sampleNames = new ObjectMap((Map) variantSource).getAsStringList("samples");
                        } else {
                            logger.warn("Unexpected object type of variantSource ({}) in file attributes. Expected {} or {}", variantSource.getClass(), VariantSource.class, Map.class);
                        }
                    }
                    if (sampleNames == null) {
                        VariantSource variantSource = readVariantSource(catalogManager, study, file);
                        fileModifyParams.get("attributes", ObjectMap.class).put("variantSource", variantSource);
                        sampleNames = variantSource.getSamples();
                    }
                }
                break;
                default:
                    return new LinkedList<>();
//                    throw new CatalogException("Unknown to get samples names from bioformat " + file.getBioformat());
            }

            //Find matching samples in catalog with the sampleName from the VariantSource.
            queryOptions.add("name", sampleNames);
            sampleList = catalogManager.getAllSamples(study.getId(), queryOptions, sessionId).getResult();

            //check if all file samples exists on Catalog
            if (sampleList.size() != sampleNames.size()) {   //Size does not match. Find the missing samples.
                Set<String> set = new HashSet<>(sampleNames);
                for (Sample sample : sampleList) {
                    set.remove(sample.getName());
                }
                logger.warn("Missing samples: m{}", set);
                if (options.getBoolean(CREATE_MISSING_SAMPLES, true)) {
                    for (String sampleName : set) {
                        if (simulate) {
                            sampleList.add(new Sample(-1, sampleName, file.getName(), null, null));
                        } else {
                            sampleList.add(catalogManager.createSample(study.getId(), sampleName, file.getName(), null, null, null, sessionId).first());
                        }
                    }
                } else {
                    throw new CatalogException("Can not find samples " + set + " in catalog"); //FIXME: Create missing samples??
                }
            }
        } else {
            //Get samples from file.sampleIds
            queryOptions.add("id", file.getSampleIds());
            sampleList = catalogManager.getAllSamples(study.getId(), queryOptions, sessionId).getResult();
        }

        List<Integer> sampleIdsList = new ArrayList<>(sampleList.size());
        for (Sample sample : sampleList) {
            sampleIdsList.add(sample.getId());
//                sampleIdsString.append(sample.getName()).append(":").append(sample.getId()).append(",");
        }
        fileModifyParams.put("sampleIds", sampleIdsList);

        return sampleList;
    }

    static public VariantSource readVariantSource(CatalogManager catalogManager, Study study, File file) throws CatalogException{
        //TODO: Fix aggregate and studyType
        VariantSource source = new VariantSource(file.getName(), Integer.toString(file.getId()), Integer.toString(study.getId()), study.getName());
        URI fileUri = catalogManager.getFileUri(file);
        return VariantStorageManager.readVariantSource(Paths.get(fileUri.getPath()), source);
    }

    public static URI createSimulatedOutDirUri() {
        return createSimulatedOutDirUri("J_" + StringUtils.randomString(10));
    }

    public static URI createSimulatedOutDirUri(String randomString) {
        return Paths.get("/tmp","simulatedJobOutdir" , randomString).toUri();
    }
}
