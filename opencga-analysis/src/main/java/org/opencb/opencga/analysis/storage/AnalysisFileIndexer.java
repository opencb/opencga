package org.opencb.opencga.analysis.storage;

import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.analysis.AnalysisJobExecuter;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.beans.Sample;
import org.opencb.opencga.catalog.beans.Study;
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
 * - create index file
 * - create command line
 * - create job
 * - update index file
 *
 * ...
 * - find files in outDir
 * - update file and job index info
 *
 *
 * UnIndexFile (fileId, backend)
 * ?????????????????????????????
 *
 */
public class AnalysisFileIndexer {

    //Indexed file attributes
    public static final String INDEXED_FILE = "indexedFile";
    public static final String DB_NAME = "dbName";
    public static final String STORAGE_ENGINE = "storageEngine";

    public static final String OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX = "OPENCGA.ANALYSIS.STORAGE.DATABASE_PREFIX";
    public static final String PARAMETERS = "parameters";
    public static final String OPENCGA_STORAGE_BIN_NAME = "opencga-storage.sh";
    public static final String CREATE_MISSING_SAMPLES = "createMissingSamples";

    private final CatalogManager catalogManager;
    protected static Logger logger = LoggerFactory.getLogger(AnalysisFileIndexer.class);

    public AnalysisFileIndexer(CatalogManager catalogManager, @Deprecated Properties properties) {
        this.catalogManager = catalogManager;
    }

    public AnalysisFileIndexer(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public QueryResult<File> index(int fileId, int outDirId, String storageEngine, String sessionId, QueryOptions options)
            throws IOException, CatalogException, AnalysisExecutionException {

        if (options == null) {
            options = new QueryOptions();
        }
        final boolean execute = options.getBoolean(AnalysisJobExecuter.EXECUTE);
        final boolean simulate = options.getBoolean(AnalysisJobExecuter.SIMULATE);
        final boolean recordOutput = options.getBoolean(AnalysisJobExecuter.RECORD_OUTPUT);
        final long start = System.currentTimeMillis();


        /** Query catalog for user data. **/
        String userId = catalogManager.getUserIdBySessionId(sessionId);
        File file = catalogManager.getFile(fileId, sessionId).first();
        File outDir = catalogManager.getFile(outDirId, sessionId).first();
        int studyIdByOutDirId = catalogManager.getStudyIdByFileId(outDirId);
        Study study = catalogManager.getStudy(studyIdByOutDirId, sessionId).getResult().get(0);

        final String dbName;
        if (options.containsKey(DB_NAME)) {
            dbName = options.getString(DB_NAME);
        } else {
            dbName = Config.getAnalysisProperties().getProperty(OPENCGA_ANALYSIS_STORAGE_DATABASE_PREFIX, "opencga_") + userId;
        }

        //TODO: Check if file can be indexed

        // ObjectMap to fill with modifications over the indexed file (like new attributes or jobId)
        ObjectMap indexFileModifyParams = new ObjectMap("attributes", new ObjectMap());

        /** Get file samples **/
        List<Sample> sampleList = getFileSamples(study, file, indexFileModifyParams, options, sessionId);

        /** Create temporal Job Outdir **/
        final URI temporalOutDirUri;
        final String randomString = "I_" + StringUtils.randomString(10);
        if (simulate) {
            temporalOutDirUri = createSimulatedOutDirUri(randomString);
        } else {
            temporalOutDirUri = catalogManager.createJobOutDir(studyIdByOutDirId, randomString, sessionId);
        }

        /** Create index file**/
        String indexedFileDescription = "Indexation of " + file.getName() + " (" + fileId + ")";
        String indexedFileName = file.getName() + "." + storageEngine;
        String indexedFilePath = Paths.get(outDir.getPath(), indexedFileName).toString();

        final File index;
        if (simulate) {
            index = new File(-10, indexedFileName, File.Type.INDEX, file.getFormat(), file.getBioformat(),
                    indexedFilePath, userId, TimeUtils.getTime(), indexedFileDescription, File.Status.INDEXING, -1, -1,
                    null, -1, null, null, new HashMap<String, Object>());
        } else {
            index = catalogManager.createFile(studyIdByOutDirId, File.Type.INDEX, file.getFormat(),
                    file.getBioformat(), indexedFilePath, null, null,
                    indexedFileDescription, File.Status.INDEXING, 0, -1, null, -1, null,
                    null, false, null, sessionId).first();
        }

        /** Create commandLine **/
        String commandLine = createCommandLine(study, file, index, sampleList, storageEngine,
                temporalOutDirUri, indexFileModifyParams, dbName, options);
        if (options.containsKey(PARAMETERS)) {
            List<String> extraParams = options.getAsStringList(PARAMETERS);
            for (String extraParam : extraParams) {
                commandLine += " " + extraParam;
            }
        }

        /** Create job **/
        ObjectMap jobResourceManagerAttributes = new ObjectMap();
        jobResourceManagerAttributes.put(Job.TYPE, Job.Type.INDEX);
        jobResourceManagerAttributes.put(Job.INDEXED_FILE_ID, index.getId());

        String jobName = "index";
        String jobDescription = "Indexing file " + file.getName() + " (" + fileId + ")";
        final Job job = AnalysisJobExecuter.createJob(catalogManager, studyIdByOutDirId, jobName,
                OPENCGA_STORAGE_BIN_NAME, jobDescription, outDir, Collections.<Integer>emptyList(),
                sessionId, randomString, temporalOutDirUri, commandLine, execute, simulate, recordOutput, jobResourceManagerAttributes).first();

        if (simulate) {
            index.getAttributes().put("job", job);
//            index.getAttributes().putAll(indexFileModifyParams.getMap("attributes"));
            index.setSampleIds(indexFileModifyParams.getAsIntegerList("sampleIds"));
//            VariantSource variantSource = (VariantSource) index.getAttributes().get("variantSource");
//            for (Map.Entry<String, Integer> entry : variantSource.getSamplesPosition().entrySet()) {
//                System.out.println("entry.getKey() = " + entry.getKey());
//                System.out.println("entry.getValue() = " + entry.getValue());
//            }
//            for (String s : variantSource.getSamples()) {
//                System.out.println("sample = " + s);
//            }
//            variantSource.setSamplesPosition(new HashMap<String, Integer>());
            return new QueryResult<>("indexFile", (int) (System.currentTimeMillis() - start), 1, 1, "", "", Collections.singletonList(index));
        } else {
            /** Update IndexFile to add extra information (jobId, sampleIds, attributes, ...) **/
            indexFileModifyParams.put("jobId", job.getId());
            catalogManager.modifyFile(index.getId(), indexFileModifyParams, sessionId).getResult();

            return new QueryResult<>("indexFile", (int) (System.currentTimeMillis() - start), 1, 1, "", "",
                    catalogManager.getFile(index.getId(), sessionId).getResult());
        }
    }

    /**
     *
     * @param study                     Study where file is located
     * @param file                      File to be indexed
     * @param indexFile                 Generated index file
     * @param sampleList
     * @param storageEngine             StorageEngine to be used
     * @param outDirUri                 Index outdir
     * @param indexFileModifyParams     This map will be used to modify the indexFile
     * @param dbName
     * @return                  CommandLine
     *
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     * @throws CatalogIOManagerException
     */
    private String createCommandLine(Study study, File file, File indexFile, List<Sample> sampleList, String storageEngine,
                                     URI outDirUri, final ObjectMap indexFileModifyParams, final String dbName, QueryOptions options)
            throws CatalogDBException, CatalogIOManagerException {

        //Create command line
        String userId = file.getOwnerId();
        String name = file.getName();
        String commandLine;
        ObjectMap indexAttributes = indexFileModifyParams.get("attributes", ObjectMap.class);

        String opencgaStorageBin = Paths.get(Config.getOpenCGAHome(), "bin", OPENCGA_STORAGE_BIN_NAME).toString();

        if(file.getBioformat() == File.Bioformat.ALIGNMENT || name.endsWith(".bam") || name.endsWith(".sam")) {
            int chunkSize = 200;    //TODO: Read from properties.
            commandLine = new StringBuilder(opencgaStorageBin)
                    .append(" --storage-engine ").append(storageEngine)
                    .append(" index-alignments ")
                    .append(" --file-id ").append(indexFile.getId())
                    .append(" --database ").append(dbName)
                    .append(" --input ").append(catalogManager.getFileUri(file))
                    .append(" --calculate-coverage ").append(chunkSize)
                    .append(" --mean-coverage ").append(chunkSize)
                    .append(" --outdir ").append(outDirUri)
//                    .append(" --credentials ")
                    .toString();

            indexAttributes.put("chunkSize", chunkSize);

        } else if (name.endsWith(".fasta") || name.endsWith(".fasta.gz")) {
            throw new UnsupportedOperationException();
        } else if (file.getBioformat() == File.Bioformat.VARIANT || name.endsWith(".vcf") || name.endsWith(".vcf.gz")) {

            StringBuilder sampleIdsString = new StringBuilder();
            for (Sample sample : sampleList) {
                sampleIdsString.append(sample.getName()).append(":").append(sample.getId()).append(",");
            }

            StringBuilder sb = new StringBuilder(opencgaStorageBin)
                    .append(" --storage-engine ").append(storageEngine)
                    .append(" index-variants ")
                    .append(" --file-id ").append(indexFile.getId())
                    .append(" --study-name \"").append(study.getName()).append("\"")
                    .append(" --study-id ").append(study.getId())
//                    .append(" --study-type ").append(study.getType())
                    .append(" --database ").append(dbName)
                    .append(" --input ").append(catalogManager.getFileUri(file))
                    .append(" --outdir ").append(outDirUri)
                    .append(" --include-genotypes ")
                    .append(" --compress-genotypes ")
                    .append(" --include-stats ")
//                    .append(" -DsampleIds=").append(sampleIdsString)
//                    .append(" --credentials ")
                    ;
            if (options.getBoolean(VariantStorageManager.ANNOTATE, true)) {
                sb.append(" --annotate ");
            }
            if (options.getBoolean(VariantStorageManager.INCLUDE_SRC, false)) {
                sb.append(" --include-src ");
            }
            commandLine = sb.toString();

        } else {
            return null;
        }
        indexAttributes.put(INDEXED_FILE, file.getId());
        indexAttributes.put(DB_NAME, dbName);
        indexAttributes.put(STORAGE_ENGINE, storageEngine);

        return commandLine;
    }




    ////AUX METHODS

    private List<Sample> getFileSamples(Study study, File file, ObjectMap indexFileModifyParams, QueryOptions options, String sessionId)
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
                        indexFileModifyParams.get("attributes", ObjectMap.class).put("variantSource", variantSource);
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
                        sampleList.add(catalogManager.createSample(study.getId(), sampleName, file.getName(), null, null, null, sessionId).first());
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
        indexFileModifyParams.put("sampleIds", sampleIdsList);

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
