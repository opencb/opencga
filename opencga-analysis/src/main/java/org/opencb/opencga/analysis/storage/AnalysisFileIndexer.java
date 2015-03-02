package org.opencb.opencga.analysis.storage;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by jacobo on 16/10/14.
 *
 * IndexFile (fileId, backend, outDir)
 * - check if indexed in the selected backend
 * - create temporal outDir (must be a new folder)
 * - create command line
 * - create index file
 * - create job
 * - update file with job info
 * - execute
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

    private final Properties properties;
    private final CatalogManager catalogManager;

    public AnalysisFileIndexer(CatalogManager catalogManager, Properties properties) {
        this.properties = properties;
        this.catalogManager = catalogManager;
    }

    public AnalysisFileIndexer(java.io.File properties) throws IOException, CatalogDBException, CatalogIOManagerException {
        this.properties = new Properties();
        this.properties.load(new FileInputStream(properties));
        this.catalogManager = new CatalogManager(this.properties);
    }

    public File index(int fileId, int outDirId, String storageEngine, String sessionId, QueryOptions options)
            throws IOException, CatalogException, AnalysisExecutionException {

        String userId = catalogManager.getUserIdBySessionId(sessionId);
        QueryResult<File> fileQueryResult = catalogManager.getFile(fileId, sessionId);
        QueryResult<File> outdirQueryResult = catalogManager.getFile(outDirId, sessionId);
        File file = fileQueryResult.getResult().get(0);
        File outdir = outdirQueryResult.getResult().get(0);

        //TODO: Check if file can be indexed

        // Create temporal Outdir
        //int studyId = catalogManager.getStudyIdByFileId(fileId);
        int studyIdByOutDirId = catalogManager.getStudyIdByFileId(outDirId);
        Study study = catalogManager.getStudy(studyIdByOutDirId, sessionId).getResult().get(0);
        String randomString = "I_" + StringUtils.randomString(10);
        URI temporalOutDirUri = catalogManager.createJobOutDir(studyIdByOutDirId, randomString, sessionId);


        //Create index file
        QueryResult<File> indexQueryResult = catalogManager.createFile(studyIdByOutDirId, File.Type.INDEX, file.getFormat(),
                file.getBioformat(), Paths.get(outdir.getPath(), file.getName()).toString() + "." + storageEngine, null, null,
                "Indexation of " + file.getName() + " (" + fileId + ")", File.Status.INDEXING, 0, -1, null, -1, null,
                null, false, null, sessionId);
        File index = indexQueryResult.getResult().get(0);

        //Create commandLine
        ObjectMap indexAttributes = new ObjectMap();
        String commandLine = createCommandLine(study, file, index, storageEngine,
                temporalOutDirUri, indexAttributes);

        //Create job
        ObjectMap jobResourceManagerAttributes = new ObjectMap();
        jobResourceManagerAttributes.put(Job.JOB_SCHEDULER_NAME, randomString);
        jobResourceManagerAttributes.put(Job.TYPE, Job.Type.INDEX);
        jobResourceManagerAttributes.put(Job.INDEXED_FILE_ID, index.getId());

        QueryResult<Job> jobQueryResult = catalogManager.createJob(studyIdByOutDirId, "Indexing file " + file.getName() + " (" + fileId + ")",
                "opencga-storage.sh", "", commandLine, temporalOutDirUri, outDirId, Arrays.asList(fileId),
                jobResourceManagerAttributes, null, sessionId);
        Job job = jobQueryResult.getResult().get(0);

        //Set JobId to IndexFile
        ObjectMap objectMap = new ObjectMap("jobId", job.getId());
        objectMap.put("attributes", indexAttributes);
        catalogManager.modifyFile(index.getId(), objectMap, sessionId).getResult();
        index.setJobId(job.getId());
        index.setAttributes(indexAttributes);

        return index;
    }

    /**
     *
     * @param study             Study where file is located
     * @param file              File to be indexed
     * @param indexFile         Generated index file
     * @param storageEngine     StorageEngine to be used
     * @param outDirUri         Index outdir
     * @param indexAttributes   This map will be filled with some index information
     * @return                  CommandLine
     *
     * @throws org.opencb.opencga.catalog.db.CatalogDBException
     * @throws CatalogIOManagerException
     */
    private String createCommandLine(Study study, File file, File indexFile, String storageEngine,
                                     URI outDirUri, ObjectMap indexAttributes)
            throws CatalogDBException, CatalogIOManagerException {

        //Create command line
        String userId = file.getOwnerId();
        String name = file.getName();
        String commandLine;
        String dbName;

        if(file.getBioformat() == File.Bioformat.ALIGNMENT || name.endsWith(".bam") || name.endsWith(".sam")) {
            int chunkSize = 200;    //TODO: Read from properties.
            dbName = userId;
            commandLine = new StringBuilder("/opt/opencga/bin/opencga-storage.sh ")
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

            dbName = userId;
            commandLine = new StringBuilder("/opt/opencga/bin/opencga-storage.sh ")
                    .append(" --storage-engine ").append(storageEngine)
                    .append(" index-variants ")
                    .append(" --file-id ").append(indexFile.getId())
                    .append(" --study-name ").append(study.getName())
                    .append(" --study-id ").append(study.getId())
                    .append(" --study-type ").append(study.getType())
                    .append(" --database ").append(dbName)
                    .append(" --input ").append(catalogManager.getFileUri(file).getPath())  //TODO: Make URI-compatible
                    .append(" --outdir ").append(outDirUri.getPath())                    //TODO: Make URI-compatible
                    .append(" --include-genotypes ")
                    .append(" --include-stats ")
                    .append(" --annotate ")
//                    .append(" --credentials ")
                    .toString();

        } else {
            return null;
        }
        indexAttributes.put(INDEXED_FILE, file.getId());
        indexAttributes.put(DB_NAME, dbName);
        indexAttributes.put(STORAGE_ENGINE, storageEngine);

        return commandLine;
    }

}
