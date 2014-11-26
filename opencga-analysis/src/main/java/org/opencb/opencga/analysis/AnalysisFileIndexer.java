package org.opencb.opencga.analysis;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 16/10/14.
 *
 * IndexFile (fileId, backend, outDir)
 * 1º check if indexed in the selected backend
 * 2º create outDir (must be a new folder)
 * 3º create command line
 * 4º launch CLI (SGE, get jobId)
 * 5º update fileBean with index info
 * ...
 * 6º find files in outDir
 * 7º update fileBean index info
 *
 *
 * UnIndexFile (fileId, backend)
 * ?????????????????????????????
 *
 */
public class AnalysisFileIndexer {

    private final Properties properties;
    private final CatalogManager catalogManager;

    public AnalysisFileIndexer(CatalogManager catalogManager, Properties properties) {
        this.properties = properties;
        this.catalogManager = catalogManager;
    }

    public AnalysisFileIndexer(java.io.File properties) throws IOException, CatalogManagerException, CatalogIOManagerException {
        this.properties = new Properties();
        this.properties.load(new FileInputStream(properties));
        this.catalogManager = new CatalogManager(this.properties);
    }
    public File index(int fileId, int outDirId, String storageEngine, String sessionId, QueryOptions options)
            throws IOException, CatalogIOManagerException, CatalogManagerException, AnalysisExecutionException {

        String userId = catalogManager.getUserIdBySessionId(sessionId);
        QueryResult<File> fileQueryResult = catalogManager.getFile(fileId, sessionId);
        File file = fileQueryResult.getResult().get(0);

        //TODO: Check if file can be indexed

        // Create temporal Outdir
        int studyId = catalogManager.getStudyIdByFileId(fileId);
        Study study = catalogManager.getStudy(studyId, sessionId).getResult().get(0);
        String randomString = "I_" + StringUtils.randomString(10);
        URI temporalOutDirUri = catalogManager.createJobOutDir(studyId, randomString, sessionId);




        //Create index file
        QueryResult<File> indexQueryResult = catalogManager.createFile(studyId, File.TYPE_INDEX, file.getFormat(),
                file.getBioformat(), file.getPath() + "." + storageEngine, "Indexation of " + file.getName(), false,
                -1, sessionId, null);
        File index = indexQueryResult.getResult().get(0);

        //Create job
        ObjectMap indexAttributes = new ObjectMap();
        indexAttributes.put(Job.TYPE, Job.TYPE_INDEX);
        indexAttributes.put(Job.INDEXED_FILE_ID, index.getId());
        indexAttributes.put(Job.JOB_SCHEDULER_NAME, randomString);
        String commandLine = createCommandLine(userId, study.getName(), studyId, file, storageEngine, temporalOutDirUri,
                indexAttributes);

        QueryResult<Job> jobQueryResult = catalogManager.createJob(studyId, "Indexing file " + file.getPath(),
                "opencga-storage.sh", "", commandLine, temporalOutDirUri, outDirId, Arrays.asList(fileId),
                indexAttributes, sessionId);
        Job job = jobQueryResult.getResult().get(0);

        //Set JobId to IndexFile
        catalogManager.modifyFile(index.getId(), new ObjectMap("jobId", job.getId()), sessionId).getResult();
        index.setJobId(job.getId());

        //Run job
        AnalysisJobExecuter.execute(job);

//        try {
//            SgeManager.queueJob(file.getBioformat() + "_indexer", ((String) job.getResourceManagerAttributes().get(Job.JOB_SCHEDULER_NAME)),
//                    -1, job.getTmpOutDirUri(), commandLine, null, "index." + file.getId());
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw new AnalysisExecutionException(e);
//        }

        return index;
    }


    private String createCommandLine(String userId, String studyName, int studyId, File file, String storageEngine,
                                     URI tmpOutDirUri, ObjectMap attributes)
            throws CatalogManagerException, CatalogIOManagerException {

        //Create command line
        String name = file.getName();
        String commandLine;
        String dbName;

        if(file.getBioformat().equals("bam") || name.endsWith(".bam") || name.endsWith(".sam")) {
            int chunkSize = 200;    //TODO: Read from properties.
            dbName = userId;
            commandLine = new StringBuilder("/opt/opencga/bin/opencga-storage.sh ")
                    .append(" index-alignments ")
                    .append(" --alias ").append(file.getId())
                    .append(" --dbName ").append(dbName)
                    .append(" --input ").append(catalogManager.getFileUri(file))
                    .append(" --mean-coverage ").append(chunkSize)
                    .append(" --outdir ").append(tmpOutDirUri)
                    .append(" --backend ").append(storageEngine)
//                    .append(" --credentials ")
                    .toString();

            attributes.put("chunkSize", chunkSize);

        } else if (name.endsWith(".fasta") || name.endsWith(".fasta.gz")) {
            throw new UnsupportedOperationException();
        } else if (file.getBioformat().equals("vcf") || name.endsWith(".vcf") || name.endsWith(".vcf.gz")) {

            dbName = userId;  //TODO: Read from properties
            commandLine = new StringBuilder("/opt/opencga/bin/opencga-storage.sh ")
                    .append(" index-variants ")
                    .append(" --alias ").append(file.getId())
                    .append(" --study ").append(studyName)
                    .append(" --study-alias ").append(studyId)
                    .append(" --dbName ").append(dbName)
                    .append(" --input ").append(catalogManager.getFileUri(file).getPath())  //TODO: Make URI-compatible
                    .append(" --outdir ").append(tmpOutDirUri.getPath())                    //TODO: Make URI-compatible
                    .append(" --backend ").append(storageEngine)
                    .append(" --include-samples ")
                    .append(" --include-stats ")
//                    .append(" --credentials ")
                    .toString();

        } else {
            return null;
        }
        attributes.put("dbName", dbName);
        attributes.put("storageEngine", storageEngine);

        return commandLine;
    }


//    public Index index(int fileId, int outDirId, String storageEngine, String sessionId, QueryOptions options)
//            throws IOException, CatalogIOManagerException, CatalogManagerException {
//        QueryResult<File> fileResult = catalogManager.getFile(fileId, sessionId);
//        File file = fileResult.getResult().get(0);
//        String jobId = "I_"+StringUtils.randomString(15);
//
//        //1º Check indexed
//        List<Index> indices = file.getIndices();
//        for (Index index : indices) {
//            if(index.getStorageEngine().equals(storageEngine)){
//                throw new IOException("File {name:'" + file.getName() + "', id:" + file.getId() + "} already indexed. " + index + "" );
//            }
//        }
//
//        int studyId = catalogManager.getStudyIdByFileId(fileId);
//        String studyName = catalogManager.getStudy(studyId, sessionId).getResult().get(0).getName();
//        String userId = catalogManager.getUserIdBySessionId(sessionId);
//        String ownerId = catalogManager.getFileOwner(fileId);
//        File outDir = catalogManager.getFile(outDirId, sessionId).getResult().get(0);
//
//        //2º Create outdir
//        Path tmpOutDirPath = Paths.get("jobs", jobId); //TODO: Create job folder outside the user workspace.
//        File tmpOutDir = catalogManager.createFolder(studyId, tmpOutDirPath, false, sessionId).getResult().get(0);
//        URI tmpOutDirUri = catalogManager.getFileUri(tmpOutDir);
//
//        //3º Create command line
//        String name = file.getName();
//        String commandLine;
//        String dbName;
//        ObjectMap attributes = new ObjectMap();
//
//        if(file.getBioformat().equals("bam") || name.endsWith(".bam") || name.endsWith(".sam")) {
//            int chunkSize = 200;    //TODO: Read from properties.
//            dbName = ownerId;
//            commandLine = new StringBuilder("/opt/opencga/bin/opencga-storage.sh ")
//                    .append(" index-alignments ")
//                    .append(" --alias ").append(file.getId())
//                    .append(" --dbName ").append(dbName)
//                    .append(" --input ").append(catalogManager.getFileUri(file))
//                    .append(" --mean-coverage ").append(chunkSize)
//                    .append(" --outdir ").append(tmpOutDirUri)
//                    .append(" --backend ").append(storageEngine)
////                    .append(" --credentials ")
//                    .toString();
//
//            attributes.put("chunkSize", chunkSize);
//
//        } else if (name.endsWith(".fasta") || name.endsWith(".fasta.gz")) {
//            throw new UnsupportedOperationException();
//        } else if (file.getBioformat().equals("vcf") || name.endsWith(".vcf") || name.endsWith(".vcf.gz")) {
//
//            dbName = "variants";  //TODO: Read from properties
//            commandLine = new StringBuilder("/opt/opencga/bin/opencga-storage.sh ")
//                    .append(" index-variants ")
//                    .append(" --alias ").append(file.getId())
//                    .append(" --study ").append(studyName)
//                    .append(" --study-alias ").append(studyId)
////                    .append(" --dbName ").append(dbName)  //TODO: Add --dbName option
//                    .append(" --input ").append(catalogManager.getFileUri(file).getPath())  //TODO: Make URI-compatible
//                    .append(" --outdir ").append(tmpOutDirUri.getPath())                    //TODO: Make URI-compatible
//                    .append(" --backend ").append(storageEngine)
//                    .append(" --include-samples ")
//                    .append(" --include-stats ")
////                    .append(" --credentials ")
//                    .toString();
//
//        } else {
//            return null;
//        }
//
//        //4º Run command
//        try {
//            SgeManager.queueJob(file.getBioformat() + "_indexer", jobId, -1, tmpOutDirUri.getPath(),
//                    commandLine, null, "index." + file.getId());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
////            int jobId = new Random(System.nanoTime()).nextInt();
//
//        //5º Update file
//        attributes.put("commandLine", commandLine);
//        Index index = new Index(Index.PENDING, dbName, storageEngine, jobId, new HashMap<String, Object>(), attributes);
//        catalogManager.setIndexFile(fileId, storageEngine, index, sessionId);
//
//        return index;
//
//    }


}
