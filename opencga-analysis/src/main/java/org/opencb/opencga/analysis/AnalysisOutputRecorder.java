package org.opencb.opencga.analysis;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogFileManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Index;
import org.opencb.opencga.catalog.beans.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jacobo on 4/11/14.
 *
 *  Modifies the job status to PROCESSING_OUTPUT
 *  Scans the temporal output directory from a job to find all generated files.
 *  Records the output in CatalogManager
 *  Moves the file to the read output
 *  Modifies the job status to READY and set the output and endTime.
 */

public class AnalysisOutputRecorder {


    private static Logger logger = LoggerFactory.getLogger(AnalysisOutputRecorder.class);
    private final CatalogManager catalogManager;
    private final String sessionId;
    private final String policy = "delete";
    private final boolean calculateChecksum = false;

    public AnalysisOutputRecorder(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    public void recordJobOutput(Job job) {

        List<Integer> fileIds = new LinkedList<>();
        CatalogFileManager catalogFileManager = new CatalogFileManager(catalogManager);

//        try {
//            /** Modify job status to PROCESSING_OUTPUT **/
//            logger.debug("Modify job {id: {}, status:{}} status to PROCESSING_OUTPUT", job.getId(), job.getStatus());
//            ObjectMap parameters = new ObjectMap("status", Job.Status.PROCESSING_OUTPUT);
//            catalogManager.modifyJob(job.getId(), parameters, sessionId);
//        } catch (CatalogException e) {
//            e.printStackTrace();
//        }

        try {
            /** Scans the output directory from a job or index to find all files. **/
            URI tmpOutDirUri = job.getTmpOutDirUri();
            logger.debug("Scan the temporal output directory ({}) from a job to find all generated files.", tmpOutDirUri);
            File outDir = catalogManager.getFile(job.getOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);
            List<URI> uris = catalogManager.getCatalogIOManagerFactory().get(tmpOutDirUri).listFiles(tmpOutDirUri);

//            int studyId = catalogManager.getAnalysisIdByJobId(job.getId());
            int studyId = catalogManager.getStudyIdByJobId(job.getId());

            logger.debug("Record the output in CatalogManager.", tmpOutDirUri);
            for (URI uri : uris) {
                String generatedFile = Paths.get(uri).toAbsolutePath().toString().substring(tmpOutDirUri.getPath().length());
                String filePath = Paths.get(outDir.getPath(), generatedFile).toString();
                QueryResult<File> searchFile = catalogManager.searchFile(studyId, new QueryOptions("path", filePath), sessionId);
                if(searchFile.getNumResults() != 0) {
                    File file = searchFile.getResult().get(0);
                    switch (policy) {
                        case "delete":
                            catalogManager.deleteFile(file.getId(), sessionId);
                            break;
                        case "rename":
                            throw new UnsupportedOperationException("Unimplemented policy 'rename'");
                        case "doError":
                            throw new UnsupportedOperationException("Unimplemented policy 'error'");
                    }
                }
                /** Records the output in CatalogManager **/
                QueryResult<File> fileQueryResult = catalogManager.createFile(
                        studyId, File.Format.PLAIN, File.Bioformat.NONE, filePath, "Generated from job " + job.getId(),
                        true, job.getId(), sessionId);

                File file = fileQueryResult.getResult().get(0);
                fileIds.add(file.getId());
                /** Moves the file to the read output **/
                catalogFileManager.upload(uri, file, null, sessionId, false, false, true, calculateChecksum);
            }
        } catch (CatalogException | IOException e) {
            e.printStackTrace();
            logger.error("Error while processing Job", e);
            return;
        }

        /** Modifies the job status to READY and set the output and endTime. **/
        try {
            switch(Job.Type.valueOf(job.getAttributes().get(Job.TYPE).toString())) {
                case INDEX:
                    Integer indexedFileId = (Integer) job.getAttributes().get(Job.INDEXED_FILE_ID);
                    File indexedFile = catalogManager.getFile(indexedFileId, sessionId).first();
                    if (indexedFile.getIndex() != null) {
                        Index index = indexedFile.getIndex();
                        index.setStatus(Index.Status.READY);
                        catalogManager.modifyFile(indexedFileId, new ObjectMap("index", index), sessionId); //Modify status
                    }
                    break;
                case ANALYSIS:
                default:
                    break;
            }
            ObjectMap parameters = new ObjectMap();
            parameters.put("output", fileIds);
            parameters.put("endTime", System.currentTimeMillis());
            catalogManager.modifyJob(job.getId(), parameters, sessionId);

            //TODO: "input" files could be modified by the tool. Have to be scanned, calculate the new Checksum and

        } catch (CatalogException e) {
            e.printStackTrace(); //TODO: Handle exception
        }
    }


/**
    public void recordIndexOutput(Index index) throws CatalogManagerException, IOException, CatalogIOManagerException {
        QueryResult<File> fileResult = catalogManager.getFileByIndexJobId(index.getJobId()); //TODO: sessionId¿?¿?
        if(fileResult.getResult().isEmpty()) {
            return;
        }
        File indexedFile = fileResult.getResult().get(0);

        List<Integer> fileIds = new LinkedList<>();
        CatalogFileManager catalogFileManager = new CatalogFileManager(catalogManager);

        try {
            File outDir = catalogManager.getFile(index.getOutDirId(), sessionId).getResult().get(0);
            URI tmpOutDirUri = URI.create(index.getTmpOutDirUri());
            List<URI> uris = catalogManager.getCatalogIOManagerFactory().get(tmpOutDirUri.getScheme()).listFiles(tmpOutDirUri);
            int studyId = catalogManager.getStudyIdByFileId(indexedFile.getId());

            for (URI uri : uris) {
                String generatedFile = Paths.get(uri).toAbsolutePath().toString().substring(tmpOutDirUri.getPath().length());
                String filePath = Paths.get(outDir.getPath(), generatedFile).toString();
                QueryResult<File> searchFile = catalogManager.searchFile(studyId, new QueryOptions("path", filePath), sessionId);
                if(searchFile.getNumResults() != 0) {
                    File file = searchFile.getResult().get(0);
                    switch (policy) {
                        case "delete":
                            catalogManager.deleteFile(file.getId(), sessionId);
                            break;
                        case "rename":
                            throw new UnsupportedOperationException("Unimplemented policy 'rename'");
                        case "doError":
                            throw new UnsupportedOperationException("Unimplemented policy 'error'");
                    }
                }
                QueryResult<File> fileQueryResult = catalogManager.createFile(
                        studyId, File.TYPE_FILE, "", filePath, "Generated from indexing file " + indexedFile.getId(),
                        true, sessionId);
                File resultFile = fileQueryResult.getResult().get(0);
                fileIds.add(resultFile.getId());
                catalogFileManager.upload(uri, resultFile, null, sessionId, false, false, true, true);
            }
        } catch (CatalogManagerException | InterruptedException | IOException | CatalogIOManagerException e) {
            e.printStackTrace();
            logger.error("Error while processing Job", e);
            return;
        }


        // Update file.attributes
        for (Index auxIndex: indexedFile.getIndices()) {
            if (auxIndex.getJobId().equals(index.getJobId())) {
//                auxIndex.setJobId(""); //Clear the jobId
                auxIndex.setStatus(Index.INDEXED);
//                auxIndex.setOutput(fileIds);
                catalogManager.setIndexFile(indexedFile.getId(), auxIndex.getStorageEngine(), auxIndex, sessionId);
            }
        }
    }
*/

}
