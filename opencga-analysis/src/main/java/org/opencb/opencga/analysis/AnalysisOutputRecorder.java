package org.opencb.opencga.analysis;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogFileManager;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
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
 *  Scans the output directory from a job or index to find all files.
 *  Records the output in CatalogManager
 *  Moves the file to the read output
 */

public class AnalysisOutputRecorder {


    private static Logger logger = LoggerFactory.getLogger(AnalysisOutputRecorder.class);
    private final CatalogManager catalogManager;
    private final String sessionId;
    private final String policy = "delete";

    public AnalysisOutputRecorder(CatalogManager catalogManager, String sessionId) {
        this.catalogManager = catalogManager;
        this.sessionId = sessionId;
    }

    public void recordJobOutput(Job job) {

        List<Integer> fileIds = new LinkedList<>();
        CatalogFileManager catalogFileManager = new CatalogFileManager(catalogManager);

        try {
            URI tmpOutDirUri = job.getTmpOutDirUri();
            File outDir = catalogManager.getFile(job.getOutDirId(), new QueryOptions("path", true), sessionId).getResult().get(0);
            List<URI> uris = catalogManager.getCatalogIOManagerFactory().get(tmpOutDirUri.getScheme()).listFiles(tmpOutDirUri);

//            int studyId = catalogManager.getAnalysisIdByJobId(job.getId());
            int studyId = catalogManager.getStudyIdByJobId(job.getId());

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
                        studyId, File.TYPE_FILE, "", filePath, "Generated from job " + job.getId(),
                        true, job.getId(), sessionId);

                File file = fileQueryResult.getResult().get(0);
                fileIds.add(file.getId());
                catalogFileManager.upload(uri, file, null, sessionId, false, false, true, true);
            }
        } catch (CatalogManagerException | InterruptedException | IOException | CatalogIOManagerException e) {
            e.printStackTrace();
            logger.error("Error while processing Job", e);
            return;
        }

        try {
            ObjectMap parameters = new ObjectMap("status", Job.READY);
            parameters.put("output", fileIds);
            parameters.put("endTime", System.currentTimeMillis());
            catalogManager.modifyJob(job.getId(), parameters, sessionId);
        } catch (CatalogManagerException e) {
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
