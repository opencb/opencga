package org.opencb.opencga.analysis.files;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogFileUtils;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileScanner {

    private static Logger logger = LoggerFactory.getLogger(FileScanner.class);

    protected final CatalogManager catalogManager;
    protected final FileScannerPolicy policy;
    protected final boolean calculateChecksum;
    protected final boolean deleteSource;

    private CatalogFileUtils catalogFileUtils;

    public enum FileScannerPolicy {
        DELETE,     //Delete file and file entry. Then create a new one
        REPLACE,    //Delete the file, but not the file entry. Updates the file information.
//        DO_ERROR,
//        RENAME,
    }

    public FileScanner(CatalogManager catalogManager, FileScannerPolicy policy, boolean calculateChecksum, boolean deleteSource) {
        this.catalogManager = catalogManager;
        this.policy = policy;
        this.calculateChecksum = calculateChecksum;
        this.deleteSource = deleteSource;
        catalogFileUtils = new CatalogFileUtils(catalogManager);
    }

    /**
     * Scans the files inside the specified URI and adds to the provided directory
     *
     * @param directory
     * @param directoryToScan
     * @return
     */
    public List<File> scan(File directory, URI directoryToScan, String sessionId) throws IOException, CatalogException {
        if (!directoryToScan.getPath().endsWith("/")) {
            throw new IOException("");
        }
        if (!directory.getType().equals(File.Type.FOLDER)) {
            throw new IOException("");
        }
        int studyId = catalogManager.getStudyIdByFileId(directory.getId());

        List<URI> uris = catalogManager.getCatalogIOManagerFactory().get(directoryToScan).listFiles(directoryToScan);

        List<File> files = new LinkedList<>();
        for (URI uri : uris) {
            boolean ignoreStatus = false;
            boolean overwrite = false;

            String generatedFile = Paths.get(uri).toAbsolutePath().toString().substring(directoryToScan.getPath().length());
            String filePath = Paths.get(directory.getPath(), generatedFile).toString();

            QueryResult<File> searchFile = catalogManager.searchFile(studyId, new QueryOptions("path", filePath), sessionId);

            File file = null;
            if (searchFile.getNumResults() != 0) {
                File existingFile = searchFile.getResult().get(0);
                switch (policy) {
                    case DELETE:
                        catalogManager.deleteFile(existingFile.getId(), sessionId);
                        break;
                    case REPLACE:
                        ignoreStatus = true;
                        overwrite = true;
                        file = existingFile;
                        break;
//                    case RENAME:
//                        throw new UnsupportedOperationException("Unimplemented policy 'rename'");
//                    case DO_ERROR:
//                        throw new UnsupportedOperationException("Unimplemented policy 'error'");
                }
            }

            if (file == null) {
                file = catalogManager.createFile(studyId, getFormat(uri), getBioformat(uri), filePath, "", true, -1, sessionId).first();
            }

            /** Moves the file to the read output **/
            catalogFileUtils.upload(uri, file, null, sessionId, ignoreStatus, overwrite, deleteSource, calculateChecksum);

            try {
                FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, null, sessionId, false);
            } catch (Exception e) {
                logger.error("Unable to read metadata information from file { id:" + file.getId() + ", name: \"" + file.getName() + "\" }", e);
            }

            files.add(catalogManager.getFile(file.getId(), sessionId).first());
        }
        return files;
    }

    private File.Bioformat getBioformat(URI uri) {
        return BioformatDetector.detect(uri);
    }

    private File.Format getFormat(URI uri) {
        return FormatDetector.detect(uri);
    }

}
