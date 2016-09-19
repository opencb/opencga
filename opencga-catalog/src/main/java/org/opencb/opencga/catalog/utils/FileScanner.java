package org.opencb.opencga.catalog.utils;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileScanner {

    private static Logger logger = LoggerFactory.getLogger(FileScanner.class);

    protected final CatalogManager catalogManager;

    private CatalogFileUtils catalogFileUtils;

    public enum FileScannerPolicy {
        DELETE,     //Delete file and file entry. Then create a new one
        REPLACE,    //Delete the file, but not the file entry. Updates the file information.
//        DO_ERROR,
//        RENAME,
    }

    public FileScanner(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        catalogFileUtils = new CatalogFileUtils(catalogManager);
    }

    /**
     * Check tracking from all files from a study.
     *
     * Set file status {@link File.FileStatus#MISSING} if the file (fileUri) is unreachable
     * Set file status to {@link File.FileStatus#READY} if was {@link File.FileStatus#MISSING} and file (fileUri) is reachable
     *
     * @param study             The study to check
     * @param sessionId         User sessionId
     * @param calculateChecksum Calculate checksum for "found files"
     * @throws CatalogException if a Catalog error occurs
     * @return found and lost files
     */
    public List<File> checkStudyFiles(Study study, boolean calculateChecksum, String sessionId) throws CatalogException {
        Query query = new Query();
        query.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), Arrays.asList(
                File.FileStatus.READY, File.FileStatus.MISSING, File.FileStatus.TRASHED));
        QueryResult<File> files = catalogManager.getAllFiles(study.getId(), query, new QueryOptions(), sessionId);

        List<File> modifiedFiles = new LinkedList<>();
        for (File file : files.getResult()) {
            File checkedFile = catalogFileUtils.checkFile(file, calculateChecksum, sessionId);
            if (checkedFile != file) {
                modifiedFiles.add(checkedFile);
            }
        }
        return modifiedFiles;
    }

    /**
     * Scan the study folder, add all untracked files and check tracking.
     *
     * @param study                 Study to resync
     * @param calculateChecksum     Calculates checksum of all the files in the directory to scan
     * @param sessionId             User sessionId
     * @return                      New, lost and found files
     * @throws CatalogException     if a Catalog error occurs
     * @throws IOException          if an I/O error occurs
     */
    public List<File> reSync(Study study, boolean calculateChecksum, String sessionId)
            throws CatalogException, IOException {
        long studyId = study.getId();
//        File root = catalogManager.getAllFiles(studyId, new QueryOptions("path", ""), sessionId).first();
        Query query = new Query();
        query.put(FileDBAdaptor.FileFilterOption.uri.toString(), "~.*"); //Where URI exists
        query.put(FileDBAdaptor.FileFilterOption.type.toString(), File.Type.DIRECTORY);
        List<File> files = catalogManager.searchFile(studyId, query, sessionId).getResult();

        List<File> scan = new LinkedList<>();
        for (File file : files) {
            scan.addAll(scan(file, catalogManager.getFileUri(file), FileScannerPolicy.REPLACE, calculateChecksum,
                    false, sessionId));
        }
        // TODO: Scan per file
        scan.addAll(checkStudyFiles(study, calculateChecksum, sessionId));

        return scan;
    }

    /**
     * Return all untracked files in a study folder.
     *
     * @param study         Study to scan
     * @param sessionId     User sessionId
     * @return              Untracked files
     * @throws CatalogException     if a Catalog error occurs
     */
    public Map<String, URI> untrackedFiles(Study study, String sessionId)
            throws CatalogException {
        long studyId = study.getId();
        URI studyUri = catalogManager.getStudyUri(studyId);

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(studyUri);
        Map<String, URI> linkedFolders = new HashMap<>();
        linkedFolders.put("", studyUri);
        Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), "~.*"); //Where URI exists)
        QueryOptions queryOptions = new QueryOptions("include", "projects.studies.files.path,projects.studies.files.uri");
        catalogManager.getAllFiles(studyId, query, queryOptions, sessionId).getResult()
                .forEach(f -> linkedFolders.put(f.getPath(), f.getUri()));

        Map<String, URI> untrackedFiles = new HashMap<>();
        for (Map.Entry<String, URI> entry : linkedFolders.entrySet()) {
            if (!ioManager.exists(entry.getValue())) {
                untrackedFiles.put(entry.getKey(), entry.getValue());
                continue;
            }
            Stream<URI> files = ioManager.listFilesStream(entry.getValue());

            Iterator<URI> iterator = files.iterator();
            while (iterator.hasNext()) {
                URI uri = iterator.next();
                String filePath = entry.getKey() + entry.getValue().relativize(uri).toString();

                QueryResult<File> searchFile = catalogManager.searchFile(studyId,
                        new Query("path", filePath),
                        new QueryOptions("include", "projects.studies.files.id"), sessionId);
                if (searchFile.getResult().isEmpty()) {
                    untrackedFiles.put(filePath, uri);
                } /*else {
                    iterator.remove(); //Remove the ones that have an entry in Catalog
                }*/
            }
        }
        return untrackedFiles;
    }


    /**
     * Scans the files inside the specified URI and adds to the provided directory.
     *
     * @param directory             Directory where add found files
     * @param directoryToScan       Directory to scan
     * @param policy                What to do when there is a file in the target path. See {@link FileScannerPolicy}
     * @param calculateChecksum     Calculates checksum of all the files in the directory to scan
     * @param deleteSource          After moving, deletes the source file. If false, force copy.
     * @param sessionId             User sessionId
     * @return found and new files.
     * @throws IOException          if an I/O error occurs
     * @throws CatalogException     if a Catalog error occurs
     */
    public List<File> scan(File directory, URI directoryToScan, FileScannerPolicy policy,
                           boolean calculateChecksum, boolean deleteSource, String sessionId)
            throws IOException, CatalogException {
        return scan(directory, directoryToScan, policy, calculateChecksum, deleteSource, uri -> true, -1, sessionId);
    }

    /**
     * Scans the files inside the specified URI and adds to the provided directory.
     *
     * @param directory             Directory where add found files
     * @param directoryToScan       Directory to scan
     * @param policy                What to do when there is a file in the target path. See {@link FileScannerPolicy}
     * @param calculateChecksum     Calculates checksum of all the files in the directory to scan
     * @param deleteSource          After moving, deletes the source file. If false, force copy.
     * @param filter                File filter. Excludes the file when this predicate returns false.
     * @param jobId                 If any, the job that has generated this files
     * @param sessionId             User sessionId
     * @return found and new files.
     * @throws IOException          if an I/O error occurs
     * @throws CatalogException     if a Catalog error occurs
     */
    public List<File> scan(File directory, URI directoryToScan, FileScannerPolicy policy,
                           boolean calculateChecksum, boolean deleteSource, Predicate<URI> filter, long jobId, String sessionId)
            throws IOException, CatalogException {
        if (filter == null) {
            filter = uri -> true;
        }
        if (directoryToScan == null) {
            directoryToScan = catalogManager.getFileUri(directory);
        }
        if (!directoryToScan.getPath().endsWith("/")) {
            directoryToScan = URI.create(directoryToScan.toString() + "/");
        }
        if (!directory.getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogException("Expected folder where place the found files.");
        }
        long studyId = catalogManager.getStudyIdByFileId(directory.getId());

        long createFilesTime = 0, uploadFilesTime = 0, metadataReadTime = 0;
        Stream<URI> uris = catalogManager.getCatalogIOManagerFactory().get(directoryToScan).listFilesStream(directoryToScan);
        List<File> files = new LinkedList<>();
        FileMetadataReader fileMetadataReader = FileMetadataReader.get(catalogManager);
        Iterator<URI> iterator = uris.iterator();
        while (iterator.hasNext()) {
            long fileScanStart = System.currentTimeMillis();
            URI uri = iterator.next();
            if (!filter.test(uri)) {
                continue;
            }
            URI generatedFile = directoryToScan.relativize(uri);
            String filePath = URI.create(directory.getPath()).resolve(generatedFile).toString();
//            String filePath = Paths.get(directory.getPath(), generatedFile.toString()).toString();
            if (generatedFile.getPath().endsWith("/") && !filePath.endsWith("/")) {
                filePath += "/";
            }

            Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), filePath);
            QueryResult<File> searchFile = catalogManager.searchFile(studyId, query, sessionId);
            File file = null;
            boolean returnFile = false;
            if (searchFile.getNumResults() != 0) {
                File existingFile = searchFile.first();
                logger.info("File already existing in target \"" + filePath + "\". FileScannerPolicy = " + policy);
                switch (policy) {
                    case DELETE:
                        logger.info("Deleting file { id:" + existingFile.getId() + ", path:\"" + existingFile.getPath() + "\" }");
                        // Delete completely the file/folder !
                        catalogManager.getFileManager().delete(Long.toString(existingFile.getId()),
                                new QueryOptions(FileManager.SKIP_TRASH, true), sessionId);
                        break;
                    case REPLACE:
                        file = existingFile;
                        break;
//                    case RENAME:
//                        throw new UnsupportedOperationException("Unimplemented policy 'rename'");
//                    case DO_ERROR:
//                        throw new UnsupportedOperationException("Unimplemented policy 'error'");
                    default:
                        throw new UnsupportedOperationException("Unimplemented policy '" + policy + "'");
                }
            }

            long createFileTime = 0, uploadFileTime = 0, metadataFileTime = 0;
            if (file == null) {
                long start, end;
                if (uri.getPath().endsWith("/")) {
                    file = catalogManager.createFolder(studyId, Paths.get(filePath), true, null, sessionId).first();
                } else {
                    start = System.currentTimeMillis();
                    File.Format format = FormatDetector.detect(uri);
                    File.Bioformat bioformat = BioformatDetector.detect(uri);
                    file = catalogManager.createFile(studyId, format, bioformat, filePath, "", true, jobId, sessionId).first();
                    end = System.currentTimeMillis();
                    createFileTime = end - start;
                    createFilesTime += createFileTime;

                    /** Moves the file to the read output **/
                    start = System.currentTimeMillis();
                    catalogFileUtils.upload(uri, file, null, sessionId, false, false, deleteSource, calculateChecksum);
                    end = System.currentTimeMillis();
                    uploadFileTime = end - start;
                    uploadFilesTime += uploadFileTime;
                    returnFile = true;      //Return file because is new
                }
                logger.debug("Created new file entry for " + uri + " { id:" + file.getId() + ", path:\"" + file.getPath() + "\" } ");
            } else {
                if (file.getType() == File.Type.FILE) {
                    if (file.getStatus().getName().equals(File.FileStatus.MISSING)) {
                        logger.info("File { id:" + file.getId() + ", path:\"" + file.getPath() + "\" } recover tracking from file " + uri);
                        logger.debug("Set status to " + File.FileStatus.READY);
                        returnFile = true;      //Return file because was missing
                    }
                    long start = System.currentTimeMillis();
                    catalogFileUtils.upload(uri, file, null, sessionId, true, true, deleteSource, calculateChecksum);
                    long end = System.currentTimeMillis();
                    uploadFilesTime += end - start;
                }
            }

            try {
                long start = System.currentTimeMillis();
                fileMetadataReader.setMetadataInformation(file, null, null, sessionId, false);
                long end = System.currentTimeMillis();
                metadataFileTime = end - start;
                metadataReadTime += metadataFileTime;
            } catch (Exception e) {
                logger.error("Unable to read metadata information from file "
                        + "{ id:" + file.getId() + ", name: \"" + file.getName() + "\" }", e);
            }

            if (returnFile) { //Return only new and found files.
                files.add(catalogManager.getFile(file.getId(), sessionId).first());
            }
            logger.info("Added file {}", filePath);
            logger.debug("{}s (create {}s, upload {}s, metadata {}s)", (System.currentTimeMillis() - fileScanStart) / 1000.0,
                    createFileTime / 1000.0, uploadFileTime / 1000.0, metadataFileTime / 1000.0);
        }
        logger.debug("Create catalog file entries: " + createFilesTime / 1000.0 + "s");
        logger.debug("Upload files: " + uploadFilesTime / 1000.0 + "s");
        logger.debug("Read metadata information: " + metadataReadTime / 1000.0 + "s");
        return files;
    }

}
