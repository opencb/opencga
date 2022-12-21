/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.utils;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileStatus;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.file.SmallFileInternal;
import org.opencb.opencga.core.models.study.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

    private FileUtils catalogFileUtils;

    public enum FileScannerPolicy {
        DELETE,     //Delete file and file entry. Then create a new one
        REPLACE,    //Delete the file, but not the file entry. Updates the file information.
//        DO_ERROR,
//        RENAME,
    }

    public FileScanner(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        catalogFileUtils = new FileUtils(catalogManager);
    }

    /**
     * Check tracking from all files from a study.
     * <p>
     * Set file status {@link FileStatus#MISSING} if the file (fileUri) is unreachable
     * Set file status to {@link FileStatus#READY} if was {@link FileStatus#MISSING} and file (fileUri) is reachable
     *
     * @param study             The study to check
     * @param sessionId         User sessionId
     * @param calculateChecksum Calculate checksum for "found files"
     * @return found and lost files
     * @throws CatalogException if a Catalog error occurs
     */
    public List<File> checkStudyFiles(Study study, boolean calculateChecksum, String sessionId) throws CatalogException {
        Query query = new Query();
        query.put(FileDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Arrays.asList(
                FileStatus.READY, FileStatus.MISSING, FileStatus.TRASHED));

        DBIterator<File> iterator = catalogManager.getFileManager().iterator(study.getFqn(), query, new QueryOptions(), sessionId);

        List<File> modifiedFiles = new LinkedList<>();
        while (iterator.hasNext()) {
            File file = iterator.next();
            File checkedFile = catalogFileUtils.checkFile(study.getFqn(), file, calculateChecksum, sessionId);
            if (checkedFile != file) {
                modifiedFiles.add(checkedFile);
            }
        }

        return modifiedFiles;
    }

    /**
     * Scan the study folder, add all untracked files and check tracking.
     *
     * @param study             Study to resync
     * @param calculateChecksum Calculates checksum of all the files in the directory to scan
     * @param sessionId         User sessionId
     * @return New, lost and found files
     * @throws CatalogException if a Catalog error occurs
     * @throws IOException      if an I/O error occurs
     */
    public List<File> reSync(Study study, boolean calculateChecksum, String sessionId) throws CatalogException, IOException {
        Query query = new Query(FileDBAdaptor.QueryParams.TYPE.key(), File.Type.DIRECTORY);
        DBIterator<File> iterator = catalogManager.getFileManager().iterator(study.getFqn(), query, null, sessionId);

        List<File> scan = new LinkedList<>();
        while (iterator.hasNext()) {
            File folder = iterator.next();
            scan.addAll(scan(folder, catalogManager.getFileManager().getUri(folder), FileScannerPolicy.REPLACE, calculateChecksum,
                    false, sessionId));
        }

        // TODO: Scan per file
        scan.addAll(checkStudyFiles(study, calculateChecksum, sessionId));

        return scan;
    }

    /**
     * Return all untracked files in a study folder.
     *
     * @param study     Study to scan
     * @param sessionId User sessionId
     * @return Untracked files
     * @throws CatalogException if a Catalog error occurs
     */
    public Map<String, URI> untrackedFiles(Study study, String sessionId) throws CatalogException {
        long studyId = study.getUid();
        URI studyUri = study.getUri();

        IOManager ioManager = null;
        try {
            ioManager = catalogManager.getIoManagerFactory().get(studyUri);
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(studyUri, e);
        }
        Map<String, URI> linkedFolders = new HashMap<>();
        linkedFolders.put("", studyUri);
        Query query = new Query(FileDBAdaptor.QueryParams.URI.key(), "~.*"); //Where URI exists)
        QueryOptions queryOptions = new QueryOptions("include", "projects.studies.files.path,projects.studies.files.uri");
        catalogManager.getFileManager().search(String.valueOf(studyId), query, queryOptions, sessionId).getResults()
                .forEach(f -> linkedFolders.put(f.getPath(), f.getUri()));

        Map<String, URI> untrackedFiles = new HashMap<>();
        for (Map.Entry<String, URI> entry : linkedFolders.entrySet()) {
            if (!ioManager.exists(entry.getValue())) {
                untrackedFiles.put(entry.getKey(), entry.getValue());
                continue;
            }

            try (Stream<URI> files = ioManager.listFilesStream(entry.getValue())) {
                Iterator<URI> iterator = files.iterator();

                while (iterator.hasNext()) {
                    URI uri = iterator.next();
                    String filePath = entry.getKey() + entry.getValue().relativize(uri).toString();

                    DataResult<File> searchFile = catalogManager.getFileManager().search(String.valueOf(studyId),
                            new Query("path", filePath), new QueryOptions("include", "projects.studies.files.id"), sessionId);
                    if (searchFile.getResults().isEmpty()) {
                        untrackedFiles.put(filePath, uri);
                    } /*else {
                    iterator.remove(); //Remove the ones that have an entry in Catalog
                }*/
                }
            }
        }
        return untrackedFiles;
    }


    /**
     * Scans the files inside the specified URI and adds to the provided directory.
     *
     * @param directory         Directory where add found files
     * @param directoryToScan   Directory to scan
     * @param policy            What to do when there is a file in the target path. See {@link FileScannerPolicy}
     * @param calculateChecksum Calculates checksum of all the files in the directory to scan
     * @param deleteSource      After moving, deletes the source file. If false, force copy.
     * @param sessionId         User sessionId
     * @return found and new files.
     * @throws IOException      if an I/O error occurs
     * @throws CatalogException if a Catalog error occurs
     */
    public List<File> scan(File directory, URI directoryToScan, FileScannerPolicy policy,
                           boolean calculateChecksum, boolean deleteSource, String sessionId)
            throws IOException, CatalogException {
        return scan(directory, directoryToScan, policy, calculateChecksum, deleteSource, uri -> true, sessionId);
    }

    /**
     * Scans the files inside the specified URI and adds to the provided directory.
     *
     * @param directory         Directory where add found files
     * @param directoryToScan   Directory to scan
     * @param policy            What to do when there is a file in the target path. See {@link FileScannerPolicy}
     * @param calculateChecksum Calculates checksum of all the files in the directory to scan
     * @param deleteSource      After moving, deletes the source file. If false, force copy.
     * @param filter            File filter. Excludes the file when this predicate returns false.
     * @param sessionId         User sessionId
     * @return found and new files.
     * @throws IOException      if an I/O error occurs
     * @throws CatalogException if a Catalog error occurs
     */
    public List<File> scan(File directory, URI directoryToScan, FileScannerPolicy policy, boolean calculateChecksum, boolean deleteSource,
                           Predicate<URI> filter, String sessionId) throws CatalogException, IOException {
        if (filter == null) {
            filter = uri -> true;
        }
        if (directoryToScan == null) {
            directoryToScan = catalogManager.getFileManager().getUri(directory);
        }
        if (!directoryToScan.getPath().endsWith("/")) {
            directoryToScan = URI.create(directoryToScan.toString() + "/");
        }
        if (!directory.getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogException("Provided folder " + directory.getPath() + " is actually a file.");
        }
        Study study = catalogManager.getFileManager().getStudy(directory, sessionId);

        long createFilesTime = 0, uploadFilesTime = 0, metadataReadTime = 0;
        IOManager ioManager = catalogManager.getIoManagerFactory().get(directoryToScan);
        Stream<URI> uris = ioManager.exists(directoryToScan)
                ? catalogManager.getIoManagerFactory().get(directoryToScan).listFilesStream(directoryToScan)
                : Stream.empty();
        List<File> files = new LinkedList<>();

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
            DataResult<File> searchFile = catalogManager.getFileManager().search(study.getFqn(), query, null, sessionId);
            File file = null;
            boolean overwrite = true;
            boolean returnFile = false;
            if (searchFile.getNumResults() != 0) {
                File existingFile = searchFile.first();
                logger.info("File already existing in target \"" + filePath + "\". FileScannerPolicy = " + policy);
                switch (policy) {
                    case DELETE:
                        logger.info("Deleting file { uid:" + existingFile.getUid() + ", path:\"" + existingFile.getPath() + "\" }");
                        Query tmpQuery = new Query(FileDBAdaptor.QueryParams.UID.key(), existingFile.getUid());

                        // Set the status of the file to PENDING DELETE
                        FileUpdateParams updateParams = new FileUpdateParams()
                                .setInternal(new SmallFileInternal(new FileStatus(FileStatus.PENDING_DELETE)));
                        catalogManager.getFileManager().update(study.getFqn(), tmpQuery, updateParams, QueryOptions.empty(), sessionId);

                        // Delete completely the file/folder !
                        catalogManager.getFileManager().delete(study.getFqn(), tmpQuery, new QueryOptions(Constants.SKIP_TRASH, true),
                                sessionId);
                        overwrite = false;
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
                    file = catalogManager.getFileManager().createFolder(study.getFqn(), Paths.get(filePath).toString(), true,
                            null, QueryOptions.empty(), sessionId).first();
                } else {
                    start = System.currentTimeMillis();

                    InputStream inputStream = new BufferedInputStream(new FileInputStream(new java.io.File(uri)));
                    file = catalogManager.getFileManager().upload(study.getFqn(), inputStream,
                            new File().setPath(filePath), overwrite, true, calculateChecksum, sessionId).first();
                    if (deleteSource) {
                        ioManager.deleteFile(uri);
                    }

                    end = System.currentTimeMillis();
                    uploadFileTime = end - start;
                    uploadFilesTime += uploadFileTime;
                    returnFile = true;      //Return file because is new
                }
                logger.debug("Created new file entry for " + uri + " { uid:" + file.getUid() + ", path:\"" + file.getPath() + "\" } ");
            } else {
                if (file.getType() == File.Type.FILE) {
                    if (file.getInternal().getStatus().getId().equals(FileStatus.MISSING)) {
                        logger.info("File { uid:" + file.getUid() + ", path:'" + file.getPath() + "' } recover tracking from file " + uri);
                        logger.debug("Set status to " + FileStatus.READY);
                        returnFile = true;      //Return file because was missing
                    }
                    long start = System.currentTimeMillis();

                    InputStream inputStream = new FileInputStream(new java.io.File(uri));
                    file = catalogManager.getFileManager().upload(study.getFqn(), inputStream, file, overwrite, true, calculateChecksum,
                            sessionId).first();

                    long end = System.currentTimeMillis();
                    uploadFilesTime += end - start;
                }
            }

            if (returnFile) { //Return only new and found files.
                files.add(file);
            }
            logger.info("Added file {}", filePath);
            logger.debug("{}s (create {}s, upload {}s, metadata {}s)", (System.currentTimeMillis() - fileScanStart) / 1000.0,
                    createFileTime / 1000.0, uploadFileTime / 1000.0, metadataFileTime / 1000.0);
        }
        // Close stream
        uris.close();

        logger.debug("Create catalog file entries: " + createFilesTime / 1000.0 + "s");
        logger.debug("Upload files: " + uploadFilesTime / 1000.0 + "s");
        logger.debug("Read metadata information: " + metadataReadTime / 1000.0 + "s");
        return files;
    }

}
