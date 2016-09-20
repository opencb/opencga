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

package org.opencb.opencga.catalog.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogFileUtils {

    private static Logger logger = LoggerFactory.getLogger(CatalogFileUtils.class);
    private final CatalogManager catalogManager;

    public CatalogFileUtils(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    /**
     * Upload file to a created entry file in Catalog.
     *
     * @param sourceUri         File URI to be moved into Catalog workspace
     * @param file              File from Catalog
     * @param sourceChecksum    Optional: Source checksum
     * @param sessionId         Valid sessionID to modify the File in Catalog
     * @param ignoreStatus      Ignore the status (uploading, uploaded, ready) from Catalog
     * @param overwrite         Overwrite if there is a file in the target
     * @param deleteSource      After moving, delete file. If false, force copy.
     * @param calculateChecksum Calculate checksum
     * @throws CatalogException CatalogException
     */
    public void upload(URI sourceUri, File file, String sourceChecksum, String sessionId,
                       boolean ignoreStatus, boolean overwrite, boolean deleteSource, boolean calculateChecksum)
            throws CatalogException {
        upload(sourceUri, file, sourceChecksum, sessionId, ignoreStatus, overwrite, deleteSource, calculateChecksum, Long.MAX_VALUE);
    }

    /**
     * Upload file to a created entry file in Catalog.
     *
     * @param sourceUri         File URI to be moved into Catalog workspace
     * @param file              File from Catalog
     * @param sourceChecksum    Optional: Source checksum
     * @param sessionId         Valid sessionID to modify the File in Catalog
     * @param ignoreStatus      Ignore the status (uploading, uploaded, ready) from Catalog
     * @param overwrite         Overwrite if there is a file in the target
     * @param deleteSource      After moving, delete file. If false, force copy.
     * @param calculateChecksum Calculate checksum
     * @param moveThreshold     File size threshold to move a file instead of copy.
     * @throws CatalogException CatalogException
     */
    public void upload(URI sourceUri, File file, String sourceChecksum, String sessionId,
                       boolean ignoreStatus, boolean overwrite, boolean deleteSource, boolean calculateChecksum, long moveThreshold)
            throws CatalogException {

        URI targetUri;
        CatalogIOManager sourceIOManager;
        CatalogIOManager targetIOManager;
        try {
            targetUri = catalogManager.getFileUri(file);
            sourceIOManager = catalogManager.getCatalogIOManagerFactory().get(sourceUri.getScheme());
            targetIOManager = catalogManager.getCatalogIOManagerFactory().get(targetUri.getScheme());
        } catch (CatalogDBException | CatalogIOException e) {
            throw new CatalogIOException("Can't upload file.", e);
        }

        // TODO: Check if can delete source
//        if (deleteSource) {
//        }

        //If the source is equals to the target, calculate checksum (if needed) and exit
        if (sourceUri.equals(targetUri)) {
            String targetChecksum;
            if (calculateChecksum) {
                logger.debug("SourceURI equals to TargetURI. Only calculate checksum and update file entry");
                targetChecksum = targetIOManager.calculateChecksum(targetUri);
            } else {
                logger.debug("SourceURI equals to TargetURI. Only update file entry");
                targetChecksum = sourceChecksum;
            }
            updateFileAttributes(file, targetChecksum, targetUri, new ObjectMap("status.name", File.FileStatus.READY), sessionId);
            return;
        }

        //Check status
        if (!ignoreStatus) {
            checkStatus(file);
        }

        //Check if there is any file in target
        checkTarget(file, targetUri, targetIOManager, overwrite);

        // Get file stats
        long size = sourceIOManager.getFileSize(sourceUri);

        //Calculate source checksum
        if (sourceChecksum == null || sourceChecksum.isEmpty()) {
            if (calculateChecksum) {
                sourceChecksum = sourceIOManager.calculateChecksum(sourceUri);
            } else {    //Do not calculate checksum.
                sourceChecksum = "null";
            }
        }

        //Move or copy file
        boolean fileMoved = false;
        String targetChecksum = "";
        if (size < moveThreshold && sourceIOManager == targetIOManager && deleteSource) {
            try {
                logger.info("Moving file {} -> {}", sourceUri, targetUri);
                sourceIOManager.moveFile(sourceUri, targetUri);
                targetChecksum = sourceChecksum;
                fileMoved = true;
            } catch (IOException | CatalogIOException e) {
                e.printStackTrace();
                logger.error("Error moving a file.", e);
            }
        }

        //If there was an error moving file or have to be copied
        if (!fileMoved) {
            try {
                copy(sourceIOManager, sourceUri, targetIOManager, targetUri);
            } catch (CatalogIOException | IOException e) {
                throw new CatalogIOException("Error while copying file. ", e);
            }

            try {
                if (calculateChecksum) {
                    targetChecksum = targetIOManager.calculateChecksum(targetUri);
                } else {
                    targetChecksum = sourceChecksum;
                }
            } catch (CatalogIOException catalogIOException) {
                try {
                    logger.info("Checksum fail. Delete target file.");
                    targetIOManager.deleteFile(targetUri);
                } catch (CatalogIOException e) {
                    e.printStackTrace();
                    //You fail at failing!
                    throw new CatalogIOException(
                            "Fail calculating target checksum : " + catalogIOException.getMessage() + ""
                                    + "Fail deleting target file : " + e.getMessage(), catalogIOException);
                }
                throw catalogIOException;
            }
        }

        //Check status
        if (!calculateChecksum || targetChecksum.equals(sourceChecksum)) {
            if (calculateChecksum) {
                logger.info("Checksum matches {}", sourceChecksum);
            } else {
                logger.info("Checksum not computed.");
            }

            updateFileAttributes(file, sourceChecksum, targetUri, new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(),
                    File.FileStatus.READY), sessionId);

            if (deleteSource && !fileMoved) {
                logger.info("Deleting source file {} after moving", sourceUri);
                try {
                    sourceIOManager.deleteFile(sourceUri);
                } catch (CatalogIOException e) {
                    throw e;
                }
            }
        } else {
            throw new CatalogIOException("Checksum mismatches at moving files. " + sourceChecksum + " =! " + targetChecksum);
        }

    }

    public void upload(InputStream inputStream, File file, String sessionId, boolean ignoreStatus,
                       boolean overwrite, boolean calculateChecksum) throws CatalogException {
        URI targetUri;
        CatalogIOManager targetIOManager;
        try {
            targetUri = catalogManager.getFileUri(file);
            targetIOManager = catalogManager.getCatalogIOManagerFactory().get(targetUri.getScheme());
        } catch (CatalogDBException | CatalogIOException e) {
            throw new CatalogIOException("Can't upload file.", e);
        }


        //Check status
        if (!ignoreStatus) {
            checkStatus(file);
        }

        //Check if there is any file in target
        checkTarget(file, targetUri, targetIOManager, overwrite);


        try {
            targetIOManager.createFile(targetUri, inputStream);
        } catch (CatalogIOException e) {
            e.printStackTrace();
        }


        String checksum = null;
        if (calculateChecksum) {
            try {
                checksum = targetIOManager.calculateChecksum(targetUri);
            } catch (CatalogIOException e) {
                e.printStackTrace();
            }
        }

        updateFileAttributes(file, checksum, targetUri, new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(),
                File.FileStatus.READY), sessionId);

    }


    @Deprecated
    /**
     * Link an external file into Catalog. Adds an URI to the field File::uri
     * A file with a uri is an "external" file. Files inside external folders are not external.
     *
     * @param file              Already existing file. Should be "STAGE" or "MISSING", or have relink == true
     * @param calculateChecksum Calculate checksum from the new linked file
     * @param externalUri       External file to link
     * @param createFoundFiles  Create a simple catalog file entry for found files. Used only for link folders
     * @param relink            Change the existing link of a file.
     * @param sessionId         User's sessionId
     * @return Modified file
     * @throws CatalogException CatalogException
     */
    public File link(File file, boolean calculateChecksum, URI externalUri, boolean createFoundFiles, boolean relink, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(file, "file");
        ParamUtils.checkObj(externalUri, "externalUri");
        ParamUtils.checkParameter(sessionId, "sessionId");

        checkCanLinkFile(file, relink);
        if (file.getType() == File.Type.DIRECTORY) {
            return linkFolder(file, calculateChecksum, externalUri, createFoundFiles, relink, sessionId);
        } else {
            return linkFile(file, calculateChecksum, externalUri, sessionId);
        }
    }

    @Deprecated
    /**
     * Creates and links an external folder.
     *
     * @param studyId           StudyId where to place the folder
     * @param filePath          Path to create the folder
     * @param parents           Make parent directories as needed
     * @param description       Folder description
     * @param calculateChecksum Calculate checksum from the new linked file
     * @param externalUri       External file to link
     * @param createFoundFiles  Create a simple catalog file entry for found files.
     * @param relink            Change the existing link of a file.
     * @param sessionId         User's sessionId
     * @return Created folder
     * @throws CatalogException CatalogException
     */
    public File linkFolder(long studyId, String filePath, boolean parents, String description, boolean calculateChecksum,
                           URI externalUri, boolean createFoundFiles, boolean relink, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(externalUri, "externalUri");
        ParamUtils.checkParameter(sessionId, "sessionId");

        File folder = catalogManager.createFolder(studyId, Paths.get(filePath), new File.FileStatus(File.FileStatus.STAGE), parents,
                description, null, sessionId).first();

        checkCanLinkFile(folder, relink);

        return linkFolder(folder, calculateChecksum, externalUri, createFoundFiles, relink, sessionId);
    }

    @Deprecated
    private void checkCanLinkFile(File file, boolean relink) throws CatalogException {
        String fileStatusEnum = file.getStatus().getName();
        if (relink) {
            if (!fileStatusEnum.equals(File.FileStatus.READY)) {
                throw new CatalogIOException("Unable to relink a file with status : " + fileStatusEnum);
            }
            if (!catalogManager.isExternal(file)) {
                throw new CatalogIOException("Unable to relink a non linked file");
            }
        } else {
            if (!fileStatusEnum.equals(File.FileStatus.STAGE) && !fileStatusEnum.equals(File.FileStatus.MISSING)) {
                throw new CatalogIOException("Unable to link a file with status : " + fileStatusEnum);
            }
        }
    }

    @Deprecated
    private File linkFile(File file, boolean calculateChecksum, URI externalUri, String sessionId) throws CatalogException {
        String checksum = null;
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(externalUri);

        if (ioManager.isDirectory(externalUri)) {
            throw new CatalogIOException("Can't link file '" + file.getPath() + "' with a folder uri " + externalUri);
        }

        if (calculateChecksum) {
            try {
                checksum = ioManager.calculateChecksum(externalUri);
            } catch (CatalogIOException e) {
                e.printStackTrace();
            }
        }

        ObjectMap objectMap = new ObjectMap("uri", externalUri);
        objectMap.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        updateFileAttributes(file, checksum, externalUri, objectMap, sessionId);
        return catalogManager.getFile(file.getId(), sessionId).first();
    }

    @Deprecated
    private File linkFolder(File folder, boolean calculateChecksum, URI externalUri, boolean createFoundFiles, boolean relink, String
            sessionId) throws CatalogException {
        logger.debug("Linking a folder");
        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(externalUri);

        if (!ioManager.isDirectory(externalUri)) {
            throw new CatalogIOException("Can't link folder '" + folder.getPath() + "' with a file uri " + externalUri);
        }
        if (folder.getUri() != null && !relink) {
            throw new CatalogIOException("Can't link folder '" + folder.getPath() + "'. Already linked to '" + folder.getUri() + "'");
        }


        //Only list files if request to create found files
        if (createFoundFiles) {
            //List files in folder
            long studyId = catalogManager.getStudyIdByFileId(folder.getId());
            Stream<URI> uris = ioManager.listFilesStream(externalUri);
            Path folderPath = Paths.get(folder.getPath());
            Map<URI, String> uriPathMap = uris
                    .filter(uri -> !ioManager.isDirectory(uri)) //Skip directories. Will be created automatically with "parents = true"
                    .collect(Collectors.toMap(Function.identity(), uri -> folderPath.resolve(externalUri.relativize(uri).getPath())
                            .toString()));

            //Search if there is any existing file in the folder with the path to use.
            Query pathsQuery = new Query(FileDBAdaptor.QueryParams.PATH.key(), new LinkedList<>(uriPathMap.values()));
            List<File> existingFiles = catalogManager.getAllFiles(studyId, pathsQuery, new QueryOptions(), sessionId).getResult();
            if (!relink) {
                if (existingFiles.size() != 0) {
                    for (File f : existingFiles) {
                        logger.warn("File already existing: { id:{}, path:\"{}\"}", f.getId(), f.getPath());
                    }
                    throw new CatalogException("Unable to link folder " + folder.getPath() + " to uri " + externalUri + ". Existing files"
                            + " on folder");
                }
            }
            Map<String, File> pathFileMap = existingFiles.stream().collect(Collectors.toMap(File::getPath, f -> f));

            //Set URI to folder. This will mark the directory as "external"
            catalogManager.modifyFile(folder.getId(), new ObjectMap("uri", externalUri), sessionId);

            //Create and link files.
            for (Map.Entry<URI, String> entry : uriPathMap.entrySet()) {
                String relativePath = entry.getValue();
                URI uri = entry.getKey();
                logger.debug("Adding file \"{}\"", relativePath);
                //Create new file. Do only create new files. Do not create already existing files.
                //Parents = true to create folders. Parameter "parents" should not be used here, it's
                //only related to the main folder creation.
                if (!pathFileMap.containsKey(entry.getValue())) {
                    File newFile = catalogManager.createFile(studyId, null, null, relativePath, "", true, -1, sessionId).first();
                    upload(uri, newFile, null, sessionId, false, false, false, calculateChecksum);
                }
            }
            for (File existingFile : existingFiles) {
                checkFile(existingFile, calculateChecksum, sessionId);
            }
        } else {
            //Set URI to folder. This will mark the directory as "external"
            catalogManager.modifyFile(folder.getId(), new ObjectMap("uri", externalUri), sessionId);
        }

        ObjectMap objectMap = new ObjectMap();
        objectMap.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        updateFileAttributes(folder, null, externalUri, objectMap, sessionId);
        return catalogManager.getFile(folder.getId(), sessionId).first();
    }

    @Deprecated
    public void delete(long fileId, String sessionId) throws CatalogException {
        delete(catalogManager.getFile(fileId, sessionId).first(), sessionId);
    }

    @Deprecated
    public void delete(File file, String sessionId) throws CatalogException {
        ParamUtils.checkObj(file, "file");

        if (!file.getStatus().getName().equals(File.FileStatus.TRASHED)) {
            throw new CatalogIOException("Only deleted files can be physically deleted");
        }
        long studyId = catalogManager.getStudyIdByFileId(file.getId());
        if (file.getType().equals(File.Type.DIRECTORY)) {
            Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~" + file.getPath() + "..*")
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.DELETED);
            List<File> files = catalogManager.getAllFiles(studyId, query, new QueryOptions(), sessionId).getResult();
            for (File f : files) {
                if (!f.getStatus().getName().equals(File.FileStatus.TRASHED)) {
                    throw new CatalogIOException("Only deleted files can be physically deleted");
                }
            }
            for (File f : files) {
                if (f.getType() == File.Type.FILE && !f.getStatus().getName().equals(File.FileStatus.TRASHED)) {
                    delete(f, sessionId);
                }
            }
            List<File> folders = files.stream()
                    .filter(f -> f.getType() == File.Type.DIRECTORY && !f.getStatus().getName().equals(File.FileStatus.TRASHED))
                    .sorted((f1, f2) -> f2.getPath().length() - f1.getPath().length())
                    .collect(Collectors.toList());
            for (File folder : folders) {
                delete(folder, sessionId);
            }
        }

        if (!catalogManager.isExternal(file)) { //Do not delete file if is external
            URI fileUri = catalogManager.getFileUri(file);
            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(fileUri);
            if (ioManager.isDirectory(fileUri)) {
                if (!ioManager.listFiles(fileUri).isEmpty()) {
                    throw new CatalogIOException("Unable to delete folder " + fileUri + ". Folder is not empty!");
                }
            }
            ioManager.deleteFile(fileUri);
        }
        catalogManager.modifyFile(file.getId(), new ObjectMap("status.name", File.FileStatus.TRASHED), sessionId);

    }

    /**
     * Check if the fileURI related to the provided file exists, and modifies the file status if necessary.
     * <p>
     * For READY files with a non existing file, set status to MISSING. "Lost file"
     * For MISSING files who recover the file, set status to READY. "Found file"
     * For TRASHED files with a non existing file, set status to DELETED.
     *
     * @param file              File to check
     * @param calculateChecksum Calculate checksum for "found files"
     * @param sessionId         User's sessionId
     * @return If there is any change, returns the modified file. Else, return the same file.
     * @throws CatalogException CatalogException
     */
    public File checkFile(File file, boolean calculateChecksum, String sessionId) throws CatalogException {
        File modifiedFile = file;
        switch (file.getStatus().getName()) {
            case File.FileStatus.READY:
            case File.FileStatus.MISSING: {
                URI fileUri = catalogManager.getFileUri(file);
                if (!catalogManager.getCatalogIOManagerFactory().get(fileUri).exists(fileUri)) {
                    logger.warn("File { id:" + file.getId() + ", path:\"" + file.getPath() + "\" } lost tracking from file " + fileUri);
                    if (!file.getStatus().getName().equals(File.FileStatus.MISSING)) {
                        logger.info("Set status to " + File.FileStatus.MISSING);
                        catalogManager.modifyFile(file.getId(), new ObjectMap("status.name", File.FileStatus.MISSING), sessionId);
                        modifiedFile = catalogManager.getFile(file.getId(), sessionId).first();
                    }
                } else if (file.getStatus().getName().equals(File.FileStatus.MISSING)) {
                    logger.info("File { id:" + file.getId() + ", path:\"" + file.getPath() + "\" } recover tracking from file " + fileUri);
                    logger.info("Set status to " + File.FileStatus.READY);
                    ObjectMap params = getModifiedFileAttributes(file, fileUri, calculateChecksum);
                    params.put("status.name", File.FileStatus.READY);
                    catalogManager.modifyFile(file.getId(), params, sessionId);
                    modifiedFile = catalogManager.getFile(file.getId(), sessionId).first();
                }
                break;
            }
            case File.FileStatus.TRASHED: {
                URI fileUri = catalogManager.getFileUri(file);
                if (!catalogManager.getCatalogIOManagerFactory().get(fileUri).exists(fileUri)) {
                    modifiedFile = file;
                    // TODO: Change status to remove.
//                    catalogManager.modifyFile(file.getId(), new ObjectMap("status.name", File.FileStatus.DELETED), sessionId);
//                    modifiedFile = catalogManager.getFile(file.getId(), sessionId).first();
                    break;
                }
            }
            default:
                break;
        }
        return modifiedFile;
    }


    /**
     * Update some file attributes.
     * diskUsage
     * modificationDate
     * attributes.checksum
     *
     * @param file              File to update
     * @param calculateChecksum Do calculate checksum
     * @param sessionId         users sessionId
     * @throws CatalogException CatalogException
     */
    public void updateFileAttributes(File file, boolean calculateChecksum, String sessionId) throws CatalogException {
        URI fileUri = catalogManager.getFileUri(file);
        String checksum = null;
        if (calculateChecksum) {
            checksum = catalogManager.getCatalogIOManagerFactory().get(fileUri).calculateChecksum(fileUri);
        }
        updateFileAttributes(file, checksum, null, null, sessionId);
    }

    /**
     * Get a ObjectMap with some fields if they have been modified.
     * diskUsage
     * modificationDate
     * attributes.checksum
     *
     * @param file              file
     * @param fileUri           If null, calls to getFileUri()
     *                          <p>
     *                          TODO: Lazy checksum: Only calculate checksum if the diskUsage has changed.
     * @param calculateChecksum Calculate checksum to check if have changed
     * @return ObjectMap ObjectMap
     * @throws CatalogException CatalogException
     */
    public ObjectMap getModifiedFileAttributes(File file, URI fileUri, boolean calculateChecksum) throws CatalogException {
        if (fileUri == null) {
            fileUri = catalogManager.getFileUri(file);
        }
        String checksum = null;
        if (calculateChecksum) {
            checksum = catalogManager.getCatalogIOManagerFactory().get(fileUri).calculateChecksum(fileUri);
        }
        return getModifiedFileAttributes(file, checksum, fileUri, null);
    }


    /**
     * Update some file attributes.
     * diskUsage
     * modificationDate
     * attributes.checksum
     *
     * @throws CatalogException CatalogException
     */
    private void updateFileAttributes(File file, String checksum, URI fileUri, ObjectMap parameters, String sessionId)
            throws CatalogException {
        parameters = getModifiedFileAttributes(file, checksum, fileUri, parameters);

        //Update file
        try {
            if (!parameters.isEmpty()) {    //If there is something to update
                catalogManager.modifyFile(file.getId(), parameters, sessionId);
            }
        } catch (CatalogException e) {
            throw new CatalogIOException("Can't update file properties in Catalog.", e);
        }
    }

    /**
     * Get a ObjectMap with some fields if they have been modified.
     * diskUsage
     * modificationDate
     * attributes.checksum
     *
     * @throws CatalogException CatalogException
     */
    private ObjectMap getModifiedFileAttributes(File file, String checksum, URI fileUri, ObjectMap parameters) throws CatalogException {
        parameters = ParamUtils.defaultObject(parameters, ObjectMap::new);
        if (fileUri == null) {
            fileUri = catalogManager.getFileUri(file);
        }
        CatalogIOManager catalogIOManager = catalogManager.getCatalogIOManagerFactory().get(fileUri);

        if (checksum != null && !checksum.isEmpty() && !checksum.equals("null")) {
            if (file.getAttributes() == null || !Objects.equals(file.getAttributes().get("checksum"), checksum)) {
                parameters.put("attributes", new ObjectMap("checksum", checksum));
            }
        }

        try {
            long size = catalogIOManager.getFileSize(fileUri);
            if (file.getDiskUsage() != size) {
                parameters.put("diskUsage", size);
            }
        } catch (CatalogIOException e) {
            e.printStackTrace();
            logger.error("Can't get fileSize", e);
        }

        String modificationDate = TimeUtils.getTime(catalogIOManager.getModificationDate(fileUri));
        if (!modificationDate.equals(file.getModificationDate())) {
            parameters.put("modificationDate", modificationDate);
        }

        return parameters;
    }

    /**
     * Check if there is any file in the URI target.
     *
     * @throws org.opencb.opencga.catalog.exceptions.CatalogIOException
     */
    private void checkTarget(File file, URI targetUri, CatalogIOManager targetIOManager, boolean overwrite) throws CatalogIOException {
        if (!overwrite && targetIOManager.exists(targetUri)) {
            throw new CatalogIOException("There is a file in the target!"
                    + "file:{id:" + file.getId() + ", targetUri: '" + targetUri + "' } "
                    + "Needs 'overwrite = true' for continue.");
        }
    }

    /**
     * Check status. To upload a fileObject, the catalog file entry must be in status "uploading".
     * Set "ignoreStatus = true" to ignore the actual file status and replace the content.
     *
     * @throws org.opencb.opencga.catalog.exceptions.CatalogIOException
     */
    private void checkStatus(File file) throws CatalogIOException {
        if (!file.getStatus().getName().equals(File.FileStatus.STAGE)) {
            throw new CatalogIOException("File status is already uploaded and ready! "
                    + "file:{ path: '" + file.getPath() + "', id:" + file.getId() + ", status: '" + file.getStatus().getName() + "' } "
                    + "Needs 'ignoreStatus = true' for continue.");
        }
    }

    /**
     * @param sourceIOManager Source IOManager
     * @param source          Source file
     * @param targetIOManager Target IOManager
     * @param target          Target file
     * @throws IOException
     * @throws org.opencb.opencga.catalog.exceptions.CatalogIOException
     */
    private void copy(CatalogIOManager sourceIOManager, URI source, CatalogIOManager targetIOManager, URI target)
            throws IOException, CatalogIOException {

        logger.info("Copying file from {} to {}", source, target);
        //If it's the same IOManager, copy intra FS
        if (sourceIOManager == targetIOManager) {
            sourceIOManager.copyFile(source, target);
            return;
        }

        //TODO: Implement copies inter FS
//        HDFS -> POSIX
//        POSIX -> HDFS

        logger.info("Unable to copy directly from {} to {} . Doing manual copy.", source.getScheme(), target.getScheme());
        DataInputStream fileObject = sourceIOManager.getFileObject(source, -1, -1);
        targetIOManager.createFile(target, fileObject);

        //throw new CatalogIOManagerException("Unable to copy from " + source.getScheme() + " to " + target.getScheme());
    }

}
