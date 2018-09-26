/*
 * Copyright 2015-2017 OpenCB
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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileUtils {

    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private final CatalogManager catalogManager;

    public static final Map<File.Format, Pattern> FORMAT_MAP = new HashMap<>();

    static {
        FORMAT_MAP.put(File.Format.IMAGE, Pattern.compile(".*\\.(png|jpg|bmp|svg|gif|jpeg|tfg)(\\.[\\w]+)*", Pattern.CASE_INSENSITIVE));
    }

    public FileUtils(CatalogManager catalogManager) {
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
            targetUri = catalogManager.getFileManager().getUri(file);
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
            targetUri = catalogManager.getFileManager().getUri(file);
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

        File folder = catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get(filePath).toString(),
                new File.FileStatus(File.FileStatus.STAGE), parents, description, null, sessionId).first();

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
            if (!file.isExternal()) {
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
        return catalogManager.getFileManager().get(file.getUid(), null, sessionId).first();
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

        Study study = catalogManager.getFileManager().getStudy(folder, sessionId);

        //Only list files if request to create found files
        if (createFoundFiles) {
            //List files in folder
            Stream<URI> uris = ioManager.listFilesStream(externalUri);
            Path folderPath = Paths.get(folder.getPath());
            Map<URI, String> uriPathMap = uris
                    .filter(uri -> !ioManager.isDirectory(uri)) //Skip directories. Will be created automatically with "parents = true"
                    .collect(Collectors.toMap(Function.identity(), uri -> folderPath.resolve(externalUri.relativize(uri).getPath())
                            .toString()));

            //Search if there is any existing file in the folder with the path to use.
            Query pathsQuery = new Query(FileDBAdaptor.QueryParams.PATH.key(), new LinkedList<>(uriPathMap.values()));
            List<File> existingFiles = catalogManager.getFileManager().get(study.getFqn(), pathsQuery, new QueryOptions(),
                    sessionId).getResult();
            if (!relink) {
                if (existingFiles.size() != 0) {
                    for (File f : existingFiles) {
                        logger.warn("File already existing: { id:{}, path:\"{}\"}", f.getUid(), f.getPath());
                    }
                    throw new CatalogException("Unable to link folder " + folder.getPath() + " to uri " + externalUri + ". Existing files"
                            + " on folder");
                }
            }
            Map<String, File> pathFileMap = existingFiles.stream().collect(Collectors.toMap(File::getPath, f -> f));

            //Set URI to folder. This will mark the directory as "external"
            catalogManager.getFileManager().update(study.getFqn(), folder.getPath(), new ObjectMap("uri", externalUri), new QueryOptions(),
                    sessionId);

            //Create and link files.
            for (Map.Entry<URI, String> entry : uriPathMap.entrySet()) {
                String relativePath = entry.getValue();
                URI uri = entry.getKey();
                logger.debug("Adding file \"{}\"", relativePath);
                //Create new file. Do only create new files. Do not create already existing files.
                //Parents = true to create folders. Parameter "parents" should not be used here, it's
                //only related to the main folder creation.
                if (!pathFileMap.containsKey(entry.getValue())) {
                    File newFile = catalogManager.getFileManager().create(study.getFqn(), File.Type.FILE, null, null,
                            relativePath, null, "", null, 0, -1, null, (long) -1, null, null, true, null, null, sessionId).first();
                    upload(uri, newFile, null, sessionId, false, false, false, calculateChecksum);
                }
            }
            for (File existingFile : existingFiles) {
                checkFile(study.getFqn(), existingFile, calculateChecksum, sessionId);
            }
        } else {
            //Set URI to folder. This will mark the directory as "external"
            catalogManager.getFileManager().update(study.getFqn(), folder.getPath(), new ObjectMap("uri", externalUri), new QueryOptions(),
                    sessionId);
        }

        ObjectMap objectMap = new ObjectMap();
        objectMap.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
        updateFileAttributes(folder, null, externalUri, objectMap, sessionId);
        return catalogManager.getFileManager().get(folder.getUid(), null, sessionId).first();
    }

    @Deprecated
    public void delete(long fileId, String sessionId) throws CatalogException {
        delete(catalogManager.getFileManager().get(fileId, null, sessionId).first(), sessionId);
    }

    @Deprecated
    public void delete(File file, String sessionId) throws CatalogException {
        ParamUtils.checkObj(file, "file");

        if (!file.getStatus().getName().equals(File.FileStatus.TRASHED)) {
            throw new CatalogIOException("Only deleted files can be physically deleted");
        }
        long studyId = file.getStudyUid();
        if (file.getType().equals(File.Type.DIRECTORY)) {
            Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~" + file.getPath() + "..*")
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.DELETED);
            List<File> files = catalogManager.getFileManager().get(String.valueOf(studyId), query, new QueryOptions(), sessionId)
                    .getResult();
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

        if (!file.isExternal()) { //Do not delete file if is external
            URI fileUri = catalogManager.getFileManager().getUri(file);
            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(fileUri);
            if (ioManager.isDirectory(fileUri)) {
                if (!ioManager.listFiles(fileUri).isEmpty()) {
                    throw new CatalogIOException("Unable to delete folder " + fileUri + ". Folder is not empty!");
                }
            }
            ioManager.deleteFile(fileUri);
        }

        Study study = catalogManager.getFileManager().getStudy(file, sessionId);
        catalogManager.getFileManager().setStatus(study.getFqn(), file.getPath(), File.FileStatus.TRASHED, null, sessionId);
    }

    /**
     * Check if the fileURI related to the provided file exists, and modifies the file status if necessary.
     * <p>
     * For READY files with a non existing file, set status to MISSING. "Lost file"
     * For MISSING files who recover the file, set status to READY. "Found file"
     * For TRASHED files with a non existing file, set status to DELETED.
     *
     * @param studyStr          Study corresponding to the file to be checked.
     * @param file              File to check
     * @param calculateChecksum Calculate checksum for "found files"
     * @param sessionId         User's sessionId
     * @return If there is any change, returns the modified file. Else, return the same file.
     * @throws CatalogException CatalogException
     */
    public File checkFile(String studyStr, File file, boolean calculateChecksum, String sessionId) throws CatalogException {
        if (!file.getType().equals(File.Type.FILE)) {
            return file;
        }

        File modifiedFile = file;
        switch (file.getStatus().getName()) {
            case File.FileStatus.READY:
            case File.FileStatus.MISSING: {
                URI fileUri = catalogManager.getFileManager().getUri(file);
                if (!catalogManager.getCatalogIOManagerFactory().get(fileUri).exists(fileUri)) {
                    logger.warn("File { id:" + file.getPath() + ", path:\"" + file.getPath() + "\" } lost tracking from file " + fileUri);
                    if (!file.getStatus().getName().equals(File.FileStatus.MISSING)) {
                        logger.info("Set status to " + File.FileStatus.MISSING);
                        catalogManager.getFileManager().setStatus(studyStr, file.getPath(), File.FileStatus.MISSING, null, sessionId);
                        modifiedFile = catalogManager.getFileManager().get(studyStr, file.getPath(), null, sessionId).first();
                    }
                } else if (file.getStatus().getName().equals(File.FileStatus.MISSING)) {
                    logger.info("File { path:\"" + file.getPath() + "\" } recover tracking from file " + fileUri);
                    logger.info("Set status to " + File.FileStatus.READY);
                    ObjectMap params = getModifiedFileAttributes(file, fileUri, calculateChecksum);
                    if (params.get(FileDBAdaptor.QueryParams.SIZE.key()) != null) {
                        catalogManager.getFileManager()
                                .setDiskUsage(studyStr, file.getPath(), params.getLong(FileDBAdaptor.QueryParams.SIZE.key()), sessionId);
                        params.remove(FileDBAdaptor.QueryParams.SIZE.key());
                    }
                    if (!params.isEmpty()) {
                        catalogManager.getFileManager().update(studyStr, file.getPath(), params, QueryOptions.empty(), sessionId);
                    }
                    // Update status
                    catalogManager.getFileManager().setStatus(studyStr, file.getPath(), File.FileStatus.READY, null, sessionId);
                    modifiedFile = catalogManager.getFileManager().get(studyStr, file.getPath(), null, sessionId).first();
                }
                break;
            }
            case File.FileStatus.TRASHED: {
                URI fileUri = catalogManager.getFileManager().getUri(file);
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
     * Get a ObjectMap with some fields if they have been modified.
     * size
     * modificationDate
     * checksum
     * uri
     *
     * @param file              file
     * @param fileUri           If null, calls to getFileUri()
     *                          <p>
     *                          TODO: Lazy checksum: Only calculate checksum if the size has changed.
     * @param calculateChecksum Calculate checksum to check if have changed
     * @return ObjectMap ObjectMap
     * @throws CatalogException CatalogException
     */
    public ObjectMap getModifiedFileAttributes(File file, URI fileUri, boolean calculateChecksum) throws CatalogException {
        if (fileUri == null) {
            fileUri = catalogManager.getFileManager().getUri(file);
        }
        String checksum = null;
        if (calculateChecksum) {
            checksum = catalogManager.getCatalogIOManagerFactory().get(fileUri).calculateChecksum(fileUri);
        }
        return getModifiedFileAttributes(file, checksum, fileUri, null);
    }

    /**
     * Update some file attributes.
     * size
     * modificationDate
     * attributes.checksum
     * uri
     *
     * @throws CatalogException CatalogException
     */
    private void updateFileAttributes(File file, String checksum, URI fileUri, ObjectMap parameters, String sessionId)
            throws CatalogException {
        parameters = getModifiedFileAttributes(file, checksum, fileUri, parameters);
        Study study = catalogManager.getFileManager().getStudy(file, sessionId);

        //Update file
        if (!parameters.isEmpty()) {    //If there is something to update
//            if (parameters.get(FileDBAdaptor.QueryParams.ATTRIBUTES.key()) != null) {
//                ObjectMap attributes = new ObjectMap(FileDBAdaptor.QueryParams.ATTRIBUTES.key(),
//                        parameters.get(FileDBAdaptor.QueryParams.ATTRIBUTES.key()));
//                catalogManager.getFileManager().update(study.getFqn(), file.getPath(), attributes, new QueryOptions(), sessionId);
//            }
            if (parameters.get(FileDBAdaptor.QueryParams.STATUS_NAME.key()) != null) {
                catalogManager.getFileManager()
                        .setStatus(study.getFqn(), file.getPath(), parameters.getString(FileDBAdaptor.QueryParams.STATUS_NAME.key()),
                                null, sessionId);
                parameters.remove(FileDBAdaptor.QueryParams.STATUS_NAME.key());
            }
            if (parameters.get(FileDBAdaptor.QueryParams.SIZE.key()) != null) {
                catalogManager.getFileManager()
                        .setDiskUsage(study.getFqn(), file.getPath(), parameters.getLong(FileDBAdaptor.QueryParams.SIZE.key()), sessionId);
                parameters.remove(FileDBAdaptor.QueryParams.SIZE.key());
            }
            if (!parameters.isEmpty()) {
                catalogManager.getFileManager().update(study.getFqn(), file.getPath(), parameters, QueryOptions.empty(), sessionId);
            }
//            if (parameters.get(FileDBAdaptor.QueryParams.URI.key()) != null) {
//                catalogManager.getFileManager()
//                        .setUri(study.getFqn(), file.getPath(), parameters.getString(FileDBAdaptor.QueryParams.URI.key()), sessionId);
//            }
        }
    }

    /**
     * Get a ObjectMap with some fields if they have been modified.
     * size
     * checksum
     * uri
     *
     * @throws CatalogException CatalogException
     */
    private ObjectMap getModifiedFileAttributes(File file, String checksum, URI fileUri, ObjectMap parameters) throws CatalogException {
        parameters = ParamUtils.defaultObject(parameters, ObjectMap::new);
        if (fileUri == null) {
            fileUri = catalogManager.getFileManager().getUri(file);
        }
        CatalogIOManager catalogIOManager = catalogManager.getCatalogIOManagerFactory().get(fileUri);

        if (StringUtils.isNotEmpty(checksum)) {
            if (file.getChecksum() == null || !checksum.equals(file.getChecksum())) {
                parameters.put(FileDBAdaptor.QueryParams.CHECKSUM.key(), checksum);
            }
        }

        if (file.getUri() == null || !file.getUri().toString().equals(fileUri.toString())) {
            parameters.put(FileDBAdaptor.QueryParams.URI.key(), fileUri.toString());
        }

        try {
            long size = catalogIOManager.getFileSize(fileUri);
            if (file.getSize() != size) {
                parameters.put(FileDBAdaptor.QueryParams.SIZE.key(), size);
            }
        } catch (CatalogIOException e) {
            e.printStackTrace();
            logger.error("Can't get fileSize", e);
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
                    + "file:{id:" + file.getPath() + ", targetUri: '" + targetUri + "' } "
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
                    + "file:{ path: '" + file.getPath() + "', id:" + file.getPath() + ", status: '" + file.getStatus().getName() + "' } "
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

    public static File.Bioformat detectBioformat(URI uri) {
        return detectBioformat(uri, detectFormat(uri), detectCompression(uri));
    }

    public static File.Bioformat detectBioformat(URI uri, File.Format format, File.Compression compression) {
        String path = uri.getPath();
        Path source = Paths.get(uri);
        String mimeType;

        try {
            switch (format) {
                case VCF:
                case GVCF:
                case BCF:
                    return File.Bioformat.VARIANT;
                case TBI:
                    break;
                case SAM:
                case BAM:
                case CRAM:
                case BAI:
                    return File.Bioformat.ALIGNMENT;
                case BIGWIG:
                    return File.Bioformat.COVERAGE;
                case FASTQ:
                    return File.Bioformat.SEQUENCE;
                case PED:
                    return File.Bioformat.PEDIGREE;
                case TAB_SEPARATED_VALUES:
                    break;
                case COMMA_SEPARATED_VALUES:
                    break;
                case PROTOCOL_BUFFER:
                    break;
                case PLAIN:
                    break;
                case JSON:
                case AVRO:
                    String file;
                    if (compression != File.Compression.NONE) {
                        file = com.google.common.io.Files.getNameWithoutExtension(uri.getPath()); //Remove compression extension
                        file = com.google.common.io.Files.getNameWithoutExtension(file);  //Remove format extension
                    } else {
                        file = com.google.common.io.Files.getNameWithoutExtension(uri.getPath()); //Remove format extension
                    }

                    if (file.endsWith("variants")) {
                        return File.Bioformat.VARIANT;
                    } else if (file.endsWith("alignments")) {
                        return File.Bioformat.ALIGNMENT;
                    }
                    break;
                case PARQUET:
                    break;
                case IMAGE:
                case BINARY:
                case EXECUTABLE:
                case UNKNOWN:
                case XML:
                    return File.Bioformat.NONE;
                default:
                    break;
            }

            mimeType = Files.probeContentType(source);

            if (path.endsWith(".nw")) {
                return File.Bioformat.OTHER_NEWICK;
            }

            if (mimeType == null
                    || !mimeType.equalsIgnoreCase("text/plain")
                    || path.endsWith(".redirection")
                    || path.endsWith(".Rout")
                    || path.endsWith("cel_files.txt")
                    || !path.endsWith(".txt")) {
                return File.Bioformat.NONE;
            }

            try (FileInputStream fstream = new FileInputStream(path);
                 BufferedReader br = new BufferedReader(new InputStreamReader(fstream))) {

                String strLine;
                int numberOfLines = 20;
                int i = 0;
                boolean names = false;
                while ((strLine = br.readLine()) != null) {
                    if (strLine.equalsIgnoreCase("")) {
                        continue;
                    }
                    if (i == numberOfLines) {
                        break;
                    }
                    if (strLine.startsWith("#")) {
                        if (strLine.startsWith("#NAMES")) {
                            names = true;
                        } else {
                            continue;
                        }
                    } else {
                        String[] fields = strLine.split("\t");
                        if (fields.length > 2) {
                            if (names && NumberUtils.isNumber(fields[1])) {
                                return File.Bioformat.DATAMATRIX_EXPRESSION;
                            }
                        } else if (fields.length == 1) {
                            if (fields[0].split(" ").length == 1 && !NumberUtils.isNumber(fields[0])) {
                                return File.Bioformat.IDLIST;
                            }
                        } else if (fields.length == 2) {
                            if (!fields[0].contains(" ") && NumberUtils.isNumber(fields[1])) {
                                return File.Bioformat.IDLIST_RANKED;
                            }
                        }
                    }
                    i++;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return File.Bioformat.NONE;
    }

    /**
     * @param uri Existing file uri to the file
     * @return File.Format. UNKNOWN if can't detect any format.
     */
    public static File.Format detectFormat(URI uri) {
        for (Map.Entry<File.Format, Pattern> entry : FORMAT_MAP.entrySet()) {
            if (entry.getValue().matcher(uri.getPath()).matches()) {
                return entry.getKey();
            }
        }

        String path = uri.getPath();
        String extension = com.google.common.io.Files.getFileExtension(path);
        if (getCompression(extension) != File.Compression.NONE) {
            path = com.google.common.io.Files.getNameWithoutExtension(path);
            extension = com.google.common.io.Files.getFileExtension(path);
        }


        switch (extension.toLowerCase()) {
            case "vcf":
                return File.Format.VCF;
            case "gvcf":
                return File.Format.GVCF;
            case "bcf":
                return File.Format.BCF;
            case "bam":
                return File.Format.BAM;
            case "bw":
                return File.Format.BIGWIG;
            case "bai":
                return File.Format.BAI;
            case "sam":
                return File.Format.SAM;
            case "cram":
                return File.Format.CRAM;
            case "ped":
                return File.Format.PED;
            case "fastq":
                return File.Format.FASTQ;
            case "tsv":
                return File.Format.TAB_SEPARATED_VALUES;
            case "csv":
                return File.Format.COMMA_SEPARATED_VALUES;
            case "txt":
            case "log":
                return File.Format.PLAIN;
            case "xml":
                return File.Format.XML;
            case "json":
                return File.Format.JSON;
            case "proto":
                return File.Format.PROTOCOL_BUFFER;
            case "avro":
                return File.Format.AVRO;
            case "parquet":
                return File.Format.PARQUET;
            case "png":
            case "bmp":
            case "svg":
            case "gif":
            case "jpeg":
            case "tif":
                return File.Format.IMAGE;
            default:
                break;
        }

        //PLAIN
        return File.Format.UNKNOWN;
    }

    public static File.Compression detectCompression(URI uri) {
        String fileExtension = com.google.common.io.Files.getFileExtension(uri.getPath());
        return getCompression(fileExtension);
    }

    public static File.Compression getCompression(String fileExtension) {
        File.Compression compression;

        // Check if fileExtension is null
        if (fileExtension == null) {
            return File.Compression.NONE;
        }

        switch (fileExtension.toLowerCase()) {
            case "gz":
            case "gzip":
                compression = File.Compression.GZIP;
                break;
            case "zip":
                compression = File.Compression.ZIP;
                break;
            case "snappy":
            case "snz":
                compression = File.Compression.SNAPPY;
                break;
            default:
                compression = File.Compression.NONE;
                break;
        }
        return compression;
    }

}
