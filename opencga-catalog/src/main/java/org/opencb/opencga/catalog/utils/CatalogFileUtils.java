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

package org.opencb.opencga.catalog.utils;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogFileUtils {

    private final CatalogManager catalogManager;
    private static Logger logger = LoggerFactory.getLogger(CatalogFileUtils.class);

    public CatalogFileUtils(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public void upload(URI sourceUri, File file, String sourceChecksum, String sessionId,
                       boolean ignoreStatus, boolean overwrite, boolean deleteSource, boolean calculateChecksum)
            throws CatalogException {
        upload(sourceUri, file, sourceChecksum, sessionId, ignoreStatus, overwrite, deleteSource, calculateChecksum, 10000000);
    }
    /**
     * Upload file to a created entry file in Catalog.
     *
     *
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
     * @throws org.opencb.opencga.catalog.exceptions.CatalogIOException
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

        if (deleteSource) {
            //TODO: Check if can delete source
        }

        //If the source is equals to the target, calculate checksum (if needed) and exit
        if (sourceUri.equals(targetUri)) {
            logger.info("SourceURI equals to TargetURI. Only calculate checksum and update file entry");
            String targetChecksum;
            if (calculateChecksum) {
                targetChecksum = targetIOManager.calculateChecksum(targetUri);
            } else {
                targetChecksum = sourceChecksum;
            }
            updateFileAttributes(file, targetChecksum, sessionId);
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
                    targetIOManager.deleteFile(targetUri);
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    //You fail at failing!
                    throw new CatalogIOException(
                            "Fail calculating target checksum : " + catalogIOException.getMessage() + "" +
                            "Fail deleting target file : " + ioException.getMessage(), catalogIOException);
                }
                throw catalogIOException;
            }
        }

        //Check status
        if(!calculateChecksum || targetChecksum.equals(sourceChecksum)) {
            if (calculateChecksum) {
                logger.info("Checksum matches {}", sourceChecksum);
            } else {
                logger.info("Checksum not computed.");
            }

            updateFileAttributes(file, sourceChecksum, sessionId);

            if(deleteSource && !fileMoved) {
                logger.info("Deleting file {} ", sourceUri);
                try {
                    sourceIOManager.deleteFile(sourceUri);
                } catch (IOException e) {
                    throw new CatalogIOException("Can't delete source.", e);
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

        updateFileAttributes(file, checksum, sessionId);

    }

    /**
     * Update some file attributes.
     *      Status -> ready
     *      diskUsage
     *      creationDate
     *      checksum
     * @throws CatalogException
     */
    private void updateFileAttributes(File file, String checksum, String sessionId) throws CatalogException {
        ObjectMap attributes = new ObjectMap();
        ObjectMap parameters = new ObjectMap();

        URI fileUri = catalogManager.getFileUri(file);
        CatalogIOManager catalogIOManager = catalogManager.getCatalogIOManagerFactory().get(fileUri);

        if (checksum != null && !checksum.isEmpty() && !checksum.equals("null")) {
            attributes.put("checksum", checksum);
        }
        long size = 0;
        try {
            size = catalogIOManager.getFileSize(fileUri);
        } catch (CatalogIOException e) {
            e.printStackTrace();
            logger.error("Can't get fileSize", e);
        }

        //String creationDate = catalogIOManager.getCreationDate(fileUri); //TODO
        String creationDate = TimeUtils.getTime();

        //Update file
        parameters.put("status", File.Status.READY);
        parameters.put("diskUsage", size);
        parameters.put("creationDate", creationDate);
        parameters.put("attributes", attributes);

        try {
            catalogManager.modifyFile(file.getId(), parameters, sessionId);
        } catch (CatalogException e) {
            throw new CatalogIOException("Can't update file properties in Catalog.", e);
        }
    }

    /**
     * Check if there is any file in the URI target.
     *
     * @throws org.opencb.opencga.catalog.exceptions.CatalogIOException
     */
    private void checkTarget(File file, URI targetUri, CatalogIOManager targetIOManager, boolean overwrite) throws CatalogIOException {
        if (!overwrite && targetIOManager.exists(targetUri)) {
            throw new CatalogIOException("There is a file in the target!" +
                    "file:{id:" + file.getId() + ", targetUri: '" + targetUri + "' } " +
                    "Needs 'overwrite = true' for continue.");
        }
    }

    /**
     * Check status. To upload a fileObject, the catalog file entry must be in status "uploading".
     * Set "ignoreStatus = true" to ignore the actual file status and replace the content.
     *
     * @throws org.opencb.opencga.catalog.exceptions.CatalogIOException
     */
    private void checkStatus(File file) throws CatalogIOException {
        if (file.getStatus() != File.Status.UPLOADING) {
            throw new CatalogIOException("File status is already uploaded and ready! " +
                    "file:{id:" + file.getId() + ", status: '" + file.getStatus() + "' } " +
                    "Needs 'ignoreStatus = true' for continue.");
        }
    }

    /**
     *
     * @param sourceIOManager   Source IOManager
     * @param source            Source file
     * @param targetIOManager   Target IOManager
     * @param target            Target file
     * @throws IOException
     * @throws org.opencb.opencga.catalog.exceptions.CatalogIOException
     */
    private void copy(CatalogIOManager sourceIOManager, URI source, CatalogIOManager targetIOManager, URI target)
            throws IOException, CatalogIOException {

        logger.info("Coping file from {} to {}", source, target);
        //If it's the same IOManager, copy intra FS
        if(sourceIOManager == targetIOManager) {
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
