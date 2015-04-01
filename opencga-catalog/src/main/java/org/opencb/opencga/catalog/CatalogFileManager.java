package org.opencb.opencga.catalog;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by jacobo on 6/11/14.
 */
public class CatalogFileManager {

    private final CatalogManager catalogManager;
    private static Logger logger = LoggerFactory.getLogger(CatalogFileManager.class);

    public CatalogFileManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public void upload(URI sourceUri, File file, String sourceChecksum, String sessionId,
                       boolean ignoreStatus, boolean overwrite, boolean deleteSource, boolean calculateChecksum)
            throws CatalogIOManagerException {
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
     * @throws CatalogIOManagerException
     */
    public void upload(URI sourceUri, File file, String sourceChecksum, String sessionId,
                       boolean ignoreStatus, boolean overwrite, boolean deleteSource, boolean calculateChecksum, long moveThreshold)
            throws CatalogIOManagerException {

        URI targetUri;
        CatalogIOManager sourceIOManager;
        CatalogIOManager targetIOManager;
        try {
            targetUri = catalogManager.getFileUri(file);
            sourceIOManager = catalogManager.getCatalogIOManagerFactory().get(sourceUri.getScheme());
            targetIOManager = catalogManager.getCatalogIOManagerFactory().get(targetUri.getScheme());
        } catch (CatalogDBException | CatalogIOManagerException e) {
            throw new CatalogIOManagerException("Can't upload file.", e);
        }

        if(deleteSource) {
            //TODO: Check if can delete source
        }

        //Check status
        switch (file.getStatus()) {
            case UPLOADING:
                break;

            case UPLOADED:
            case READY:
                if(!ignoreStatus) {
                    throw new CatalogIOManagerException("File status is already uploaded and ready! " +
                            "file:{id:" + file.getId() + ", status: '" + file.getStatus() + "' } " +
                            "Needs 'ignoreStatus = true' for continue.");
                }
                break;
        }

        //Check if there is any file in target
        if (!overwrite && targetIOManager.exists(targetUri)) {
            throw new CatalogIOManagerException("There is a file in the target!" +
                    "file:{id:" + file.getId() + ", targetUri: '" + targetUri + "' } " +
                    "Needs 'overwrite = true' for continue.");
        }

        // Get file stats
        long size = 0;
        try {
            size = sourceIOManager.getFileSize(sourceUri);
        } catch (CatalogIOManagerException e) {
            e.printStackTrace();
            logger.error("Can't get fileSize", e);
        }
        String creationDate = TimeUtils.getTime();
//        String creationDate = sourceIOManager.getCreationDate(sourceUri);


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
            } catch (IOException | CatalogIOManagerException e) {
                e.printStackTrace();
                logger.error("Error moving a file.", e);
            }
        }

        //If there was an error moving file or have to be copied
        if (!fileMoved) {
            try {
                copy(sourceIOManager, sourceUri, targetIOManager, targetUri);
            } catch (CatalogIOManagerException | IOException e) {
                throw new CatalogIOManagerException("Error while copying file. ", e);
            }

            try {
                if (calculateChecksum) {
                    targetChecksum = targetIOManager.calculateChecksum(targetUri);
                } else {
                    targetChecksum = sourceChecksum;
                }
            } catch (CatalogIOManagerException catalogIOManagerException) {
                try {
                    targetIOManager.deleteFile(targetUri);
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                    //You fail at failing!
                    throw new CatalogIOManagerException(
                            "Fail calculating target checksum : " + catalogIOManagerException.getMessage() + "" +
                            "Fail deleting target file : " + ioException.getMessage(), catalogIOManagerException);
                }
                throw catalogIOManagerException;
            }
        }

        //Check status
        if(!calculateChecksum || targetChecksum.equals(sourceChecksum)) {
            ObjectMap attributes = new ObjectMap();
            ObjectMap parameters = new ObjectMap();

            if (calculateChecksum) {
                logger.info("Checksum matches {}", sourceChecksum);
                attributes.put("checksum", sourceChecksum);
            } else {
                logger.info("Checksum not computed.");
            }

            //Update file
            parameters.put("status", File.Status.READY);
            parameters.put("diskUsage", size);
            parameters.put("creationDate", creationDate);
            parameters.put("attributes", attributes);

            try {
                catalogManager.modifyFile(file.getId(), parameters, sessionId);
            } catch (CatalogException e) {
                throw new CatalogIOManagerException("Can't update file properties in Catalog.", e);
            }

            if(deleteSource && !fileMoved) {
                logger.info("Deleting file {} ", sourceUri);
                try {
                    sourceIOManager.deleteFile(sourceUri);
                } catch (IOException e) {
                    throw new CatalogIOManagerException("Can't delete source.", e);
                }
            }
        } else {
            throw new CatalogIOManagerException("Checksum mismatches at moving files. " + sourceChecksum + " =! " + targetChecksum);
        }

    }

    /**
     *
     * @param sourceIOManager   Source IOManager
     * @param source            Source file
     * @param targetIOManager   Target IOManager
     * @param target            Target file
     * @throws IOException
     * @throws CatalogIOManagerException
     */
    private void copy(CatalogIOManager sourceIOManager, URI source, CatalogIOManager targetIOManager, URI target)
            throws IOException, CatalogIOManagerException {

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
