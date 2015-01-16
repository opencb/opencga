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
     * @param deleteSource      After moving, delete file
     * @param secureMove        Don't check checksum. (pending)
     * @throws CatalogIOManagerException
     */
    public void upload(URI sourceUri, File file, String sourceChecksum, String sessionId,
                       boolean ignoreStatus, boolean overwrite, boolean deleteSource, boolean secureMove)
            throws CatalogIOManagerException {

        URI targetUri;
        CatalogIOManager sourceIOManager;
        CatalogIOManager targetIOManager;
        try {
            targetUri = catalogManager.getFileUri(file);
            sourceIOManager = catalogManager.getCatalogIOManagerFactory().get(sourceUri.getScheme());
            targetIOManager = catalogManager.getCatalogIOManagerFactory().get(targetUri.getScheme());
        } catch (CatalogDBException | CatalogIOManagerException | IOException e) {
            throw new CatalogIOManagerException("Can't upload file.", e);
        }

        if(deleteSource) {
            //TODO: Check if can delete source
        }

//        long size = sourceIOManager.getFileSize(sourceUri);       //TODO: Add fileStats methods
//        String creationDate = sourceIOManager.getCreationDate(sourceUri);
        long size = 0;
        String creationDate = TimeUtils.getTime();


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

        if(sourceChecksum == null || sourceChecksum.isEmpty()) {
            sourceChecksum = sourceIOManager.calculateChecksum(sourceUri);
        }

        try {
            copy(sourceIOManager, sourceUri, targetIOManager, targetUri);
        } catch (CatalogIOManagerException | IOException e) {
            throw new CatalogIOManagerException("Error while copying file. ", e);
        }

        String targetChecksum;
        try {
            targetChecksum = targetIOManager.calculateChecksum(targetUri);
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

        if(targetChecksum.equals(sourceChecksum)) {
            logger.info("Checksum matches {}", sourceChecksum);

            //Update file
            ObjectMap parameters = new ObjectMap();
            parameters.put("status", File.Status.READY);
//            parameters.put("diskUsage", size);  // TODO
//            parameters.put("creationDate", creationDate);
            parameters.put("attributes", new ObjectMap("checksum", targetChecksum));
            try {
                catalogManager.modifyFile(file.getId(), parameters, sessionId);
            } catch (CatalogException e) {
                throw new CatalogIOManagerException("Can't update file properties in Catalog.", e);
            }

            if(deleteSource) {
                logger.info("Deleting file {} ", sourceUri);
                try {
                    sourceIOManager.deleteFile(sourceUri);
                } catch (IOException e) {
                    throw new CatalogIOManagerException("Can't delete source.", e);
                }
            }
        } else {
            throw new CatalogIOManagerException("Checksum mismatches at moving files.");
        }

    }

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
