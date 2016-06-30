package org.opencb.opencga.storage.mongodb.variant.exceptions;

import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager.MongoDBVariantOptions;

import java.util.List;

/**
 * Created on 30/06/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoVariantStorageManagerException extends StorageManagerException {

    public MongoVariantStorageManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public MongoVariantStorageManagerException(String message) {
        super(message);
    }

    public static MongoVariantStorageManagerException operationInProgressException(BatchFileOperation opInProgress) {
        return new MongoVariantStorageManagerException("Can not load any file while there is "
                + "an operation \"" + opInProgress.getOperationName() + "\" "
                + "in status \"" + opInProgress.currentStatus() + "\" for files " + opInProgress.getFileIds() + ". "
                + "Finish operations to continue.");
    }

    public static MongoVariantStorageManagerException filesBeingMergedException(List<Integer> fileIds) {
        return new MongoVariantStorageManagerException(
                "Files " + fileIds + " are already being loaded in the variants collection "
                        + "right now. To ignore this, relaunch with " + MongoDBVariantOptions.MERGE_RESUME.key() + "=true");
    }

    public static MongoVariantStorageManagerException fileBeingStagedException(int fileId, String fileName) {
        return new MongoVariantStorageManagerException(
                "File \"" + fileName + "\" (" + fileId + ") is already being loaded in the stage collection "
                        + "right now. To ignore this, relaunch with " + MongoDBVariantOptions.STAGE_RESUME.key() + "=true");
    }
}
