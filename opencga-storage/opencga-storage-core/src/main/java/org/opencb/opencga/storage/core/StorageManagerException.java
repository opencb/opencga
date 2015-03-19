package org.opencb.opencga.storage.core;

/**
 * Created by jacobo on 1/02/15.
 */
public class StorageManagerException extends Exception {
    public StorageManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageManagerException(String message) {
        super(message);
    }
}
