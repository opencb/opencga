package org.opencb.opencga.storage.hadoop.exceptions;

import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

/**
 * Any exception produced by the StorageHadoop plugin.
 *
 * Created on 11/03/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StorageHadoopException extends StorageManagerException {
    public StorageHadoopException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageHadoopException(String message) {
        super(message);
    }
}
