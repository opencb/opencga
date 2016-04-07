package org.opencb.opencga.storage.core.exceptions;

import org.opencb.opencga.storage.core.StorageETLResult;

import java.util.List;

/**
 * Exception during the ETL pipeline.
 *
 * Includes a list of {@link StorageETLResult} with the execution
 * details, where at least one of them has failed.
 *
 * Created on 04/04/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StorageETLException extends StorageManagerException {

    private final List<StorageETLResult> results;

    public StorageETLException(String message, Throwable cause, List<StorageETLResult> results) {
        super(message, cause);
        this.results = results;
    }

    public StorageETLException(String message, List<StorageETLResult> results) {
        super(message);
        this.results = results;
    }

    public List<StorageETLResult> getResults() {
        return results;
    }
}
