package org.opencb.opencga.storage.core.exceptions;

import org.opencb.opencga.storage.core.StorageETLResult;

import java.util.List;

/**
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

}
