package org.opencb.opencga.catalog.db;

import org.opencb.opencga.catalog.CatalogException;

/**
 * Created by imedina on 11/09/14.
 */
public class CatalogDBException extends CatalogException {

    public CatalogDBException(String msg) {
        super(msg);
    }

    public CatalogDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogDBException(Throwable cause) {
        super(cause);
    }
}
