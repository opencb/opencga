package org.opencb.opencga.catalog.db;

/**
 * Created by imedina on 11/09/14.
 */
public class CatalogDBException extends Exception {

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
