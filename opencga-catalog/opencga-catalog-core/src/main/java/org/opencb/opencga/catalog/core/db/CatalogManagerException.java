package org.opencb.opencga.catalog.core.db;

/**
 * Created by imedina on 11/09/14.
 */
public class CatalogManagerException extends Exception {

    public CatalogManagerException(String msg) {
        super(msg);
    }

    public CatalogManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogManagerException(Throwable cause) {
        super(cause);
    }
}
