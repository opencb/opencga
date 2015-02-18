package org.opencb.opencga.catalog;

/**
 * Created by jacobo on 12/12/14.
 */
public class CatalogException extends Exception  {

    public CatalogException(String message) {
        super(message);
    }

    public CatalogException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogException(Throwable cause) {
        super(cause);
    }
}
