package org.opencb.opencga.catalog.exceptions;

/**
 * Created by wasim on 05/06/17.
 */
public class CatalogTokenException extends CatalogException{
    public CatalogTokenException(String message) {
        super(message);
    }

    public CatalogTokenException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogTokenException(Throwable cause) {
        super(cause);
    }
}
