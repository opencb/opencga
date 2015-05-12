package org.opencb.opencga.catalog.exceptions;

/**
 * Created by hpccoll1 on 12/05/15.
 */
public class CatalogParameterException extends CatalogException {
    public CatalogParameterException(String message) {
        super(message);
    }

    public CatalogParameterException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogParameterException(Throwable cause) {
        super(cause);
    }
}
