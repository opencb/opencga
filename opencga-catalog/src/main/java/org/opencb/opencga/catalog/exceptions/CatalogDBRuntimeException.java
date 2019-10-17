package org.opencb.opencga.catalog.exceptions;

public class CatalogDBRuntimeException extends RuntimeException {

    public CatalogDBRuntimeException(String message) {
        super(message);
    }

    public CatalogDBRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public CatalogDBRuntimeException(Throwable cause) {
        super(cause);
    }

}
