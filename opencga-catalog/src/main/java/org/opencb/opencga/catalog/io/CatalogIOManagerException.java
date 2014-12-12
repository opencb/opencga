package org.opencb.opencga.catalog.io;


import org.opencb.opencga.catalog.CatalogException;

public class CatalogIOManagerException extends CatalogException {

    private static final long serialVersionUID = 1L;

    public CatalogIOManagerException(String msg) {
        super(msg);
    }

    public CatalogIOManagerException(String message, Throwable cause) {
        super(message, cause);
    }
}
