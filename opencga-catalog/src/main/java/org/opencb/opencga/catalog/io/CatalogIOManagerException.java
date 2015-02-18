package org.opencb.opencga.catalog.io;


import org.opencb.opencga.catalog.CatalogException;

import java.net.URISyntaxException;

public class CatalogIOManagerException extends CatalogException {

    private static final long serialVersionUID = 1L;

    public CatalogIOManagerException(String msg) {
        super(msg);
    }

    public CatalogIOManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public static CatalogIOManagerException uriSyntaxException(String name, URISyntaxException e) {
        return new CatalogIOManagerException("Uri syntax error while parsing \"" + name + "\"", e);
    }

}
