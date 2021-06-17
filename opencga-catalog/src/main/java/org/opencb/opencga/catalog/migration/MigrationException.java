package org.opencb.opencga.catalog.migration;

import org.opencb.opencga.catalog.exceptions.CatalogException;

public class MigrationException extends CatalogException {

    public MigrationException(String message) {
        super(message);
    }

    public MigrationException(String message, Throwable cause) {
        super(message, cause);
    }

    public MigrationException(Throwable cause) {
        super(cause);
    }
}
