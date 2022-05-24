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

    public static MigrationException offlineMigrationException(Migration migration) {
        return new MigrationException("Migration '" + migration.id() + "' requires database to be offline. Please, ensure the database "
                + "cannot be accessed and run try again with '--offline' flag.");
    }
}
