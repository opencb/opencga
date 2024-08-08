package org.opencb.opencga.catalog.migration;

public class MigrationRuntimeException extends IllegalArgumentException {

    public MigrationRuntimeException(String message) {
        super(message);
    }

    public MigrationRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

}
