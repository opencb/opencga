package org.opencb.opencga.catalog.migration;

import org.opencb.opencga.catalog.managers.CatalogManager;

public abstract class MigrationTool {

    protected CatalogManager catalogManager;

    public MigrationTool() {
    }

    public final String getId() {
        return "";
    }

    public final void setup(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public final void execute() throws MigrationException {
        try {
            run();
        } catch (MigrationException e) {
            throw e;
        } catch (Exception e) {
            throw new MigrationException("Error running  migration '" + getId() + "'");
        }
    }

    protected abstract void run() throws MigrationException;

}
