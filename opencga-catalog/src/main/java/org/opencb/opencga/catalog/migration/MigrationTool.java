package org.opencb.opencga.catalog.migration;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.managers.CatalogManager;

public abstract class MigrationTool {

    protected CatalogManager catalogManager;
    protected ObjectMap params;

    public MigrationTool() {
    }

    public final String getId() {
        return "";
    }

    public final void setup(CatalogManager catalogManager, ObjectMap params) {
        this.catalogManager = catalogManager;
        this.params = params;
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
