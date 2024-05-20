package org.opencb.opencga.app.migrations.v2.v2_0_5.catalog.java;


import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "initialise_groups", description = "Initialise userIds list from groups #1791", version = "2.0.5", date = 20210621,
        deprecatedSince = "v3.0.0")
public class initialiseGroups extends MigrationTool {

    @Override
    protected void run() throws CatalogException {
    }
}
