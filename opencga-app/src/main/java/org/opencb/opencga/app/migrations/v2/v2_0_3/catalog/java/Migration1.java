package org.opencb.opencga.app.migrations.v2.v2_0_3.catalog.java;

import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "recalculate_roles", description = "Recalculate roles from Family #1763", version = "2.0.3", date = 20210528,
    deprecatedSince = "v3.0.0")
public class Migration1 extends MigrationTool {

    @Override
    protected void run() throws CatalogException {
    }
}
