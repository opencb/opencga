package org.opencb.opencga.app.migrations.v2.v2_0_6.catalog;


import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

@Migration(id = "remove_file_references_from_sample", description = "Remove deleted file references from samples #1815", version = "2.0.6",
        date = 20210901, deprecatedSince = "3.0.0")
public class removeDeletedFileReferencesFromSample extends MigrationTool {

    @Override
    protected void run() throws CatalogException {
    }

}
