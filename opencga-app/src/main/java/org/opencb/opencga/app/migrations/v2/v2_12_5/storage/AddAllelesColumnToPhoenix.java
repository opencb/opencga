package org.opencb.opencga.app.migrations.v2.v2_12_5.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

@Migration(id="add_missing_column_to_phoenix_TASK-6005", description = "Add missing ALLELES column to phoenix #TASK-6005",
        version = "2.12.5", domain = Migration.MigrationDomain.STORAGE, date = 20240510
)
public class AddAllelesColumnToPhoenix extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
        for (String project : getVariantStorageProjects(organizationId)) {
            VariantStorageEngine engine = getVariantStorageEngineByProject(project);
            if (engine.getStorageEngineId().equals("hadoop")) {
                logger.info("Adding missing columns (if any) for project " + project);
                // Using same class for both migrations
                Class<?> aClass = Class.forName("org.opencb.opencga.storage.hadoop.variant.migration.v2_3_0.AddMissingColumns");
                Runnable runnable = (Runnable) aClass
                        .getConstructor(Object.class)
                        .newInstance(engine);
                runnable.run();
            }
        }
    }
}