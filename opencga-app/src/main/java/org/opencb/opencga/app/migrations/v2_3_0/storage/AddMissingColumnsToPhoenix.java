package org.opencb.opencga.app.migrations.v2_3_0.storage;

import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

@Migration(id="add_missing_columns_to_phoenix_TASK-789", description = "Add missing 1000G columns to phoenix #TASK-789",
        version = "2.3.0", domain = Migration.MigrationDomain.STORAGE, date = 20220614
)
public class AddMissingColumnsToPhoenix extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
        for (String project : getVariantStorageProjects()) {
            VariantStorageEngine engine = getVariantStorageEngineByProject(project);
            if (engine.getStorageEngineId().equals("hadoop")) {
                logger.info("Adding missing columns (if any) for project " + project);
                Class<?> aClass = Class.forName("org.opencb.opencga.storage.hadoop.variant.migration.v2_3_0.AddMissingColumns");
                Runnable runnable = (Runnable) aClass
                        .getConstructor(Object.class)
                        .newInstance(engine);
                runnable.run();
            }
        }
    }
}
