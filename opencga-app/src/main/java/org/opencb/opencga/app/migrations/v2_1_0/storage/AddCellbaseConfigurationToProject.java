package org.opencb.opencga.app.migrations.v2_1_0.storage;


import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.project.Project;

import static org.opencb.opencga.core.api.ParamConstants.USER_PROJECT_SEPARATOR;

@Migration(id = "add_cellbase_configuration_to_project", description = "Add cellbase configuration from storage-configuration.yml to project.internal.cellbase", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 3,
        date = 20210616)
public class AddCellbaseConfigurationToProject extends MigrationTool {

    @Override
    protected void run() throws Exception {
        StorageConfiguration storageConfiguration = readStorageConfiguration();
//        StorageEngineFactory engineFactory = StorageEngineFactory.get(storageConfiguration);
//        VariantStorageManager variantStorageManager = new VariantStorageManager(catalogManager, engineFactory);

        for (Project project : catalogManager.getProjectManager().search(new Query(), QueryOptions.empty(), token).getResults()) {
            if (project.getInternal() == null || project.getCellbase() == null) {
                String userToken = catalogManager.getUserManager()
                        .getNonExpiringToken(project.getFqn().split(USER_PROJECT_SEPARATOR)[0], token);
                catalogManager.getProjectManager()
                        .setCellbaseConfiguration(project.getFqn(), storageConfiguration.getCellbase(), userToken);
            }
        }
    }
}
