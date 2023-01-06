package org.opencb.opencga.app.migrations.v2_6_0.storage;

import org.apache.solr.common.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.core.config.storage.CellBaseConfiguration;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;

@Migration(id = "add_cellbase_data_release" ,
        description = "Add default cellbase data release if missing",
        version = "2.6.0",
        domain = Migration.MigrationDomain.STORAGE,
        language = Migration.MigrationLanguage.JAVA,
        date = 20230104
)
public class AddCellbaseDataRelease extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
        VariantStorageManager variantStorageManager = getVariantStorageManager();
        for (String projectFqn : getVariantStorageProjects()) {
            Project project = catalogManager.getProjectManager().get(projectFqn, new QueryOptions(), token).first();
            CellBaseConfiguration cellbase = project.getCellbase();
            CellBaseUtils cellBaseUtils = getVariantStorageEngineByProject(projectFqn).getCellBaseUtils();
            boolean updateCellbase = false;
            if (cellbase == null) {
                cellbase = new CellBaseConfiguration(cellBaseUtils.getURL(), cellBaseUtils.getVersion());
                updateCellbase = true;
            }

            if (StringUtils.isEmpty(cellbase.getDataRelease())) {
                if (cellBaseUtils.getDataRelease() != null) {
                    cellbase.setDataRelease(cellBaseUtils.getDataRelease());
                    updateCellbase = true;
                } else {
                    if (cellBaseUtils.supportsDataRelease()) {
                        cellbase.setDataRelease("1");
                        updateCellbase = true;
                    } else {
                        String serverVersion = cellBaseUtils.getVersionFromServer();
                        logger.info("DataRelease not supported on version '" + serverVersion + "' . Leaving empty");
                    }
                }
            }
            if (updateCellbase) {
                logger.info("Update cellbase info for project '{}' with '{}'", projectFqn, cellBaseUtils);
                variantStorageManager.setCellbaseConfiguration(projectFqn, null, false, null, token);
            }
        }
    }
}
