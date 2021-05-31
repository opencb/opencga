package org.opencb.opencga.app.migrations.v2_0_3.catalog.java;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;

@Migration(id="recalculate_roles", description = "Recalculate roles from Family #1763", version = "2.0.3", rank = 1)
public class Migration1 extends MigrationTool {

    @Override
    protected void run() throws CatalogException {
        // Add automatically roles to all the family members
        QueryOptions familyUpdateOptions = new QueryOptions(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, true);
        for (Project project : catalogManager.getProjectManager().get(new Query(), new QueryOptions(), token).getResults()) {
            if (project.getStudies() != null) {
                for (Study study : project.getStudies()) {
                    logger.info("Updating family roles from study {}", study.getFqn());
                    catalogManager.getFamilyManager().update(study.getFqn(), new Query(), null, familyUpdateOptions, token);
                }
            }
        }
    }
}
