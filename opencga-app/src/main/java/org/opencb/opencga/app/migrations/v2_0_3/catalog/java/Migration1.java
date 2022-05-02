package org.opencb.opencga.app.migrations.v2_0_3.catalog.java;

import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.concurrent.TimeUnit;

@Migration(id = "recalculate_roles", description = "Recalculate roles from Family #1763", version = "2.0.3", date = 20210528)
public class Migration1 extends MigrationTool {

    @Override
    protected void run() throws CatalogException {
        // Add automatically roles to all the family members
        StopWatch stopWatch = new StopWatch();

        QueryOptions familyUpdateOptions = new QueryOptions(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, true);
        QueryOptions queryOptions = new QueryOptions(FamilyManager.INCLUDE_FAMILY_IDS)
                .append(QueryOptions.LIMIT, 1000);
        for (Project project : catalogManager.getProjectManager().search(new Query(), new QueryOptions(), token).getResults()) {
            if (project.getStudies() != null) {
                for (Study study : project.getStudies()) {
                    stopWatch.start();

                    int skip = 0;
                    boolean more = true;
                    do {
                        queryOptions.put(QueryOptions.SKIP, skip);

                        OpenCGAResult<Family> search = catalogManager.getFamilyManager().search(study.getFqn(), new Query(), queryOptions,
                                token);
                        if (search.getNumResults() < 1000) {
                            more = false;
                        }
                        skip += search.getNumResults();

                        for (Family family : search.getResults()) {
                            try {
                                catalogManager.getFamilyManager().update(study.getFqn(), family.getId(), null, familyUpdateOptions, token);
                                logger.info("Updated roles from family '{}' - '{}'", study.getFqn(), family.getId());
                            } catch (CatalogException e) {
                                continue;
                            }
                        }

                        logger.info("Updated {} families in study '{}' in {} seconds", skip, study.getFqn(),
                                stopWatch.getTime(TimeUnit.SECONDS));
                    } while (more);

                    stopWatch.stop();
                    stopWatch.reset();
                }
            }
        }
    }
}
