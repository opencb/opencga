package org.opencb.opencga.app.migrations.v2_0_5.catalog.java;


import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;

@Migration(id="initialise_groups", description = "Initialise userIds list from groups #1791", version = "2.0.5", rank = 1)
public class initialiseGroups extends MigrationTool {

    @Override
    protected void run() throws CatalogException {
        StopWatch stopWatch = new StopWatch();
        // Initialise all list of users from groups
        for (Project project : catalogManager.getProjectManager().get(new Query(), new QueryOptions(), token).getResults()) {
            if (project.getStudies() != null) {
                for (Study study : project.getStudies()) {
                    stopWatch.start();

                    for (Group group : study.getGroups()) {
                        if (group.getUserIds() == null) {
                            logger.info("Fixing group '{}' from study '{}'", group.getId(), study.getFqn());
                            catalogManager.getStudyManager().deleteGroup(study.getFqn(), group.getId(), token);

                            // Create it again. The new implementation takes now good care of the list of userIds
                            catalogManager.getStudyManager().createGroup(study.getFqn(), group, token);
                        }
                    }

                    stopWatch.stop();
                    stopWatch.reset();
                }
            }
        }
    }
}
