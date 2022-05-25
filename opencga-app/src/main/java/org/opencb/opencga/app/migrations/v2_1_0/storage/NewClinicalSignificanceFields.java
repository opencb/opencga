package org.opencb.opencga.app.migrations.v2_1_0.storage;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.operations.VariantAnnotationRebuilderOperationTool;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationRun;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.models.job.ExecutionReferenceParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


@Migration(id = "new_clinical_significance_fields", description = "Add new clinical significance fields and combinations for variant storage and solr", version = "2.1.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 1,
        date = 20210708)
public class NewClinicalSignificanceFields extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {
//        getVariantStorageManager().getStorageConfiguration().getVariantEngine().getEngine().equals("hadoop");
        MigrationRun migrationRun = getMigrationRun();
        Map<String, Execution> executions = new HashMap<>();
        for (ExecutionReferenceParam executionReference : migrationRun.getExecutions()) {
            Execution job = catalogManager.getExecutionManager().get(executionReference.getStudyId(), executionReference.getId(), new QueryOptions(), token)
                    .first();
            executions.put(job.getParams().get(ParamConstants.PROJECT_PARAM).toString(), job);
        }
        for (String project : getVariantStorageProjects()) {
            Execution execution = executions.get(project);
            if (execution != null) {
                int patch = Integer.parseInt(execution.getParams().get("patch").toString());
                String status = execution.getInternal().getStatus().getId();
                if (status.equals(Enums.ExecutionStatus.DONE)) {
                    if (patch == getAnnotation().patch()) {
                        // Skip this project. Already migrated
                        logger.info("Project {} already migrated", project);
                        continue;
                    } else {
                        logger.info("Rerun job, as the patch has changed.");
                    }
                } else if (status.equals(Enums.ExecutionStatus.ERROR) || status.equals(Enums.ExecutionStatus.ABORTED)) {
                    logger.info("Retry migration job for project {}", project);
                } else {
                    logger.info("Job {} for migrating project {} in status {}. Wait for completion", execution.getId(), project, status);
                    continue;
                }
                getMigrationRun().removeExecution(execution);
            }

            ObjectMap params = new ObjectMap()
                    .append(ParamConstants.PROJECT_PARAM, project)
                    .append("patch", getAnnotation().patch());
            getMigrationRun().addExecution(catalogManager.getExecutionManager().submitProject(project, VariantAnnotationRebuilderOperationTool.ID,
                    Enums.Priority.MEDIUM, params, null, null, null, new ArrayList<>(), token).first());
//            VariantStorageEngine storageEngine = getVariantStorageEngineByProject(project);
//            getVariantStorageManager()
        }
    }
}
