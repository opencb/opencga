package org.opencb.opencga.app.migrations.v2_8_0.catalog;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.family.PedigreeGraphInitAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationRun;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.catalog.utils.PedigreeGraphUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobReferenceParam;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;

import java.util.*;

@Migration(id = "calculate_pedigree_graph" ,
        description = "Calculate Pedigree Graph for all the families",
        version = "2.8.0",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20230313
)
public class CalculatePedigreeGraphMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MigrationRun migrationRun = getMigrationRun();

        // Map study studyFqn -> job
        Map<String, Job> jobs = new HashMap<>();
        for (JobReferenceParam jobReference : migrationRun.getJobs()) {
            Job job = catalogManager.getJobManager().get(jobReference.getStudyId(), jobReference.getId(), new QueryOptions(), token)
                    .first();
            logger.info("Reading already executed job '{}' for study '{}' with status '{}'",
                    job.getId(),
                    job.getStudy().getId(),
                    job.getInternal().getStatus().getId());
            jobs.put(job.getStudy().getId(), job);
        }

        Set<String> studies = new LinkedHashSet<>(getStudies());
        logger.info("Study IDs (num. total = {}) to initialize pedigree graphs: {}", studies.size(), StringUtils.join(studies, ", "));

        // Ensure that studies with already executed jobs are included in the migration run
        getMigrationRun().getJobs().forEach(j -> studies.add(j.getStudyId()));

        logger.info("Study IDs (num. total = {}) after adding studies from migration jobs: {}", studies.size(),
                StringUtils.join(studies, ", "));

        for (String study : studies) {
            Job job = jobs.get(study);
            if (job != null) {
                String status = job.getInternal().getStatus().getId();
                if (status.equals(Enums.ExecutionStatus.DONE)) {
                    // Skip this study. Already migrated
                    logger.info("Study {} already migrated", study);
                    continue;
                } else if (status.equals(Enums.ExecutionStatus.ERROR) || status.equals(Enums.ExecutionStatus.ABORTED)) {
                    logger.info("Retry migration job for study {}", study);
                } else {
                    logger.info("Job {} for migrating study {} in status {}. Wait for completion", job.getId(), study, status);
                    continue;
                }
                getMigrationRun().removeJob(job);
            }

            logger.info("Adding new job to migrate/initialize pedigree graph for study {}", study);
            ObjectMap params = new ObjectMap()
                    .append(ParamConstants.STUDY_PARAM, study);
            Job newJob = catalogManager.getJobManager().submit(study, PedigreeGraphInitAnalysis.ID, Enums.Priority.MEDIUM,
                    params, null, null, null, new ArrayList<>(), token).first();
            getMigrationRun().addJob(newJob);
        }
    }

    public List<String> getStudies() throws CatalogException {
        Set<String> studies = new LinkedHashSet<>();
        QueryOptions projectOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("id", "studies"));
        QueryOptions familyOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList("id", "members", "pedigreeGraph"));
        for (Project project : catalogManager.getProjectManager().search(new Query(), projectOptions, token).getResults()) {
            if (CollectionUtils.isNotEmpty(project.getStudies())) {
                for (Study study : project.getStudies()) {
                    String id = study.getFqn();
                    for (Family family : catalogManager.getFamilyManager().search(id, new Query(), familyOptions, token).getResults()) {
                        if (PedigreeGraphUtils.hasMinTwoGenerations(family)
                                && (family.getPedigreeGraph() == null || StringUtils.isEmpty(family.getPedigreeGraph().getBase64()))) {
                            studies.add(id);
                            break;
                        }
                    }
                }
            }
        }
        return new ArrayList<>(studies);
    }
}
