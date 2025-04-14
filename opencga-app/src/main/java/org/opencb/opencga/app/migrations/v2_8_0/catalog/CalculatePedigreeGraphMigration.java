package org.opencb.opencga.app.migrations.v2_8_0.catalog;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.family.PedigreeGraphInitAnalysis;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FamilyManager;
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
        Map<String, List<Job>> jobsMap = new HashMap<>();
        for (JobReferenceParam jobReference : migrationRun.getJobs()) {
            Job job = catalogManager.getJobManager().get(jobReference.getStudyId(), jobReference.getId(), new QueryOptions(), token)
                    .first();
            logger.info("Reading already executed job '{}' for study '{}' with status '{}'",
                    job.getId(),
                    job.getStudy().getId(),
                    job.getInternal().getStatus().getId());
            jobsMap.computeIfAbsent(job.getStudy().getId(), k -> new ArrayList<>()).add(job);
        }

        Set<String> studies = new LinkedHashSet<>(getStudies());
        logger.info("Study IDs (num. total = {}) to initialize pedigree graphs: {}", studies.size(), StringUtils.join(studies, ", "));

        // Ensure that studies with already executed jobs are included in the migration run
        getMigrationRun().getJobs().forEach(j -> studies.add(j.getStudyId()));

        logger.info("Study IDs (num. total = {}) after adding studies from migration jobs: {}", studies.size(),
                StringUtils.join(studies, ", "));

        for (String study : studies) {
            List<Job> jobs = jobsMap.get(study);
            boolean migrated = false;
            boolean running = false;
            List<Job> errorJobs = new ArrayList<>();
            if (jobs != null) {
                for (Job job : jobs) {
                    String status = job.getInternal().getStatus().getId();
                    if (status.equals(Enums.ExecutionStatus.DONE)) {
                        migrated = true;
                    } else if (status.equals(Enums.ExecutionStatus.ERROR) || status.equals(Enums.ExecutionStatus.ABORTED)) {
                        logger.info("Retry migration job for study {}", study);
                        errorJobs.add(job);
                    } else {
                        running = true;
                        logger.info("Job {} for migrating study {} in status {}. Wait for completion", job.getId(), study, status);
                    }
                }
            }
            if (running) {
                // Skip this study. There is a job running
                logger.info("Study {} has a job running. Skip", study);
                continue;
            }
            // Remove error jobs if any
            for (Job errorJob : errorJobs) {
                logger.info("Remove error job {} for study {}", errorJob.getId(), study);
                getMigrationRun().removeJob(errorJob);
            }
            if (migrated) {
                // Skip this study. Already migrated
                logger.info("Study {} already migrated", study);
                continue;
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
        for (Project project : catalogManager.getProjectManager().search(new Query(), projectOptions, token).getResults()) {
            if (CollectionUtils.isNotEmpty(project.getStudies())) {
                for (Study study : project.getStudies()) {
                    String id = study.getFqn();
                    try (DBIterator<Family> iterator = catalogManager.getFamilyManager().iterator(id, new Query(), FamilyManager.INCLUDE_FAMILY_FOR_PEDIGREE, token)) {
                        while (iterator.hasNext()) {
                            Family family = iterator.next();
                            if (PedigreeGraphUtils.hasMinTwoGenerations(family)
                                    && (family.getPedigreeGraph() == null || StringUtils.isEmpty(family.getPedigreeGraph().getBase64()))) {
                                studies.add(id);
                                break;
                            }
                        }
                    }
                }
            }
        }
        return new ArrayList<>(studies);
    }
}
