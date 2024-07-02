package org.opencb.opencga.master.monitor.daemons;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.ToolFactory;
import org.opencb.opencga.analysis.variant.operations.VariantAnnotationIndexOperationTool;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.analysis.variant.operations.VariantSecondaryAnnotationIndexOperationTool;
import org.opencb.opencga.analysis.variant.operations.VariantSecondarySampleIndexOperationTool;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.OperationConfig;
import org.opencb.opencga.core.config.OperationExecutionConfig;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.variant.OperationIndexStatus;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class VariantOperationOrchestrator {

    public static final String ORCHESTRATOR_TAG = "VariantOperationOrchestrator";
    private final CatalogManager catalogManager;
    private final OperationConfig operationConfig;
    private final String token;

    private static final String ATTEMPT = "attempt";
    private static final String FAILED_ATTEMPT_JOB_IDS = "failedAttemptJobIds";

    protected static Logger logger = LoggerFactory.getLogger(VariantOperationOrchestrator.class);

    /**
     * Initialize VariantOperationOrchestrator with the catalog manager, configuration and token.
     *
     * @param catalogManager Instance of a working CatalogManager.
     * @param operationConfig  Main configuration file.
     * @param token          Valid administrator token.
     */
    public VariantOperationOrchestrator(CatalogManager catalogManager, Configuration operationConfig, String token) {
        this.catalogManager = catalogManager;
        if (operationConfig.getAnalysis().getOperations() == null) {
            this.operationConfig = new OperationConfig();
        } else {
            this.operationConfig = operationConfig.getAnalysis().getOperations();
        }
        this.token = token;
    }

    public void checkPendingVariantOperations() throws CatalogException, ToolException {
        checkPendingVariantOperations(operationConfig.getAnnotationIndex(), new VariantAnnotationIndexOperationRules());
        checkPendingVariantOperations(operationConfig.getVariantSecondaryAnnotationIndex(),
                new VariantSecondaryAnnotationIndexOperationRules());
        checkPendingVariantOperations(operationConfig.getVariantSecondarySampleIndex(), new VariantSecondarySampleIndexOperationRules());
    }

    private void checkPendingVariantOperations(OperationExecutionConfig config, OperationRules operationRules)
            throws CatalogException, ToolException {
        String toolId = operationRules.getToolId();
        if (config.getPolicy() == OperationExecutionConfig.Policy.NEVER) {
            logger.info("Automatic operation '{}' is disabled. Nothing to do.", toolId);
            return;
        }

        if (!isNightTime() && config.getPolicy() == OperationExecutionConfig.Policy.NIGHTLY) {
            logger.info("Waiting until night time to check for pending operation '{}'", toolId);
            return;
        }

        List<String> organizationIds = catalogManager.getOrganizationManager().getOrganizationIds(token);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.PROJECTS.key());
        for (String organizationId : organizationIds) {
            Organization organization = catalogManager.getOrganizationManager().get(organizationId, options, token).first();
            for (Project project : organization.getProjects()) {
                if (CollectionUtils.isEmpty(project.getStudies())) {
                    // Empty project. Skip
                    continue;
                }

                Tool tool = new ToolFactory().getTool(toolId);
                if (tool.scope() == Tool.Scope.PROJECT) {
                    List<String> studyFqns = project.getStudies().stream().map(Study::getFqn).collect(Collectors.toList());
                    // 1. Check if operation is pending
                    operationRules.isOperationRequired(project, null);

                    // 2. Check if the operation is already created on any study of the project
                    if (pendingJobs(studyFqns, toolId)) {
                        logger.info("There's already a pending job for tool '{}' in project '{}'. Skipping.", toolId, project.getFqn());
                        continue;
                    }

                    // 3. Check general rules
                    if (noPendingJobs(studyFqns, operationRules.dependantTools())) {
                        // Get last execution of this job
                        Job lastJobExecution = findLastJobExecution(studyFqns.get(0), toolId);
                        ObjectMap attributes = getNewAttributes(lastJobExecution);
                        if (attributes.getInt(ATTEMPT) > config.getMaxAttempts()) {
                            logger.info("Max attempts reached for tool '{}' in project '{}'. Skipping.", toolId, project.getFqn());
                            continue;
                        }

                        Map<String, Object> paramsMap;
                        if (config.getJobParams() == null) {
                            paramsMap = new HashMap<>(config.getJobParams());
                        } else {
                            paramsMap = new HashMap<>();
                        }
                        paramsMap.put(ParamConstants.PROJECT_PARAM, project.getFqn());
                        catalogManager.getJobManager().submit(studyFqns.get(0), toolId, Enums.Priority.HIGH, paramsMap, null,
                                generateJobDescription(config, operationRules, attributes), null,
                                Collections.singletonList(ORCHESTRATOR_TAG), attributes, token);
                    }
                } else if (tool.scope() == Tool.Scope.STUDY) {
                    for (Study study : project.getStudies()) {
                        // 1. Check if operation is pending
                        operationRules.isOperationRequired(project, study);

                        // 2. Check if the operation is already created
                        if (pendingJobs(study.getFqn(), toolId)) {
                            // Pending jobs of its own type. Skip study
                            logger.info("There's already a pending job for tool '{}' in study '{}'. Skipping.", toolId, study.getFqn());
                            continue;
                        }

                        // 3. Check general rules
                        if (noPendingJobs(study.getFqn(), operationRules.dependantTools())) {
                            // Get last execution of this job
                            Job lastJobExecution = findLastJobExecution(study.getFqn(), toolId);
                            ObjectMap attributes = getNewAttributes(lastJobExecution);
                            if (attributes.getInt(ATTEMPT) > config.getMaxAttempts()) {
                                logger.info("Max attempts reached for tool '{}' in study '{}'. Skipping.", toolId, study.getFqn());
                                continue;
                            }

                            Map<String, Object> paramsMap;
                            if (config.getJobParams() == null) {
                                paramsMap = new HashMap<>(config.getJobParams());
                            } else {
                                paramsMap = new HashMap<>();
                            }
                            paramsMap.put(ParamConstants.STUDY_PARAM, study.getFqn());
                            catalogManager.getJobManager().submit(study.getFqn(), toolId, Enums.Priority.HIGH, paramsMap, null,
                                    generateJobDescription(config, operationRules, attributes), null,
                                    Collections.singletonList(ORCHESTRATOR_TAG), attributes, token);
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected tools with scope " + tool.scope() + " for tool id '" + toolId + "'");
                }
            }
        }
    }

    private String generateJobDescription(OperationExecutionConfig config, OperationRules operationRules, ObjectMap attributes) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Job automatically launched by the orchestrator: \n");
        stringBuilder.append("- Tool: ").append(operationRules.getToolId()).append(".\n");
        stringBuilder.append("- Policy: ").append(config.getPolicy()).append(".\n");
        stringBuilder.append("- Number of attempts: ").append(attributes.getInt(ATTEMPT)).append(" out of ").append(config.getMaxAttempts())
                .append(".\n");
        String jobIds = StringUtils.join(attributes.getAsStringList(FAILED_ATTEMPT_JOB_IDS), ", ");
        stringBuilder.append("- Job ids from previous attempts: ").append(jobIds).append(".\n");
        return stringBuilder.toString();
    }

    private interface OperationRules {

        String getToolId();

        boolean isOperationRequired(Project project, Study study);

        List<String> dependantTools();

    }

    private class VariantSecondarySampleIndexOperationRules implements OperationRules {

        @Override
        public String getToolId() {
            return VariantSecondarySampleIndexOperationTool.ID;
        }

        @Override
        public boolean isOperationRequired(Project project, Study study) {
            return study.getInternal().getVariant().getSecondarySampleIndex().getStatus().getId().equals(OperationIndexStatus.PENDING);
        }

        @Override
        public List<String> dependantTools() {
            return Collections.unmodifiableList(Arrays.asList(VariantIndexOperationTool.ID, VariantAnnotationIndexOperationTool.ID,
                    VariantSecondaryAnnotationIndexOperationTool.ID));
        }
    }

    private class VariantSecondaryAnnotationIndexOperationRules implements OperationRules {

        @Override
        public String getToolId() {
            return VariantSecondaryAnnotationIndexOperationTool.ID;
        }

        @Override
        public boolean isOperationRequired(Project project, Study study) {
            return project.getInternal().getVariant().getSecondaryAnnotationIndex().getStatus().getId()
                    .equals(OperationIndexStatus.PENDING);
        }

        @Override
        public List<String> dependantTools() {
            return Collections.unmodifiableList(Arrays.asList(VariantIndexOperationTool.ID, VariantAnnotationIndexOperationTool.ID));
        }
    }

    private class VariantAnnotationIndexOperationRules implements OperationRules {

        @Override
        public String getToolId() {
            return VariantAnnotationIndexOperationTool.ID;
        }

        @Override
        public boolean isOperationRequired(Project project, Study study) {
            return project.getInternal().getVariant().getAnnotationIndex().getStatus().getId().equals(OperationIndexStatus.PENDING);
        }

        @Override
        public List<String> dependantTools() {
            return Collections.unmodifiableList(Collections.singletonList(VariantIndexOperationTool.ID));
        }
    }

    private static boolean isNightTime() {
        boolean isNightTime = false;
        Calendar calendar = Calendar.getInstance();
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        // Check date time is between 00:00 and 05:00
        if (hour < 5) {
            isNightTime = true;
        }
        return isNightTime;
    }

    private boolean pendingJobs(String studyIds, String toolId) throws CatalogException {
        return !noPendingJobs(Collections.singletonList(studyIds), Collections.singletonList(toolId));
    }

    private boolean pendingJobs(List<String> studyIds, String toolId) throws CatalogException {
        return !noPendingJobs(studyIds, Collections.singletonList(toolId));
    }

    private boolean noPendingJobs(List<String> studyIds, String toolId) throws CatalogException {
        return noPendingJobs(studyIds, Collections.singletonList(toolId));
    }

    private boolean noPendingJobs(String studyId, List<String> toolIds) throws CatalogException {
        return noPendingJobs(Collections.singletonList(studyId), toolIds);
    }

    /**
     * Check if there is any pending (or queueud or running) job in any of the listed studies, for any of the listed tools.
     * @param studyIds List of studies where to check
     * @param toolIds  List of tools to check
     * @return         If there is any pending job
     * @throws CatalogException on error
     */
    private boolean noPendingJobs(List<String> studyIds, List<String> toolIds) throws CatalogException {
        for (String studyId : studyIds) {
            Query query = new Query()
                    .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Arrays.asList(Enums.ExecutionStatus.PENDING,
                            Enums.ExecutionStatus.QUEUED, Enums.ExecutionStatus.RUNNING))
                    .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), toolIds);

            long numMatches = catalogManager.getJobManager().count(studyId, query, token).getNumMatches();
            if (numMatches != 0) {
                return false;
            }
        }
        return true;
    }

    private Job findLastJobExecution(String studyId, String toolId) throws CatalogException {
            Query query = new Query()
                    .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), toolId);
            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.SORT, JobDBAdaptor.QueryParams.CREATION_DATE.key())
                    .append(QueryOptions.ORDER, QueryOptions.DESCENDING)
                    .append(QueryOptions.LIMIT, 1);

            OpenCGAResult<Job> search = catalogManager.getJobManager().search(studyId, query, queryOptions, token);
            return search.getNumResults() > 0 ? search.first() : null;
    }

    private ObjectMap getNewAttributes(Job job) {
        ObjectMap attributes = new ObjectMap();
        // If job was launched by the Orchestrator and failed, extract number of attempts
        if (job != null && (Enums.ExecutionStatus.ERROR.equals(job.getInternal().getStatus().getId())
                || Enums.ExecutionStatus.ABORTED.equals(job.getInternal().getStatus().getId()))
                && job.getTags().contains(ORCHESTRATOR_TAG)) {
            ObjectMap jobAttributes = new ObjectMap(job.getAttributes());
            int jobAttempt = jobAttributes.getInt(ATTEMPT);

            List<String> jobIds = new ArrayList<>(jobAttributes.getAsStringList(FAILED_ATTEMPT_JOB_IDS));
            // Add last job id
            jobIds.add(job.getId());

            attributes.put(ATTEMPT, jobAttempt + 1);
            attributes.put(FAILED_ATTEMPT_JOB_IDS, jobIds);
        } else {
            attributes.put(ATTEMPT, 1);
            attributes.put(FAILED_ATTEMPT_JOB_IDS, Collections.emptyList());
        }
        return attributes;
    }

}
