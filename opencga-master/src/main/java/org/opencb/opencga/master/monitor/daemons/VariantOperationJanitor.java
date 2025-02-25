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

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;

public class VariantOperationJanitor extends MonitorParentDaemon {

    public static final String TAG = "VariantOperationJanitor";
    private final CatalogManager catalogManager;
    private final OperationConfig operationConfig;
    private final String token;
    private final List<OperationChore> chores;

    static final String ATTEMPT = "attempt";
    static final String FAILED_ATTEMPT_JOB_IDS = "failedAttemptJobIds";

    protected static Logger logger = LoggerFactory.getLogger(VariantOperationJanitor.class);

    /**
     * Initialize VariantOperationJanitor with the catalog manager, configuration and token.
     *
     * @param catalogManager Instance of a working CatalogManager.
     * @param token          Valid administrator token.
     */
    public VariantOperationJanitor(CatalogManager catalogManager, String token) {
        super(30000, token, catalogManager);
        this.catalogManager = catalogManager;
        if (catalogManager.getConfiguration().getAnalysis().getOperations() == null) {
            this.operationConfig = new OperationConfig();
        } else {
            this.operationConfig = catalogManager.getConfiguration().getAnalysis().getOperations();
        }
        this.token = token;

        chores = Arrays.asList(
                new VariantAnnotationIndexOperationChore(operationConfig),
                new VariantSecondaryAnnotationIndexOperationChore(operationConfig),
                new VariantSecondarySampleIndexOperationChore(operationConfig));
    }

    @Override
    public void apply() {
        try {
            checkPendingVariantOperations();
        } catch (Exception e) {
            logger.error("Error checking pending variant operations", e);
        }
    }

    public void checkPendingVariantOperations() throws CatalogException, ToolException {
        for (OperationChore chore : chores) {
            try {
                checkPendingVariantOperations(chore);
            } catch (Exception e) {
                logger.error("Error checking variant operation chore '{}'", chore.getToolId(), e);
            }
        }
    }

    private void checkPendingVariantOperations(OperationChore operationChore)
            throws CatalogException, ToolException {
        OperationExecutionConfig config = operationChore.getConfig();
        String toolId = operationChore.getToolId();
        if (config.getPolicy() == null) {
            logger.warn("Policy for operation '{}' is not defined. Skipping.", toolId);
            return;
        }

        if (config.getPolicy() == OperationExecutionConfig.Policy.NEVER) {
            logger.info("Automatic operation chore '{}' is disabled. Nothing to do.", toolId);
            return;
        }

        if (!isNightTime() && config.getPolicy() == OperationExecutionConfig.Policy.NIGHTLY) {
            logger.info("Waiting until night time to check for pending operation '{}'", toolId);
            return;
        }

        if (!isWeekend() && config.getPolicy() == OperationExecutionConfig.Policy.WEEKLY) {
            logger.info("Waiting until weekend to check for pending operation '{}'", toolId);
            return;
        }

        List<String> organizationIds = catalogManager.getOrganizationManager().getOrganizationIds(token);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                OrganizationDBAdaptor.QueryParams.OWNER.key(), OrganizationDBAdaptor.QueryParams.PROJECTS.key()
        ));
        for (String organizationId : organizationIds) {
            Organization organization = catalogManager.getOrganizationManager().get(organizationId, options, token).first();
            String ownerToken = catalogManager.getUserManager().getToken(organizationId, organization.getOwner(), null, null, token);
            for (Project project : organization.getProjects()) {
                if (CollectionUtils.isEmpty(project.getStudies())) {
                    // Empty project. Skip
                    continue;
                }

                Tool tool = new ToolFactory().getTool(toolId);
                switch (tool.scope()) {
                    case PROJECT:
                        try {
                            checkPendingChore(operationChore, project, ownerToken);
                        } catch (Exception e) {
                            logger.error("Error checking pending chore '{}' in project '{}'. Ignore exception.",
                                    toolId, project.getFqn(), e);
                        }
                        break;
                    case STUDY:
                        for (Study study : project.getStudies()) {
                            try {
                                checkPendingChore(operationChore, project, study, ownerToken);
                            } catch (Exception e) {
                                logger.error("Error checking pending chore '{}' in study '{}'. Ignore exception.",
                                        toolId, study.getFqn(), e);
                            }
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected tools with scope " + tool.scope() + " for tool id '" + toolId + "'");
                }
            }
        }
    }

    private void checkPendingChore(OperationChore operationChore, Project project, String ownerToken) throws CatalogException {
        OperationExecutionConfig config = operationChore.getConfig();
        String toolId = operationChore.getToolId();
        List<String> studyFqns = project.getStudies().stream().map(Study::getFqn).collect(Collectors.toList());
        // 1. Check if operation is pending
        if (!operationChore.isOperationRequired(project, null)) {
            return;
        }

        // 2. Check if the operation is already created on any study of the project
        if (pendingJobs(studyFqns, toolId)) {
            logger.info("There's already a pending job for tool '{}' in project '{}'. Skipping.", toolId, project.getFqn());
            return;
        }

        // 3. Check general rules
        if (noPendingJobs(studyFqns, operationChore.dependantTools())) {
            // Get last execution of this job
            Job lastJobExecution = findLastJobExecution(studyFqns.get(0), toolId);
            ObjectMap attributes = getNewAttributes(lastJobExecution);
            if (attributes.getInt(ATTEMPT) > config.getMaxAttempts()) {
                logger.info("Max attempts reached for tool '{}' in project '{}'. Skipping.", toolId, project.getFqn());
                return;
            }

            Map<String, Object> paramsMap;
            if (config.getJobParams() == null) {
                paramsMap = new HashMap<>(config.getJobParams());
            } else {
                paramsMap = new HashMap<>();
            }
            paramsMap.put(ParamConstants.PROJECT_PARAM, project.getFqn());
            operationChore.addSpecificParams(paramsMap);
            catalogManager.getJobManager().submit(studyFqns.get(0), toolId, Enums.Priority.HIGH, paramsMap, null,
                    generateJobDescription(config, operationChore, attributes), null,
                    Collections.singletonList(TAG), null, null, null, attributes, ownerToken);
        }
    }

    private void checkPendingChore(OperationChore operationChore, Project project, Study study, String ownerToken) throws CatalogException {
        OperationExecutionConfig config = operationChore.getConfig();
        String toolId = operationChore.getToolId();
        // 1. Check if operation is pending
        if (!operationChore.isOperationRequired(project, study)) {
            return;
        }

        // 2. Check if the operation is already created
        if (pendingJobs(study.getFqn(), toolId)) {
            // Pending jobs of its own type. Skip study
            logger.info("There's already a pending job for tool '{}' in study '{}'. Skipping.", toolId, study.getFqn());
            return;
        }

        // 3. Check general rules
        if (noPendingJobs(study.getFqn(), operationChore.dependantTools())) {
            // Get last execution of this job
            Job lastJobExecution = findLastJobExecution(study.getFqn(), toolId);
            ObjectMap attributes = getNewAttributes(lastJobExecution);
            if (attributes.getInt(ATTEMPT) > config.getMaxAttempts()) {
                logger.info("Max attempts reached for tool '{}' in study '{}'. Skipping.", toolId, study.getFqn());
                return;
            }

            Map<String, Object> paramsMap;
            if (config.getJobParams() == null) {
                paramsMap = new HashMap<>(config.getJobParams());
            } else {
                paramsMap = new HashMap<>();
            }
            paramsMap.put(ParamConstants.STUDY_PARAM, study.getFqn());
            operationChore.addSpecificParams(paramsMap);
            catalogManager.getJobManager().submit(study.getFqn(), toolId, Enums.Priority.HIGH, paramsMap, null,
                    generateJobDescription(config, operationChore, attributes), null,
                    Collections.singletonList(TAG), null, null, null, attributes, ownerToken);
        }
    }

    private String generateJobDescription(OperationExecutionConfig config, OperationChore operationChore, ObjectMap attributes) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Job automatically launched by the variant operation janitor. ");
        stringBuilder.append("Tool: ").append(operationChore.getToolId()).append("; ");
        stringBuilder.append("Policy: ").append(config.getPolicy()).append("; ");
        stringBuilder.append("Attempt number: ").append(attributes.getInt(ATTEMPT)).append(" out of ").append(config.getMaxAttempts())
                .append("; ");
        String jobIds = StringUtils.join(attributes.getAsStringList(FAILED_ATTEMPT_JOB_IDS), ", ");
        stringBuilder.append("Job ids from previous attempts: ").append(jobIds).append(".");
        return stringBuilder.toString();
    }

    private interface OperationChore {

        String getToolId();

        boolean isOperationRequired(Project project, Study study);

        List<String> dependantTools();

        OperationExecutionConfig getConfig();

        default void addSpecificParams(Map<String, Object> params) {
        }

    }

    private static class VariantSecondarySampleIndexOperationChore implements OperationChore {

        private final OperationExecutionConfig operationConfig;

        VariantSecondarySampleIndexOperationChore(OperationConfig operationConfig) {
            this.operationConfig = operationConfig.getVariantSecondarySampleIndex();
        }

        @Override
        public String getToolId() {
            return VariantSecondarySampleIndexOperationTool.ID;
        }

        @Override
        public boolean isOperationRequired(Project project, Study study) {
            if (study.getInternal()
                    .getVariant()
                    .getSecondarySampleIndex()
                    .getStatus().getId().equals(OperationIndexStatus.PENDING)) {
                return true;
            }
//            try (DBIterator<Family> iterator = catalogManager.getFamilyManager().iterator(study.getFqn(), new Query(),
//                    new QueryOptions(QueryOptions.INCLUDE, FamilyDBAdaptor.QueryParams.MEMBERS), token)) {
//                while (iterator.hasNext()) {
//                    Family family = iterator.next();
//                    List<Trio> trios = variantStorageManager.getTriosFromFamily(study.getFqn(), family, true, token);
//                    for (Trio trio : trios) {
//                        String childSample = trio.getChild();
//                        Sample sample = catalogManager.getSampleManager().get(study.getFqn(), childSample,
//                                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INTERNAL_VARIANT), token).first();
//                        if (!sample.getInternal().getVariant().getSecondarySampleIndex().getFamilyStatus().getId()
//                                .equals(IndexStatus.READY)) {
//                            return true;
//                        }
//                    }
//                }
//            } catch (Exception e) {
//                logger.error("Error checking if secondary sample index is required", e);
//            }

            return false;
        }

        @Override
        public List<String> dependantTools() {
            return Collections.unmodifiableList(Arrays.asList(VariantIndexOperationTool.ID, VariantAnnotationIndexOperationTool.ID,
                    VariantSecondaryAnnotationIndexOperationTool.ID));
        }

        @Override
        public OperationExecutionConfig getConfig() {
            return operationConfig;
        }

        @Override
        public void addSpecificParams(Map<String, Object> params) {
            params.put("sample", ParamConstants.ALL);
            params.put("familyIndex", true);
        }
    }

    private static class VariantSecondaryAnnotationIndexOperationChore implements OperationChore {

        private final OperationExecutionConfig operationConfig;

        VariantSecondaryAnnotationIndexOperationChore(OperationConfig operationConfig) {
            this.operationConfig = operationConfig.getVariantSecondaryAnnotationIndex();
        }

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

        @Override
        public OperationExecutionConfig getConfig() {
            return operationConfig;
        }
    }

    private static class VariantAnnotationIndexOperationChore implements OperationChore {

        private final OperationExecutionConfig operationConfig;

        VariantAnnotationIndexOperationChore(OperationConfig operationConfig) {
            this.operationConfig = operationConfig.getVariantAnnotationIndex();
        }

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

        @Override
        public OperationExecutionConfig getConfig() {
            return operationConfig;
        }
    }

    private static boolean isNightTime() {
        boolean isNightTime = false;
        // Check date time is between 00:00 and 05:00
        //TODO: Define night time in configuration
        LocalTime now = LocalTime.now();
        int hour = now.getHour();
        if (hour < 5) {
            isNightTime = true;
        }
        return isNightTime;
    }

    private static boolean isWeekend() {
        LocalDate today = LocalDate.now();
        DayOfWeek dayOfWeek = today.getDayOfWeek();
        return dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY;
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

    Job findLastJobExecution(String studyId, String toolId) throws CatalogException {
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
        // If job was launched by the Janitor and failed, extract number of attempts
        if (job != null && (Enums.ExecutionStatus.ERROR.equals(job.getInternal().getStatus().getId())
                || Enums.ExecutionStatus.ABORTED.equals(job.getInternal().getStatus().getId()))
                && job.getTags().contains(TAG)) {
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
