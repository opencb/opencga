package org.opencb.opencga.master.monitor.daemons;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
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
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class VariantOperationManager {

    private final CatalogManager catalogManager;
    private final OperationConfig configuration;
    private final String token;

    protected static Logger logger = LoggerFactory.getLogger(VariantOperationManager.class);

    /**
     * Initialize VariantOperationManager with the catalog manager, configuration and token.
     *
     * @param catalogManager Instance of a working CatalogManager.
     * @param configuration  Main configuration file.
     * @param token          Valid administrator token.
     */
    public VariantOperationManager(CatalogManager catalogManager, Configuration configuration, String token) {
        this.catalogManager = catalogManager;
        this.configuration = configuration.getAnalysis().getOperations();
        this.token = token;
    }

    public void checkPendingVariantOperations() {
        if (configuration.getVariantSecondaryAnnotationIndex().getPolicy() == OperationExecutionConfig.Policy.NEVER
                && configuration.getVariantSecondarySampleIndex().getPolicy() == OperationExecutionConfig.Policy.NEVER) {
            // Nothing to do
            return;
        }

        boolean isNightTime = false;
        Calendar calendar = Calendar.getInstance();
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        // Check date time is between 00:00 and 05:00
        if (hour < 5) {
            isNightTime = true;
        }
        if (!isNightTime && configuration.getVariantSecondaryAnnotationIndex().getPolicy() == OperationExecutionConfig.Policy.NIGHTLY
                && configuration.getVariantSecondarySampleIndex().getPolicy() == OperationExecutionConfig.Policy.NIGHTLY) {
            // Waiting until night time
            return;
        }

        List<String> organizationIds = catalogManager.getOrganizationManager().getOrganizationIds(token);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, OrganizationDBAdaptor.QueryParams.PROJECTS.key());
        for (String organizationId : organizationIds) {
            Organization organization = catalogManager.getOrganizationManager().get(organizationId, options, token).first();
            for (Project project : organization.getProjects()) {
                if (project.getInternal().getStatus() != null) { // TODO: To be changed for the variant status
                    if (CollectionUtils.isNotEmpty(project.getStudies())) {
                        // Check project tools
                        List<String> studyFqns = project.getStudies().stream().map(Study::getFqn).collect(Collectors.toList());
                        if (isNightTime || configuration.getVariantSecondaryAnnotationIndex().getPolicy() == OperationExecutionConfig.Policy.IMMEDIATE) {
                            if (jobDoesNotExist(studyFqns, VariantSecondaryAnnotationIndexOperationTool.ID)) {
                                Map<String, Object> paramsMap = new HashMap<>();
                                paramsMap.put(ParamConstants.PROJECT_PARAM, project.getFqn());
                                catalogManager.getJobManager().submit(studyFqns.get(0), VariantSecondaryAnnotationIndexOperationTool.ID,
                                        Enums.Priority.MEDIUM, paramsMap, token);
                            } else {
                                logger.debug("There's already a job planned for the tool '{}'", VariantSecondaryAnnotationIndexOperationTool.ID);
                            }
                        } else {
                            logger.warn("Job policy '{}' not satisfied", configuration.getVariantSecondaryAnnotationIndex().getPolicy());
                        }

                        // Check study tools
                        for (Study study : project.getStudies()) {
                            if (study.getInternal().getStatus() != null) { // TODO: To be changed for the variant status
                                if (isNightTime || configuration.getVariantSecondarySampleIndex().getPolicy() == OperationExecutionConfig.Policy.IMMEDIATE) {
                                    if (jobDoesNotExist(Collections.singletonList(study.getFqn()), VariantSecondarySampleIndexOperationTool.ID)) {
                                        Map<String, Object> paramsMap = new HashMap<>();
                                        catalogManager.getJobManager().submit(study.getFqn(), VariantSecondarySampleIndexOperationTool.ID,
                                                Enums.Priority.MEDIUM, paramsMap, token);
                                    } else {
                                        logger.debug("There's already a job planned for the tool '{}'", VariantSecondarySampleIndexOperationTool.ID);
                                    }
                                } else {
                                    logger.warn("Job policy '{}' not satisfied", configuration.getVariantSecondaryAnnotationIndex().getPolicy());
                                }
                            }
                        }
                    } else {
                        logger.warn("Could not submit job for project '{}'. No studies found", project.getFqn());
                    }
                }
            }
        }
    }

    private boolean jobDoesNotExist(List<String> studyIds, String toolId) throws CatalogException {
        String studyId = studyIds.get(0);
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.INTERNAL_STATUS_ID.key(), Arrays.asList(Enums.ExecutionStatus.PENDING,
                        Enums.ExecutionStatus.QUEUED, Enums.ExecutionStatus.RUNNING))
                .append(JobDBAdaptor.QueryParams.TOOL_ID.key(), toolId);

        long numMatches = catalogManager.getJobManager().count(studyId, query, token).getNumMatches();
        logger.debug("Number of {}, {} or {} jobs from tool '{}' is {}", Enums.ExecutionStatus.PENDING,
                Enums.ExecutionStatus.QUEUED, Enums.ExecutionStatus.RUNNING, toolId, numMatches);
        return numMatches == 0;
    }

}
