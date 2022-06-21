package org.opencb.opencga.master.monitor.daemons;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.Catalet;
import org.opencb.opencga.catalog.utils.CheckUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.Pipeline;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class PipelineParentDaemon extends MonitorParentDaemon {

    public static final Pattern EXECUTION_VARIABLE_PATTERN = Pattern.compile("(EXECUTION\\(([^)]+)\\))");
    public static final Pattern CATALET_VARIABLE_PATTERN = Pattern.compile("(FETCH_FIELD\\(([^)]+)\\))");

    public PipelineParentDaemon(int interval, String token, CatalogManager catalogManager) throws CatalogDBException {
        super(interval, token, catalogManager);
    }

    protected String getPipelineJobId(String key, Pipeline.PipelineJob pipelineJob) {
        if (StringUtils.isNotEmpty(pipelineJob.getToolId())) {
            return pipelineJob.getToolId();
        }
        if (pipelineJob.getExecutable() != null && StringUtils.isNotEmpty(pipelineJob.getExecutable().getId())) {
            return pipelineJob.getExecutable().getId();
        }
        return key;
    }

    protected Job fillDynamicJobInformation(Execution execution, Job job, String userToken) throws CatalogException {
        // Replace any EXECUTION(params) for the values
        Job jobResult = fillDynamicJobValues(execution, job);

        // Automatically fill in job params values fetching data from catalog (FETCH_FIELD)
        fetchFromCatalogData(execution.getStudy().getId(), jobResult.getParams(), userToken);

        return jobResult;
    }

    protected boolean checkJobCondition(Execution execution, Pipeline.PipelineJob pipelineJob) throws CatalogException {
        Pipeline.PipelineJobCondition jobCondition = pipelineJob.getWhen();
        if (pipelineJob.getWhen() != null) {
            if (CollectionUtils.isEmpty(jobCondition.getChecks())) {
                return true;
            }

            // By default, comparator is going to be an AND if undefined
            Pipeline.Comparator comparator = jobCondition.getComparator() != null ? jobCondition.getComparator() : Pipeline.Comparator.AND;

            boolean satisfies = comparator == Pipeline.Comparator.AND; // true if AND, false if OR
            for (String condition : jobCondition.getChecks()) {
                String resolvedCondition = resolveDynamicValue(condition, execution);
                boolean check = CheckUtils.check(resolvedCondition);
                if (comparator == Pipeline.Comparator.AND) {
                    satisfies = satisfies && check;
                } else {
                    satisfies = satisfies || check;
                }
            }
            return satisfies;
        }
        return true;
    }

    private String resolveDynamicValue(String value, Execution execution) throws CatalogException {
        if (EXECUTION_VARIABLE_PATTERN.matcher(value).find()) {
            try {
                String executionJsonString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(execution);
                ObjectMap executionMap = JacksonUtils.getDefaultObjectMapper().readValue(executionJsonString, ObjectMap.class);
                return ParamUtils.resolveDynamicPropertyValue(executionMap, value, EXECUTION_VARIABLE_PATTERN);
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not process dynamic variables from JSON properly", e);
            }
        } else {
            return value;
        }
    }

    private Job fillDynamicJobValues(Execution execution, Job job) throws CatalogException {
        try {
            String jobJsonString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(job);
            ObjectMap jobMap = JacksonUtils.getDefaultObjectMapper().readValue(jobJsonString, ObjectMap.class);

            String executionJsonString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(execution);
            ObjectMap executionMap = JacksonUtils.getDefaultObjectMapper().readValue(executionJsonString, ObjectMap.class);

            ParamUtils.processDynamicVariables(executionMap, jobMap, EXECUTION_VARIABLE_PATTERN);

            jobJsonString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(jobMap);
            return JacksonUtils.getDefaultObjectMapper().readValue(jobJsonString, Job.class);
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not process dynamic variables from JSON properly", e);
        }
    }

    private void fetchFromCatalogData(String studyId, Map<String, Object> params, String userToken) throws CatalogException {
        Catalet catalet = null;

        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (entry.getValue() instanceof String) {
                Matcher matcher = CATALET_VARIABLE_PATTERN.matcher(String.valueOf(entry.getValue()));
                if (matcher.find()) {
                    if (catalet == null) {
                        catalet = new Catalet(catalogManager, studyId, userToken);
                    }

                    // Fetch data from catalog
                    Object fetch = catalet.fetch(matcher.group(1));

                    // Set new content
                    entry.setValue(fetch);
                }
            }
        }
    }

}
