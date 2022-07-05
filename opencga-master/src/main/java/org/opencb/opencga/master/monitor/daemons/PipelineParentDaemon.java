package org.opencb.opencga.master.monitor.daemons;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.Catalet;
import org.opencb.opencga.catalog.utils.CheckUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.job.Execution;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.Pipeline;
import org.opencb.opencga.core.tools.OpenCgaTool;
import org.opencb.opencga.core.tools.ToolFactory;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class PipelineParentDaemon extends MonitorParentDaemon {

    public static final Pattern EXECUTION_VARIABLE_PATTERN = Pattern.compile("(EXECUTION\\(([^)]+)\\))");
    public static final Pattern CATALET_VARIABLE_PATTERN =
            Pattern.compile("(FETCH_FIELD\\([\\w]+,[\\s]*[^\\s,]+,[\\s]*[^(),]+,[\\s]*[^\\s,()]+\\))");

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

    protected Execution fillDynamicExecutionParams(Execution execution) throws CatalogException {
        // Replace any EXECUTION(params) for the values
        try {
            String executionJsonString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(execution);
            ObjectMap executionMap = JacksonUtils.getDefaultObjectMapper().readValue(executionJsonString, ObjectMap.class);

            ParamUtils.processDynamicVariables(executionMap, EXECUTION_VARIABLE_PATTERN);

            executionJsonString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(executionMap);
            return JacksonUtils.getDefaultObjectMapper().readValue(executionJsonString, Execution.class);
        } catch (JsonProcessingException | ToolException e) {
            throw new CatalogException("Could not process dynamic execution variables from JSON properly: " + e.getMessage(), e);
        }
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
                String resolvedCondition = String.valueOf(resolveDynamicValue(condition, execution));
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


    protected Map<String, Object> filterJobParams(Map<String, Object> params, String toolId) throws ToolException {
        // Extract all the allowed params for the job
        Class<? extends OpenCgaTool> toolClass = new ToolFactory().getToolClass(toolId);
        Set<String> jobAllowedParams = new HashSet<>();
        jobAllowedParams.add("study");
        for (Field declaredField : toolClass.getDeclaredFields()) {
            if (declaredField.getDeclaredAnnotations().length > 0) {
                if (declaredField.getDeclaredAnnotations()[0].annotationType().getName().equals(ToolParams.class.getName())) {
                    for (Field field : declaredField.getType().getDeclaredFields()) {
                        jobAllowedParams.add(field.getName());
                    }
                }
            }
        }

        if (jobAllowedParams.size() == 1) {
            throw new ToolException("Could not find a ToolParams annotation for the tool '" + toolId + "'");
        }

        Map<String, Object> finalParams = new HashMap<>();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            // Remove all params that are not in the allowed params list that are not empty
            if (jobAllowedParams.contains(entry.getKey()) && ObjectUtils.isNotEmpty(entry.getValue())) {
                finalParams.put(entry.getKey(), entry.getValue());
            }
        }

        return finalParams;
    }

    private Object resolveDynamicValue(String value, Execution execution) throws CatalogException {
        if (EXECUTION_VARIABLE_PATTERN.matcher(value).find()) {
            try {
                String executionJsonString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(execution);
                ObjectMap executionMap = JacksonUtils.getDefaultObjectMapper().readValue(executionJsonString, ObjectMap.class);
                return ParamUtils.resolveDynamicPropertyValue(executionMap, value, EXECUTION_VARIABLE_PATTERN);
            } catch (JsonProcessingException | ToolException e) {
                throw new CatalogException("Could not process dynamic variables from JSON properly: " + e.getMessage(), e);
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
        } catch (JsonProcessingException | ToolException e) {
            throw new CatalogException("Could not process dynamic variables from JSON properly: " + e.getMessage(), e);
        }
    }

    private void fetchFromCatalogData(String studyId, Map<String, Object> params, String userToken) throws CatalogException {
        Catalet catalet = null;

        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (entry.getValue() instanceof String) {
                Object value = entry.getValue();
                Matcher matcher = CATALET_VARIABLE_PATTERN.matcher(String.valueOf(value));
                while (matcher.find()) {
                    if (catalet == null) {
                        catalet = new Catalet(catalogManager, studyId, userToken);
                    }

                    // Fetch data from catalog
                    Object fetch = catalet.fetch(matcher.group(1));

                    if (matcher.group(1).equals(value)) {
                        // If the group matching is the entire value, we can simply assign the new value to the variable
                        value = fetch;
                    } else {
                        // If the group matching is just part of the string, we will replace the matching group
                        value = String.valueOf(value).replace(matcher.group(1), String.valueOf(fetch));
                    }

                    matcher = CATALET_VARIABLE_PATTERN.matcher(String.valueOf(value));
                }

                entry.setValue(value);
            }
        }
    }

}
