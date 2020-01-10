package org.opencb.opencga.client.rest.operations;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.api.operations.variant.*;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.core.tools.ToolParams;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperationClient extends AbstractParentClient {

    public OperationClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
    }

    private static final String OPERATION_URL = "operation";

    public RestResponse<Job> variantSecondaryIndex(String project, String study, VariantSecondaryIndexParams indexParams,
                                                   Map<String, ?> params)
            throws IOException {
        return execute(OPERATION_URL, "/variant/secondaryIndex",
                buildRestPOSTParams(project, study, indexParams, params), POST, Job.class);
    }

    public RestResponse<Job> variantSecondaryIndexDelete(String study, List<String> sample, Map<String, ?> params) throws IOException {
        return execute(OPERATION_URL, "/variant/secondaryIndex/delete",
                copy(params)
                .append(ParamConstants.STUDY_PARAM, study)
                .append("sample", sample), DELETE, Job.class);
    }

    public RestResponse<Job> variantAnnotationIndex(String project, String study, VariantAnnotationIndexParams body, Map<String, ?> params)
            throws IOException {
        return execute(OPERATION_URL, "/variant/annotation/index", buildRestPOSTParams(project, study, body, params), POST, Job.class);
    }

    public RestResponse<Job> variantAnnotationDelete(String project, VariantAnnotationDeleteParams deleteParams,  Map<String, ?> params)
            throws IOException {
        return execute(OPERATION_URL, "/variant/annotation/delete",
                buildRestDELETEParams(project, null, deleteParams, params), DELETE, Job.class);
    }

    public RestResponse<Job> variantAnnotationSave(String project, VariantAnnotationSaveParams body, Map<String, ?> params)
            throws IOException {
        return execute(OPERATION_URL, "/variant/annotation/save", buildRestPOSTParams(project, null, body, params), POST, Job.class);
    }

    public RestResponse<Job> variantScoreIndex(String study, VariantScoreIndexParams body, Map<String, ?> params) throws IOException {
        return execute(OPERATION_URL, "/variant/score/index", buildRestPOSTParams(null, study, body, params), POST, Job.class);
    }

    public RestResponse<Job> variantScoreDelete(String study, VariantScoreDeleteParams deleteParams, Map<String, ?> params)
            throws IOException {
        return execute(OPERATION_URL, "/variant/score/delete", buildRestDELETEParams(null, study, deleteParams, params), DELETE, Job.class);
    }

    public RestResponse<Job> variantSampleGenotypeIndex(String study, VariantSampleIndexParams body, Map<String, ?> params)
            throws IOException {
        return execute(OPERATION_URL, "/variant/sample/genotype/index", buildRestPOSTParams(null, study, body, params), POST, Job.class);
    }

    public RestResponse<Job> variantFamilyGenotypeIndex(String study, VariantFamilyIndexParams body, Map<String, ?> params)
            throws IOException {
        return execute(OPERATION_URL, "/variant/family/genotype/index", buildRestPOSTParams(null, study, body, params), POST, Job.class);
    }

    public RestResponse<Job> variantAggregateFamily(String study, VariantAggregateFamilyParams body, Map<String, ?> params)
            throws IOException {
        return execute(OPERATION_URL, "/variant/family/aggregate", buildRestPOSTParams(null, study, body, params), POST, Job.class);
    }

    public RestResponse<Job> variantAggregate(String study, VariantAggregateParams body, Map<String, ?> params) throws IOException {
        return execute(OPERATION_URL, "/variant/aggregate", buildRestPOSTParams(null, study, body, params), POST, Job.class);
    }

    private ObjectMap copy(Map<String, ?> params) {
        if (params == null) {
            return new ObjectMap();
        } else {
            return new ObjectMap(params);
        }
    }

    private ObjectMap buildRestDELETEParams(String project, String study, ToolParams deleteParams, Map<String, ?> params) {
        ObjectMap restParams = deleteParams.toObjectMap();
        restParams.putAll(toDynamicParams(params));
        restParams.putIfNotEmpty(ParamConstants.PROJECT_PARAM, project);
        restParams.putIfNotEmpty(ParamConstants.STUDY_PARAM, study);
        return restParams;
    }

    private ObjectMap buildRestPOSTParams(String project, String study, ToolParams body, Map<String, ?> params) {
        ObjectMap restParams = new ObjectMap("body", body.toObjectMap());
        restParams.putAll(toDynamicParams(params));
        restParams.putIfNotEmpty(ParamConstants.PROJECT_PARAM, project);
        restParams.putIfNotEmpty(ParamConstants.STUDY_PARAM, study);
        return restParams;
    }

    private Map<String, String> toDynamicParams(Map<String, ?> params) {
        ObjectMap objectMap = new ObjectMap(params);
        Map<String, String> dynamicParams = new HashMap<>();
        for (String key : params.keySet()) {
            dynamicParams.put("dynamic_" + key, objectMap.getString(key));
        }
        return dynamicParams;
    }
}
