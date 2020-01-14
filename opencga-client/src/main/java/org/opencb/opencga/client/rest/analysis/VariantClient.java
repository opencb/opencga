/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.client.rest.analysis;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.api.operations.variant.VariantFileDeleteParams;
import org.opencb.opencga.core.api.variant.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.core.tools.ToolParams;

import java.io.IOException;
import java.util.List;

/**
 * Created by pfurio on 23/11/16.
 */
public class VariantClient extends AbstractParentClient {

    private static final String VARIANT_URL = "analysis/variant";

    public static class RestResponseMixing<T> {
        @JsonDeserialize(contentAs = VariantQueryResult.class)
        private List<DataResult<T>> response;
    }

    public VariantClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
        jsonObjectMapper.addMixIn(RestResponse.class, RestResponseMixing.class);
    }

    public RestResponse<Job> index(String study, VariantIndexParams body) throws IOException {
        return execute(VARIANT_URL, "index", buildRestPOSTParams(null, study, body), POST, Job.class);
    }

    public RestResponse<Job> fileDelete(String study, VariantFileDeleteParams deleteParams)
            throws IOException {
        return execute(VARIANT_URL, "file/delete", buildRestDELETEParams(null, study, deleteParams), DELETE, Job.class);
    }

    public RestResponse<VariantMetadata> metadata(ObjectMap params, QueryOptions options) throws IOException {
        if (options != null) {
            params = new ObjectMap(params);
            params.putAll(options);
        }
        return execute(VARIANT_URL, "metadata", params, GET, VariantMetadata.class);
    }

    public RestResponse<Variant> query(ObjectMap params, QueryOptions options) throws IOException {
        if (options != null) {
            params = new ObjectMap(params);
            params.putAll(options);
        }
        return execute(VARIANT_URL, "query", params, GET, Variant.class);
    }

    public RestResponse<Job> export(String study, Query query, QueryOptions options, String outdir, String outputFileName,
                                    String outputFormat, boolean compress)
            throws IOException {
        ObjectMap params = query == null ? new ObjectMap() : new ObjectMap(query);
        params.putAll(options);
        params.put("outdir", outdir);
        params.put("outputFileName", outputFileName);
        params.put("outputFormat", outputFormat);
        params.put("compress", compress);
        return execute(VARIANT_URL, "export", new ObjectMap("body", params).append(ParamConstants.STUDY_PARAM, study), POST, Job.class);
    }

    public RestResponse<VariantAnnotation> annotationQuery(String annotationId, ObjectMap params, QueryOptions options)
            throws IOException {
        if (options != null) {
            params = new ObjectMap(params);
            params.putAll(options);
        }
        params.put("annotationId", annotationId);
        return execute(VARIANT_URL, "annotation/query", params, GET, VariantAnnotation.class);
    }

    public RestResponse<ObjectMap> annotationMetadata(String annotationId, String project, QueryOptions options)
            throws IOException {
        if (options != null) {
            options = new QueryOptions(options);
        } else {
            options = new QueryOptions();
        }
        options.put("annotationId", annotationId);
        options.put(ParamConstants.PROJECT_PARAM, project);
        return execute(VARIANT_URL, "annotation/metadata", options, GET, ObjectMap.class);
    }

    public VariantQueryResult<Variant> query2(ObjectMap params, QueryOptions options) throws IOException {
        if (options != null) {
            params = new ObjectMap(params);
            params.putAll(options);
        }
        return executeVariantQuery(VARIANT_URL, "query", params, GET, Variant.class);
    }

//    public VariantQueryResult<Variant> queryResult(ObjectMap params, QueryOptions options) throws CatalogException, IOException {
//        return ((VariantQueryResult<Variant>) query(params, options).getResponse().get(0));
//    }

    public RestResponse<Long> count(ObjectMap params, QueryOptions options) throws IOException {
        if (options != null) {
            params.putAll(options);
        }
        return execute(VARIANT_URL, "query", params, GET, Long.class);
    }

    public RestResponse<ObjectMap> genericQuery(ObjectMap params, QueryOptions options) throws IOException {
        if (options != null) {
            params.putAll(options);
        }
        return execute(VARIANT_URL, "query", params, GET, ObjectMap.class);
    }

    public RestResponse<Job> statsRun(String study, VariantStatsAnalysisParams body) throws IOException {
        return execute(VARIANT_URL, "/stats/run", buildRestPOSTParams(null, study, body), POST, Job.class);
    }

    public RestResponse<Job> sampleRun(String study, SampleVariantFilterParams body) throws IOException {
        return execute(VARIANT_URL, "/sample/run", buildRestPOSTParams(null, study, body), POST, Job.class);
    }

    public RestResponse<Variant> sampleQuery(String variant, String study, List<String> genotype, int limit, int skip, QueryOptions options)
            throws IOException {
        if (options == null) {
            options = new QueryOptions();
        } else {
            options = new QueryOptions(options);
        }
        options.append(ParamConstants.STUDY_PARAM, study)
                .append("variant", variant)
                .append("genotype", genotype == null ? null : String.join(",", genotype))
                .append(QueryOptions.LIMIT, limit)
                .append(QueryOptions.SKIP, skip);
        return execute(VARIANT_URL, "/sample/query", options, GET, Variant.class);
    }

    public RestResponse<Job> sampleStatsRun(String study, SampleVariantStatsAnalysisParams body) throws IOException {
        return execute(VARIANT_URL, "/sample/stats/run", buildRestPOSTParams(null, study, body), POST, Job.class);
    }

    public RestResponse<SampleVariantStats> sampleStatsInfo(String study, List<String> sample) throws IOException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study)
                .append("sample", String.join(",", sample));
        return execute(VARIANT_URL, "/sample/stats/info", params, GET, SampleVariantStats.class);
    }

    public RestResponse<Job> cohortStatsRun(String study, CohortVariantStatsAnalysisParams body) throws IOException {
        return execute(VARIANT_URL, "/cohort/stats/run", buildRestPOSTParams(null, study, body), POST, Job.class);
    }

    public RestResponse<VariantSetStats> cohortStatsInfo(String study, List<String> cohort) throws IOException {
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, study)
                .append("cohort", String.join(",", cohort));
        return execute(VARIANT_URL, "/cohort/stats/info", params, GET, VariantSetStats.class);
    }

    public RestResponse<Job> gwasRun(String study, GwasAnalysisParams body) throws IOException {
        return execute(VARIANT_URL, "/gwas/run", buildRestPOSTParams(null, study, body), POST, Job.class);
    }

//    public RestResponse<Job> hwRun(ObjectMap params) throws IOException {
//        return execute(VARIANT_URL, "/hw/run", new ObjectMap("body", params), POST, Job.class);
//    }

//    public RestResponse<Job> ibsRun(ObjectMap params) throws IOException {
//        return execute(VARIANT_URL, "/ibs/run", new ObjectMap("body", params), POST, Job.class);
//    }


    // Wrappers

    public RestResponse<Job> plinkRun(String study, PlinkRunParams body) throws IOException {
        return execute(VARIANT_URL, "/plink/run", buildRestPOSTParams(null, study, body), POST, Job.class);
    }

    public RestResponse<Job> rvtestsRun(String study, RvtestsRunParams body) throws IOException {
        return execute(VARIANT_URL, "/rvtests/run", buildRestPOSTParams(null, study, body), POST, Job.class);
    }

    private ObjectMap buildRestDELETEParams(String project, String study, ToolParams deleteParams) {
        ObjectMap restParams = deleteParams.toObjectMap();
        restParams.putIfNotEmpty(ParamConstants.PROJECT_PARAM, project);
        restParams.putIfNotEmpty(ParamConstants.STUDY_PARAM, study);
        return restParams;
    }

    private ObjectMap buildRestPOSTParams(String project, String study, ToolParams body) {
        ObjectMap restParams = new ObjectMap("body", body.toObjectMap());
        restParams.putIfNotEmpty(ParamConstants.PROJECT_PARAM, project);
        restParams.putIfNotEmpty(ParamConstants.STUDY_PARAM, study);
        return restParams;
    }

}
