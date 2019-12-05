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
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.results.VariantQueryResult;

import java.io.IOException;
import java.util.List;

/**
 * Created by pfurio on 23/11/16.
 */
public class VariantClient extends AbstractParentClient {

    private static final String VARIANT_URL = "analysis/variant";

    public static class DataResponseMixing<T> {
        @JsonDeserialize(contentAs = VariantQueryResult.class)
        private List<DataResult<T>> response;
    }

    public VariantClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
        jsonObjectMapper.addMixIn(DataResponse.class, DataResponseMixing.class);
    }

    public DataResponse<Job> index(String study, ObjectMap params) throws IOException {
        return execute(VARIANT_URL, "index", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<VariantMetadata> metadata(ObjectMap params, QueryOptions options) throws IOException {
        if (options != null) {
            params = new ObjectMap(params);
            params.putAll(options);
        }
        return execute(VARIANT_URL, "metadata", params, GET, VariantMetadata.class);
    }

    public DataResponse<Variant> query(ObjectMap params, QueryOptions options) throws IOException {
        if (options != null) {
            params = new ObjectMap(params);
            params.putAll(options);
        }
        return execute(VARIANT_URL, "query", params, GET, Variant.class);
    }

    public DataResponse<Job> export(String study, Query query, QueryOptions options, String outdir, String outputFileName,
                                    String outputFormat, boolean compress)
            throws IOException {
        ObjectMap params = query == null ? new ObjectMap() : new ObjectMap(query);
        params.putAll(options);
        params.put("outdir", outdir);
        params.put("outputFileName", outputFileName);
        params.put("outputFormat", outputFormat);
        params.put("compress", compress);
        return execute(VARIANT_URL, "export", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<VariantAnnotation> annotationQuery(String annotationId, ObjectMap params, QueryOptions options)
            throws IOException {
        if (options != null) {
            params = new ObjectMap(params);
            params.putAll(options);
        }
        params.put("annotationId", annotationId);
        return execute(VARIANT_URL, "annotation/query", params, GET, VariantAnnotation.class);
    }

    public DataResponse<ObjectMap> annotationMetadata(String annotationId, String project, QueryOptions options)
            throws IOException {
        if (options != null) {
            options = new QueryOptions(options);
        } else {
            options = new QueryOptions();
        }
        options.put("annotationId", annotationId);
        options.put("project", project);
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

    public DataResponse<Long> count(ObjectMap params, QueryOptions options) throws IOException {
        if (options != null) {
            params.putAll(options);
        }
        return execute(VARIANT_URL, "query", params, GET, Long.class);
    }

    public DataResponse<ObjectMap> genericQuery(ObjectMap params, QueryOptions options) throws IOException {
        if (options != null) {
            params.putAll(options);
        }
        return execute(VARIANT_URL, "query", params, GET, ObjectMap.class);
    }

    public DataResponse<Job> statsRun(String study, ObjectMap params) throws IOException {
        return execute(VARIANT_URL, "/stats/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<Job> sampleStatsRun(String study, ObjectMap params) throws IOException {
        return execute(VARIANT_URL, "/sample/stats/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<SampleVariantStats> sampleStatsInfo(String study, List<String> sample) throws IOException {
        ObjectMap params = new ObjectMap("study", study).append("sample", String.join(",", sample));
        return execute(VARIANT_URL, "/sample/stats/info", params, GET, SampleVariantStats.class);
    }

    public DataResponse<Job> cohortStatsRun(String study, ObjectMap params) throws IOException {
        return execute(VARIANT_URL, "/cohort/stats/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<VariantSetStats> cohortStatsInfo(String study, List<String> cohort) throws IOException {
        ObjectMap params = new ObjectMap("study", study).append("cohort", String.join(",", cohort));
        return execute(VARIANT_URL, "/cohort/stats/info", params, GET, VariantSetStats.class);
    }

    public DataResponse<Job> gwasRun(String study, ObjectMap params) throws IOException {
        return execute(VARIANT_URL, "/gwas/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

//    public DataResponse<Job> hwRun(ObjectMap params) throws IOException {
//        return execute(VARIANT_URL, "/hw/run", new ObjectMap("body", params), POST, Job.class);
//    }

//    public DataResponse<Job> ibsRun(ObjectMap params) throws IOException {
//        return execute(VARIANT_URL, "/ibs/run", new ObjectMap("body", params), POST, Job.class);
//    }


    // Wrappers

    public DataResponse<Job> plinkRun(String study, ObjectMap params) throws IOException {
        return execute(VARIANT_URL, "/plink/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<Job> rvtestsRun(String study, ObjectMap params) throws IOException {
        return execute(VARIANT_URL, "/rvtests/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }
}
