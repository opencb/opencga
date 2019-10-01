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

package org.opencb.opencga.client.rest.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;

import java.io.IOException;

/**
 * Created by imedina on 24/05/16.
 */
public class SampleClient extends AnnotationClient<Sample, SampleAclEntry> {

    private static final String SAMPLES_URL = "samples";

    public SampleClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = SAMPLES_URL;
        this.clazz = Sample.class;
        this.aclClass = SampleAclEntry.class;
    }

    public DataResponse<Sample> create(String studyId, String sampleId, ObjectMap bodyParams) throws IOException {
        bodyParams.put("id", sampleId);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("body", bodyParams);
        params.putIfNotNull(STUDY, studyId);

        return execute(SAMPLES_URL, "create", params, POST, Sample.class);
    }

    public DataResponse<Sample> update(String study, String id, String annotationSetAction, ObjectMap params) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(params);
        ObjectMap p = new ObjectMap("body", json);
        p.putIfNotEmpty("study", study);
        p.putIfNotEmpty("annotationSetsAction", annotationSetAction);
        logger.debug("Json in update client: " + json);
        return execute(SAMPLES_URL, id, "update", p, POST, Sample.class);
    }

    public DataResponse<Sample> loadFromPed(String studyId, ObjectMap params) throws IOException {
        params = addParamsToObjectMap(params, "study", studyId);
        return execute(SAMPLES_URL, "load", params, GET, Sample.class);
    }

    public DataResponse<ObjectMap> groupBy(String studyId, String fields, ObjectMap params) throws IOException {
        params = addParamsToObjectMap(params, "study", studyId, "fields", fields);
        return execute(SAMPLES_URL, "groupBy", params, GET, ObjectMap.class);
    }

    public DataResponse<FacetQueryResult> stats(String study, Query query, QueryOptions queryOptions) throws IOException {
        ObjectMap params = new ObjectMap(query);
        params.putAll(queryOptions);
        params.put("study", study);
        return execute(SAMPLES_URL, "stats", params, GET, FacetQueryResult.class);
    }

}
