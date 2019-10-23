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

import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.models.Individual;

import java.io.IOException;

/**
 * Created by imedina on 24/05/16.
 */
public class IndividualClient extends AnnotationClient<Individual> {

    private static final String INDIVIDUALS_URL = "individuals";

    public IndividualClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = INDIVIDUALS_URL;
        this.clazz = Individual.class;
    }

    public DataResponse<Individual> create(String studyId, ObjectMap bodyParams) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(STUDY, studyId);
        params.putIfNotNull("body", bodyParams);
        return execute(INDIVIDUALS_URL, "create", params, POST, Individual.class);
    }

    public DataResponse<ObjectMap> groupBy(String studyId, String fields, ObjectMap params) throws IOException {
        params = addParamsToObjectMap(params, "study", studyId, "fields", fields);
        return execute(INDIVIDUALS_URL, "groupBy", params, GET, ObjectMap.class);
    }

    public DataResponse<DataResult> stats(String study, Query query, QueryOptions queryOptions) throws IOException {
        ObjectMap params = new ObjectMap(query);
        params.putAll(queryOptions);
        params.put("study", study);
        return execute(INDIVIDUALS_URL, "stats", params, GET, DataResult.class);
    }
}
