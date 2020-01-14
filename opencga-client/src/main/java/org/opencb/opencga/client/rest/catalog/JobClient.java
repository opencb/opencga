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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;

/**
 * Created by imedina on 24/05/16.
 */
public class JobClient extends CatalogClient<Job> {

    private static final String JOBS_URL = "jobs";

    public JobClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = JOBS_URL;
        this.clazz = Job.class;
    }

    public RestResponse<Job> create(String studyId, ObjectMap bodyParams) throws IOException, ClientException {
        if (bodyParams == null || bodyParams.size() == 0) {
            throw new ClientException("Missing body parameters");
        }

        ObjectMap params = new ObjectMap("body", bodyParams);
        params.append("study", studyId);
        return execute(JOBS_URL, "create", params, POST, Job.class);
    }

    public RestResponse<Job> visit(String jobId, Query query) throws IOException {
        return execute(JOBS_URL, jobId, "visit", query, GET, Job.class);
    }
    public RestResponse<Job> groupBy(String studyId, String fields, ObjectMap params) throws IOException {
        params = addParamsToObjectMap(params, "study", studyId, "fields", fields);
        return execute(JOBS_URL, "groupBy", params, GET, Job.class);
    }

}
