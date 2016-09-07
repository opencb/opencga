/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.client.rest;

import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.acls.permissions.JobAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by imedina on 24/05/16.
 */
public class JobClient extends AbstractParentClient<Job, JobAclEntry> {

    private static final String JOBS_URL = "jobs";

    protected JobClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = JOBS_URL;
        this.clazz = Job.class;
        this.aclClass = JobAclEntry.class;
    }

    public QueryResponse<Job> create(String studyId, String jobName, String toolId, ObjectMap params) throws CatalogException, IOException {

        if (params.containsKey("method") && params.get("method").equals("GET")) {
            params = addParamsToObjectMap(params, "studyId", studyId, "name", jobName, "toolId", toolId);
            return execute(JOBS_URL, "create", params, GET, Job.class);
        }
        params = addParamsToObjectMap(params, "name", jobName, "toolId", toolId);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(params);
        ObjectMap p = new ObjectMap("body", json);
        p.append("studyId", studyId);
        return execute(JOBS_URL, "create", p, POST, Job.class);
    }

    public QueryResponse<Job> visit(String jobId, QueryOptions options) throws CatalogException, IOException {
        return execute(JOBS_URL, jobId, "visit", options, GET, Job.class);
    }
    public QueryResponse<Job> groupBy(String studyId, String fields, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "studyId", studyId, "fields", fields);
        return execute(JOBS_URL, "groupBy", params, GET, Job.class);
    }

}
