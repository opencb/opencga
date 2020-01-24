/*
* Copyright 2015-2020 OpenCB
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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobAclUpdateParams;
import org.opencb.opencga.core.models.job.JobCreateParams;
import org.opencb.opencga.core.models.job.JobUpdateParams;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Job webservices.
 *    Client version: 2.0.0
 *    PATH: jobs
 */
public class JobClient extends AbstractParentClient {

    public JobClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Delete existing jobs.
     * @param jobs Comma separated list of job ids.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> delete(String jobs, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("jobs", jobs, null, null, "delete", params, DELETE, Job.class);
    }

    /**
     * Job search method.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("jobs", null, null, null, "search", params, GET, Job.class);
    }

    /**
     * Get job information.
     * @param jobs Comma separated list of job IDs or UUIDs up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> info(String jobs, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("jobs", jobs, null, null, "info", params, GET, Job.class);
    }

    /**
     * Update some job attributes.
     * @param jobs Comma separated list of job IDs or UUIDs up to a maximum of 100.
     * @param data params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> update(String jobs, JobUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("jobs", jobs, null, null, "update", params, POST, Job.class);
    }

    /**
     * Return the acl of the job. If member is provided, it will only return the acl for the member.
     * @param jobs Comma separated list of job IDs or UUIDs up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> acl(String jobs, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("jobs", jobs, null, null, "acl", params, GET, ObjectMap.class);
    }

    /**
     * Update the set of permissions granted for the member.
     * @param members Comma separated list of user or group ids.
     * @param data JSON containing the parameters to add ACLs.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> updateAcl(String members, JobAclUpdateParams data) throws ClientException {
        ObjectMap params = new ObjectMap();
        params.put("body", data);
        return execute("jobs", members, null, null, "update", params, POST, ObjectMap.class);
    }

    /**
     * Register an executed job with POST method.
     * @param data job.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> create(JobCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("jobs", null, null, null, "create", params, POST, Job.class);
    }
}
