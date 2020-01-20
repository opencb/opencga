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

import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Project webservices.
 *    Client version: 2.0.0
 *    PATH: projects
 */
public class ProjectClient extends AbstractParentClient {

    public ProjectClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Fetch project information.
     * @param projects Comma separated list of projects [user@]project up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Project> info(String projects, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("projects", projects, null, null, "info", params, GET, Project.class);
    }

    /**
     * Update some project attributes.
     * @param project Project [user@]project where project can be either the ID or the alias.
     * @param data JSON containing the params to be updated. It will be only possible to update organism fields not previously defined.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Project> update(String project, ProjectUpdateParams data) throws ClientException {
        ObjectMap params = new ObjectMap();
        params.put("body", data);
        return execute("projects", project, null, null, "update", params, POST, Project.class);
    }

    /**
     * Fetch catalog project stats.
     * @param projects Comma separated list of projects [user@]project up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<FacetField> aggregationStats(String projects, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("projects", projects, null, null, "aggregationStats", params, GET, FacetField.class);
    }

    /**
     * Search projects.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Project> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("projects", null, null, null, "search", params, GET, Project.class);
    }

    /**
     * Create a new project.
     * @param data JSON containing the mandatory parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Project> create(ProjectCreateParams data) throws ClientException {
        ObjectMap params = new ObjectMap();
        params.put("body", data);
        return execute("projects", null, null, null, "create", params, POST, Project.class);
    }

    /**
     * Increment current release number in the project.
     * @param project Project [user@]project where project can be either the ID or the alias.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Integer> incRelease(String project) throws ClientException {
        ObjectMap params = new ObjectMap();
        return execute("projects", project, null, null, "incRelease", params, POST, Integer.class);
    }

    /**
     * Fetch all the studies contained in the project.
     * @param project Project [user@]project where project can be either the ID or the alias.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Study> studies(String project, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("projects", project, null, null, "studies", params, GET, Study.class);
    }
}
