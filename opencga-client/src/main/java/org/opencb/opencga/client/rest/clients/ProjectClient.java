/*
* Copyright 2015-2024 OpenCB
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

package org.opencb.opencga.client.rest.clients;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.rest.*;
import org.opencb.opencga.core.client.ParentClient;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.project.ProjectCreateParams;
import org.opencb.opencga.core.models.project.ProjectUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.RestResponse;


/*
* WARNING: AUTOGENERATED CODE
*
* This code was generated by a tool.
*
* Manual changes to this file may cause unexpected behavior in your application.
* Manual changes to this file will be overwritten if the code is regenerated.
*/


/**
 * This class contains methods for the Project webservices.
 *    PATH: projects
 */
public class ProjectClient extends ParentClient {

    public ProjectClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Create a new project.
     * @param data JSON containing the mandatory parameters.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       includeResult: Flag indicating to include the created or updated document result in the response.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Project> create(ProjectCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("projects", null, null, null, "create", params, POST, Project.class);
    }

    /**
     * Search projects.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       limit: Number of results to be returned.
     *       skip: Number of results to skip.
     *       organization: Project organization.
     *       id: Project [organization@]project where project can be either the ID or the alias.
     *       name: Project name.
     *       fqn: Project fqn.
     *       organization: Project organization.
     *       description: Project description.
     *       study: Study id.
     *       creationDate: Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
     *       modificationDate: Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
     *       internalStatus: Filter by internal status.
     *       attributes: Attributes.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Project> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("projects", null, null, null, "search", params, GET, Project.class);
    }

    /**
     * Fetch project information.
     * @param projects Comma separated list of projects [organization@]project up to a maximum of 100.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Project> info(String projects, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("projects", projects, null, null, "info", params, GET, Project.class);
    }

    /**
     * Increment current release number in the project.
     * @param project Project [organization@]project where project can be either the ID or the alias.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Integer> incRelease(String project) throws ClientException {
        ObjectMap params = new ObjectMap();
        return execute("projects", project, null, null, "incRelease", params, POST, Integer.class);
    }

    /**
     * Fetch all the studies contained in the project.
     * @param project Project [organization@]project where project can be either the ID or the alias.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       limit: Number of results to be returned.
     *       skip: Number of results to skip.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Study> studies(String project, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("projects", project, null, null, "studies", params, GET, Study.class);
    }

    /**
     * Update some project attributes.
     * @param project Project [organization@]project where project can be either the ID or the alias.
     * @param data JSON containing the params to be updated. It will be only possible to update organism fields not previously defined.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       includeResult: Flag indicating to include the created or updated document result in the response.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Project> update(String project, ProjectUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("projects", project, null, null, "update", params, POST, Project.class);
    }
}
