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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;

/**
 * Created by imedina on 09/05/16.
 */
public class ProjectClient extends CatalogClient<Project> {

    private static final String PROJECTS_URL = "projects";

    public ProjectClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = PROJECTS_URL;
        this.clazz = Project.class;
    }

    public RestResponse<Project> create(ObjectMap bodyParams) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull("body", bodyParams);
        return execute(PROJECTS_URL, "create", params, POST, Project.class);
    }

    public RestResponse<Study> getStudies(String projectId, QueryOptions options) throws IOException {
        return execute(PROJECTS_URL, projectId, "studies", options, GET, Study.class);
    }

}
