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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by imedina on 09/05/16.
 */
public class ProjectClient extends AbstractParentClient {

    private static final String PROJECTS_URL = "projects";

    protected ProjectClient(String sessionId, ClientConfiguration configuration) {
        super(sessionId, configuration);
    }

    public QueryResponse<Project> get(String projectId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<Project> project = execute(PROJECTS_URL, projectId, "info", options, Project.class);
        return project;
    }

    public QueryResponse<Study> getStudies(String projectId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<Study> studies = execute(PROJECTS_URL, projectId, "studies", options, Study.class);
        return studies;
    }

    public QueryResponse<Project> update(String projectId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<Project> project = execute(PROJECTS_URL, projectId, "update", options, Project.class);
        return project;
    }

    public QueryResponse<ObjectMap> delete(String projectId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<ObjectMap> project = execute(PROJECTS_URL, projectId, "delete", options, ObjectMap.class);
        return project;
    }
}
