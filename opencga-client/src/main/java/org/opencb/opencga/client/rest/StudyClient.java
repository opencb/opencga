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
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by swaathi on 10/05/16.
 */
public class StudyClient extends AbstractParentClient<Study> {

    private static final String STUDY_URL = "studies";

    protected StudyClient(String sessionId, ClientConfiguration configuration) {
        super(sessionId, configuration);

        this.category = STUDY_URL;
        this.clazz = Study.class;
    }

    public QueryResponse<Study> create(String projectId, String studyName, String studyAlias, String studyDescription,
                                       ObjectMap params) throws CatalogException, IOException {
        addParamsToObjectMap(params, "projectId", projectId, "name", studyName, "alias", studyAlias, "description", studyDescription);
        return execute(STUDY_URL, "create", params, Study.class);
    }

    public QueryResponse<Sample> getSamples(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "samples", options, Sample.class);
    }

    public QueryResponse<File> getFiles(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "files", options, File.class);
    }

    public QueryResponse<Job> getJobs(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "jobs", options, Job.class);
    }

    public QueryResponse<ObjectMap> getStatus(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "status", options, ObjectMap.class);
    }

    public QueryResponse<Study> update(String studyId, ObjectMap params) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "update", params, Study.class);
    }

    public QueryResponse<Study> delete(String studyId, ObjectMap params) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "delete", params, Study.class);
    }
}
