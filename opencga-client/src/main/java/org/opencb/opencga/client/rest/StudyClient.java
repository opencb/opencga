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
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by swaathi on 10/05/16.
 */
public class StudyClient extends AbstractParentClient<Study, StudyAclEntry> {

    private static final String STUDY_URL = "studies";

    public enum GroupUpdateParams {
        ADD_USERS("addUsers"),
        SET_USERS("setUsers"),
        REMOVE_USERS("removeUsers");

        private String key;

        GroupUpdateParams(String value) {
            this.key = value;
        }

        public String key() {
            return this.key;
        }
    }

    protected StudyClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
        this.category = STUDY_URL;
        this.clazz = Study.class;
        this.aclClass = StudyAclEntry.class;
    }

    public QueryResponse<Study> create(String projectId, String studyName, String studyAlias, ObjectMap params)
            throws CatalogException, IOException {
        if (params.containsKey("method")) {
            if (params.get("method").equals("GET")) {
                params = addParamsToObjectMap(params, "projectId", projectId, "name", studyName, "alias", studyAlias);
                return execute(STUDY_URL, "create", params, GET, Study.class);
            } else {
                params.remove("method");
            }
        }
        params = addParamsToObjectMap(params, "name", studyName, "alias", studyAlias);
        ObjectMapper mapper = new ObjectMapper();
        String json = "[" + mapper.writeValueAsString(params) + "]";
        //String json = mapper.writeValueAsString(params);
        System.out.println("Json: " + json);
        ObjectMap p = new ObjectMap("body", json);
        p = addParamsToObjectMap(p, "projectId", projectId);
        return execute(STUDY_URL, "create", p, POST, Study.class);
    }

    public QueryResponse<Study> search(Query query, QueryOptions options) throws IOException {
        ObjectMap myQuery = new ObjectMap(query);
        myQuery.putAll(options);
        if (myQuery.containsKey("method")) {
            if (myQuery.get("method").equals("GET")) {
                return execute(category, "search", myQuery, GET, clazz);
            } else {
                query.remove("method");
            }
        }
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(query);
        System.out.println("Json: " + json);
        ObjectMap p = new ObjectMap("body", json);
        p.putAll(options);
        return execute(category, "search", p, POST, clazz);
    }
    public QueryResponse<StudySummary> getSummary(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "summary", options, GET, StudySummary.class);
    }

    public QueryResponse<Sample> getSamples(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "samples", options, GET, Sample.class);
    }

    public QueryResponse<File> getFiles(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "files", options, GET, File.class);
    }

    public QueryResponse<Job> getJobs(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "jobs", options, GET, Job.class);
    }

    public QueryResponse<ObjectMap> getStatus(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "status", options, GET, ObjectMap.class);
    }

    public QueryResponse<Variant> getVariants(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "variants", options, GET, Variant.class);
    }

    public QueryResponse<Alignment> getAlignments(String studyId, String sampleId, String fileId, String region, Query query,
                                                  QueryOptions options) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap(query);
        params.putAll(options);
        params = addParamsToObjectMap(params, "sampleId", sampleId, "fileId", fileId, "region", region);
        params.putIfAbsent("view_as_pairs", false);
        params.putIfAbsent("include_coverage", true);
        params.putIfAbsent("process_differences", true);
        params.putIfAbsent("histogram", false);
        params.putIfAbsent("interval", 200);
        return execute(STUDY_URL, studyId, "alignments", params, GET, Alignment.class);
    }

    public QueryResponse scanFiles(String studyId, QueryOptions options) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "scanFiles", options, GET, Object.class);
    }

    public QueryResponse<ObjectMap> createGroup(String studyId, String groupId, String users, QueryOptions options)
            throws CatalogException, IOException {
        ObjectMap params = new ObjectMap(options);
        params = addParamsToObjectMap(params, "groupId", groupId, "users", users);
        return execute(STUDY_URL, studyId, "groups", null, "create", params, GET, ObjectMap.class);
    }

    public QueryResponse<ObjectMap> deleteGroup(String studyId, String groupId, QueryOptions options)
            throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "groups", groupId, "delete", options, GET, ObjectMap.class);
    }

    public QueryResponse<ObjectMap> updateGroup(String studyId, String groupId, ObjectMap objectMap) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "groups", groupId, "update", objectMap, GET, ObjectMap.class);
    }

    public QueryResponse<ObjectMap> groups(String studyId, ObjectMap objectMap) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap(objectMap);
        return execute(STUDY_URL, studyId, "groups", params, GET, ObjectMap.class);
    }

    public QueryResponse<ObjectMap> infoGroup(String studyId,  String groupId, ObjectMap objectMap) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "groups", groupId, "info", objectMap, GET, ObjectMap.class);
    }

    public QueryResponse<Study> update(String studyId, ObjectMap params) throws CatalogException, IOException {
        if (params.containsKey("method")) {
            if (params.get("method").equals("GET")) {
                return execute(STUDY_URL, studyId, "update", params, GET, Study.class);
            }
        }
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(params);
        ObjectMap p = new ObjectMap("body", json);
        return execute(STUDY_URL, studyId, "update", p, POST, Study.class);
    }

    public QueryResponse<Study> delete(String studyId, ObjectMap params) throws CatalogException, IOException {
        return execute(STUDY_URL, studyId, "delete", params, GET, Study.class);
    }
}
