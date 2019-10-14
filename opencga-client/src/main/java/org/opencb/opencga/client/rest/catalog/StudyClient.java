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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.*;

import java.io.IOException;

/**
 * Created by swaathi on 10/05/16.
 */
public class StudyClient extends CatalogClient<Study> {

    private static final String STUDY_URL = "studies";

    public StudyClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
        this.category = STUDY_URL;
        this.clazz = Study.class;
    }

    public DataResponse<Study> create(String projectId, String studyId, ObjectMap params) throws IOException {
        params = addParamsToObjectMap(params, "id", studyId);
        ObjectMap p = new ObjectMap("body", params);
        p = addParamsToObjectMap(p, "projectId", projectId);
        return execute(STUDY_URL, "create", p, POST, Study.class);
    }

    public DataResponse<ObjectMap> getStats(String studyId, Query query, QueryOptions options) throws IOException {
        ObjectMap params = new ObjectMap(query);
        params.putAll(options);

        return execute(STUDY_URL, studyId, "stats", params, GET, ObjectMap.class);
    }

    @Deprecated
    public DataResponse<Sample> getSamples(String studyId, QueryOptions options) throws IOException {
        return execute(STUDY_URL, studyId, "samples", options, GET, Sample.class);
    }

    @Deprecated
    public DataResponse<File> getFiles(String studyId, QueryOptions options) throws IOException {
        return execute(STUDY_URL, studyId, "files", options, GET, File.class);
    }

    @Deprecated
    public DataResponse<Job> getJobs(String studyId, QueryOptions options) throws IOException {
        return execute(STUDY_URL, studyId, "jobs", options, GET, Job.class);
    }

    public DataResponse scanFiles(String studyId, QueryOptions options) throws IOException {
        return execute(STUDY_URL, studyId, "scanFiles", options, GET, Object.class);
    }

    public DataResponse resyncFiles(String studyId, QueryOptions options) throws IOException {
        return execute(STUDY_URL, studyId, "resyncFiles", options, GET, Object.class);
    }

    public DataResponse<ObjectMap> createGroup(String studyId, String groupId, String groupName, String users) throws IOException {
        ObjectMap queryParams = new ObjectMap();
        queryParams.append("action", "ADD");

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotEmpty("id", groupId);
        bodyParams.putIfNotEmpty("name", groupName);
        bodyParams.putIfNotEmpty("users", users);
        return execute(STUDY_URL, studyId, "groups", null, "update", queryParams.append("body", bodyParams), POST, ObjectMap.class);
    }

    public DataResponse<ObjectMap> deleteGroup(String studyId, String groupId, QueryOptions options) throws IOException {
        ObjectMap queryParams = new ObjectMap();
        queryParams.append("action", "REMOVE");

        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotEmpty("id", groupId);

        return execute(STUDY_URL, studyId, "groups", null, "update", queryParams.append("body", bodyParams), POST, ObjectMap.class);
    }

    public DataResponse<ObjectMap> updateGroup(String studyId, String groupId, ObjectMap objectMap) throws IOException {
        ObjectMap bodyParams = new ObjectMap("body", objectMap);
        return execute(STUDY_URL, studyId, "groups", groupId, "update", bodyParams, POST, ObjectMap.class);
    }

    public DataResponse<ObjectMap> updateGroupMember(String studyId, ObjectMap objectMap) throws IOException {
        ObjectMap bodyParams = new ObjectMap("body", objectMap);
        return execute(STUDY_URL, studyId, "groups", "members", "update", bodyParams, POST, ObjectMap.class);
    }

    public DataResponse<ObjectMap> updateGroupAdmins(String studyId, ObjectMap objectMap) throws IOException {
        ObjectMap bodyParams = new ObjectMap("body", objectMap);
        return execute(STUDY_URL, studyId, "groups", "admins", "update", bodyParams, POST, ObjectMap.class);
    }

    public DataResponse<ObjectMap> groups(String studyId, ObjectMap objectMap) throws IOException {
        ObjectMap params = new ObjectMap(objectMap);
        return execute(STUDY_URL, studyId, "groups", params, GET, ObjectMap.class);
    }

    public DataResponse<Study> update(String studyId, String study, ObjectMap params) throws IOException {
        ObjectMapper mapper = JacksonUtils.getUpdateObjectMapper();
        String json = mapper.writeValueAsString(params);
        ObjectMap p = new ObjectMap("body", json);
        return execute(STUDY_URL, studyId, "update", p, POST, Study.class);
    }

    public DataResponse<Study> delete(String studyId, ObjectMap params) throws IOException {
        return execute(STUDY_URL, studyId, "delete", params, GET, Study.class);
    }

    public DataResponse<VariableSet> getVariableSets(String studyId, Query query) throws IOException {
        return execute(STUDY_URL, studyId, "variableSets", query, GET, VariableSet.class);
    }

    public DataResponse<VariableSet> updateVariableSet(String studyId, Query query, ObjectMap variableSet) throws IOException {
        ObjectMapper mapper = JacksonUtils.getUpdateObjectMapper();
        String json = mapper.writeValueAsString(variableSet);
        query.append("body", json);
        return execute(STUDY_URL, studyId, "variableSets", "", "update", query, POST, VariableSet.class);
    }

    public DataResponse<VariableSet> updateVariableSetVariable(String studyId, String variableSet, Query query, ObjectMap variable)
            throws IOException {
        ObjectMapper mapper = JacksonUtils.getUpdateObjectMapper();
        String json = mapper.writeValueAsString(variable);
        query.append("body", json);
        return execute(STUDY_URL, studyId, "variableSets", variableSet, "variables/update", query, POST, VariableSet.class);
    }
}
