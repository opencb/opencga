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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.config.ClientConfiguration;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 04/05/16.
 */
public abstract class AbstractParentClient<T, A> {

    protected Client client;

    private String userId;
    private String sessionId;
    private ClientConfiguration configuration;

    protected String category;
    protected Class<T> clazz;
    protected Class<A> aclClass;

    protected static ObjectMapper jsonObjectMapper;

    private static final int BATCH_SIZE = 2000;
    private static final int DEFAULT_SKIP = 0;
    protected static final String GET = "GET";
    protected static final String POST = "POST";

    protected AbstractParentClient(String userId, String sessionId, ClientConfiguration configuration) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.configuration = configuration;

        init();
    }

    public enum AclParams {
        ADD_PERMISSIONS("addPermissions"),
        REMOVE_PERMISSIONS("removePermissions"),
        SET_PERMISSIONS("setPermissions");

        private String key;

        AclParams(String value) {
            this.key = value;
        }

        public String key() {
            return this.key;
        }
    }

    private void init() {
        this.client = ClientBuilder.newClient();
        jsonObjectMapper = new ObjectMapper();
    }


    public QueryResponse<Long> count(Query query) throws IOException {
        return execute(category, "count", query, GET, Long.class);
    }

    public QueryResponse<T> get(String id, QueryOptions options) throws CatalogException, IOException {
        return execute(category, id, "info", options, GET, clazz);
    }

    public QueryResponse<T> search(Query query, QueryOptions options) throws IOException {
        ObjectMap myQuery = new ObjectMap(query);
        myQuery.putAll(options);
        return execute(category, "search", myQuery, GET, clazz);
    }

    public QueryResponse<T> update(String id, ObjectMap params) throws CatalogException, IOException {
        return execute(category, id, "update", params, GET, clazz);
    }

    public QueryResponse<T> delete(String id, ObjectMap params) throws CatalogException, IOException {
        return execute(category, id, "delete", params, GET, clazz);
    }


   /* /**
     * Shares the document with the list of members.
     *
     * @param ids Comma separated list of ids.
     * @param members Comma separated list of members (groups and/or users).
     * @param permissions Comma separated list of permissions.
     * @param params Non mandatory parameters.
     * @return a QueryResponse containing all the permissions set.
     * @throws IOException in case of any error.
     */
   /* public QueryResponse<A> share(String ids, String members, List<String> permissions, ObjectMap params) throws IOException {
        params = addParamsToObjectMap(params, "permissions", permissions, "members", members);
        return execute(category, ids, "share", params, aclClass);
    }

    public QueryResponse<Object> unshare(String ids, String members, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "members", members);
        return execute(category, ids, "unshare", params, Object.class);
    }*/

//    public QueryResponse<StudyAcl> getAcl(String studyId) throws IOException {
//        return execute(STUDY_URL, studyId, "acls", new ObjectMap(), GET, StudyAcl.class);
//    }
//
//    public QueryResponse<StudyAcl> createAcl(String studyId, String members, String permissions, ObjectMap objectMap) throws IOException {
//        ObjectMap params = new ObjectMap(objectMap);
//        params = addParamsToObjectMap(params, "members", members, "permissions", permissions);
//        return execute(STUDY_URL, studyId, "acls", null, "create", params, GET, StudyAcl.class);
//    }

    public QueryResponse<A> getAcls(String id) throws IOException {
        return execute(category, id, "acl", new ObjectMap(), GET, aclClass);
    }

    public QueryResponse<A> getAcl(String id, String memberId) throws CatalogException, IOException {
        return execute(category, id, "acl", memberId, "info", new ObjectMap(), GET, aclClass);
    }

    public QueryResponse<A> createAcl(String id, String permissions, String members, ObjectMap params) throws CatalogException,
            IOException {
        params = addParamsToObjectMap(params,  "permissions", permissions, "members", members);
        return execute(category, id, "acl", null, "create", params, GET, aclClass);
    }

    public QueryResponse<Object> deleteAcl(String id, String memberId, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "memberId", memberId);
        return execute(category, id, "acl", memberId, "delete", params, GET, Object.class);
    }

    public QueryResponse<A> updateAcl(String id, String memberId, ObjectMap params) throws CatalogException, IOException {
        return execute(category, id, "acl", memberId, "update", params, GET, aclClass);
    }

    protected <T> QueryResponse<T> execute(String category, String action, Map<String, Object> params, String method, Class<T> clazz)
            throws IOException {
        return execute(category, null, action, params, method, clazz);
    }

    protected <T> QueryResponse<T> execute(String category, String id, String action, Map<String, Object> params, String method,
                                           Class<T> clazz) throws IOException {
        return execute(category, id, null, null, action, params, method, clazz);
    }

    protected <T> QueryResponse<T> execute(String category1, String id1, String category2, String id2, String action,
                                           Map<String, Object> params, String method, Class<T> clazz) throws IOException {

        System.out.println("configuration = " + configuration);
        // Build the basic URL
        WebTarget path = client
                .target(configuration.getRest().getHost())
                .path("webservices")
                .path("rest")
                .path("v1")
                .path(category1);

        // TODO we still have to check if there are multiple IDs, the limit is 200 pero query, this can be parallelized
        // Some WS do not have IDs such as 'create'
        if (id1 != null && !id1.isEmpty()) {
            path = path.path(id1);
        }

        if (category2 != null && !category2.isEmpty()) {
            path = path.path(category2);
        }

        if (id2 != null && !id2.isEmpty()) {
            path = path.path(id2);
        }

        // Add the last URL part, the 'action'
        path = path.path(action);

        if (params == null) {
            params = new HashMap<>();
        }

        int numRequiredFeatures = (int) params.getOrDefault(QueryOptions.LIMIT, Integer.MAX_VALUE);
        int limit = Math.min(numRequiredFeatures, BATCH_SIZE);

        int skip = (int) params.getOrDefault(QueryOptions.SKIP, DEFAULT_SKIP);

        // Session ID is needed almost always, the only exceptions are 'create/user', 'login' and 'changePassword'
        if (this.sessionId != null && !this.sessionId.isEmpty()) {
            path = path.queryParam("sid", this.sessionId);
        }

        QueryResponse<T> finalQueryResponse = null;
        QueryResponse<T> queryResponse;

        while (true) {
            params.put(QueryOptions.SKIP, skip);
            params.put(QueryOptions.LIMIT, limit);

            if (!action.equals("upload")) {
                queryResponse = (QueryResponse<T>) callRest(path, params, clazz, method);
            } else {
                queryResponse = (QueryResponse<T>) callUploadRest(path, params, clazz);
            }
            int numResults = queryResponse.getResponse().get(0).getNumResults();

            if (finalQueryResponse == null) {
                finalQueryResponse = queryResponse;
            } else {
                if (numResults > 0) {
                    finalQueryResponse.getResponse().get(0).getResult().addAll(queryResponse.getResponse().get(0).getResult());
                    finalQueryResponse.getResponse().get(0).setNumResults(finalQueryResponse.getResponse().get(0).getResult().size());
                }
            }

            int numTotalResults = finalQueryResponse.getResponse().get(0).getNumResults();
            if (numResults < limit || numTotalResults == numRequiredFeatures || numResults == 0) {
                break;
            }

            // DO NOT CHANGE THE ORDER OF THE FOLLOWING CODE
            skip += numResults;
            if (skip + BATCH_SIZE < numRequiredFeatures) {
                limit = BATCH_SIZE;
            } else {
                limit = numRequiredFeatures - numTotalResults;
            }

        }
        return finalQueryResponse;
    }

    /**
     * Call to WS using get or post method.
     *
     * @param path Path of the WS.
     * @param params Params to be passed to the WS.
     * @param clazz Expected return class.
     * @param method Method by which the query will be done (GET or POST).
     * @return A queryResponse object containing the results of the query.
     * @throws IOException if the path is wrong and cannot be converted to a proper url.
     */
    protected QueryResponse<T> callRest(WebTarget path, Map<String, Object> params, Class clazz, String method) throws IOException {

        String jsonString = "{}";
        if (method.equalsIgnoreCase(GET)) {
            // TODO we still have to check the limit of the query, and keep querying while there are more results
            if (params != null) {
                for (String s : params.keySet()) {
                    path = path.queryParam(s, params.get(s));
                }
            }

            System.out.println("GET URL: " + path.getUri().toURL());
            jsonString = path.request().get().readEntity(String.class);
        } else if (method.equalsIgnoreCase(POST)) {
            // TODO we still have to check the limit of the query, and keep querying while there are more results
//            Form form = new Form();
//            if (params != null) {
//                for (String s : params.keySet()) {
//                    Object value = params.get(s);
//                    if (value instanceof Number) {
//                        form.param(s, (Integer.toString((int) params.get(s))));
//                    } else {
//                        form.param(s, ((String) params.get(s)));
//                    }
//                }
//            }

            System.out.println("POST URL: " + path.getUri().toURL());
            jsonString = path.request().accept(MediaType.APPLICATION_JSON).post(Entity.entity(params, MediaType.APPLICATION_JSON),
                    String.class);
        }
        return parseResult(jsonString, clazz);
    }

    /**
     * Call to upload WS.
     *
     * @param path Path of the WS.
     * @param params Params to be passed to the WS.
     * @param clazz Expected return class.
     * @return A queryResponse object containing the results of the query.
     * @throws IOException if the path is wrong and cannot be converted to a proper url.
     */
    protected QueryResponse<T> callUploadRest(WebTarget path, Map<String, Object> params, Class clazz) throws IOException {

        String jsonString;

        String filePath = ((String) params.get("file"));
        params.remove("file");

        path.register(MultiPartFeature.class);

        final FileDataBodyPart filePart = new FileDataBodyPart("file", new File(filePath));
        FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
        // Add the rest of the parameters to the form
        for (Map.Entry<String, Object> stringObjectEntry : params.entrySet()) {
            formDataMultiPart.field(stringObjectEntry.getKey(), stringObjectEntry.getValue().toString());
        }
        final FormDataMultiPart multipart = (FormDataMultiPart) formDataMultiPart.bodyPart(filePart);

        jsonString = path.request().post(Entity.entity(multipart, multipart.getMediaType()), String.class);

        formDataMultiPart.close();
        multipart.close();

        return parseResult(jsonString, clazz);
    }

    public static <T> QueryResponse<T> parseResult(String json, Class<T> clazz) throws IOException {
        if (json != null && !json.isEmpty()) {
            ObjectReader reader = jsonObjectMapper
                    .readerFor(jsonObjectMapper.getTypeFactory().constructParametrizedType(QueryResponse.class, QueryResult.class, clazz));
            return reader.readValue(json);
        } else {
            return new QueryResponse<>();
        }
    }

    @Deprecated
    protected Map<String, Object> createParamsMap(String key, Object value) {
        Map<String, Object> params= new HashMap<>(10);
        params.put(key, value);
        return params;
    }

    protected ObjectMap createIfNull(ObjectMap objectMap) {
        if (objectMap == null) {
            objectMap = new ObjectMap();
        }
        return objectMap;
    }

    protected ObjectMap addParamsToObjectMap(ObjectMap objectMap, String key, Object value, Object ... params) {
        objectMap = createIfNull(objectMap);
        objectMap.put(key, value);
        if (params != null && params.length > 0) {
            for (int i = 0; i < params.length; i += 2) {
                objectMap.put(params[i].toString(), params[i + 1]);
            }
        }
        return objectMap;
    }


    public String getSessionId() {
        return sessionId;
    }

    public AbstractParentClient setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public ClientConfiguration getConfiguration() {
        return configuration;
    }

    public AbstractParentClient setConfiguration(ClientConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public AbstractParentClient setUserId(String userId) {
        this.userId = userId;
        return this;
    }
}
