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

package org.opencb.opencga.client.rest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by imedina on 04/05/16.
 */
public abstract class AbstractParentClient {

    protected Client client;

    private String userId;
    private String sessionId;
    private ClientConfiguration configuration;

    protected ObjectMapper jsonObjectMapper;

    private static int timeout = 10000;
    private static int batchSize = 2000;
    private static int defaultLimit = 2000;
    private static final int DEFAULT_SKIP = 0;
    protected static final String GET = "GET";
    protected static final String POST = "POST";

    protected Logger logger;

    protected AbstractParentClient(String userId, String sessionId, ClientConfiguration configuration) {
        this.userId = userId;
        this.sessionId = sessionId;
        this.configuration = configuration;

        init();
    }

    private void init() {
        this.logger = LoggerFactory.getLogger(this.getClass().toString());
        this.client = ClientBuilder.newClient();
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        if (configuration.getRest() != null) {
            if (configuration.getRest().getTimeout() > 0) {
                timeout = configuration.getRest().getTimeout();
            }
            if (configuration.getRest().getBatchQuerySize() > 0) {
                batchSize = configuration.getRest().getBatchQuerySize();
            }
            if (configuration.getRest().getDefaultLimit() > 0) {
                defaultLimit = configuration.getRest().getDefaultLimit();
            }
        }
    }

    protected <T> VariantQueryResult<T> executeVariantQuery(String category, String action, Map<String, Object> params, String method,
                                                            Class<T> clazz) throws IOException {
        QueryResponse<T> queryResponse = execute(category, null, action, params, method, clazz);
        return (VariantQueryResult<T>) queryResponse.first();
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
                                           Map<String, Object> paramsMap, String method, Class<T> clazz) throws IOException {

        ObjectMap params;
        if (paramsMap == null) {
            params = new ObjectMap();
        } else {
            params = new ObjectMap(paramsMap);
        }

        client.property(ClientProperties.CONNECT_TIMEOUT, 1000);
        client.property(ClientProperties.READ_TIMEOUT, timeout);

        // Build the basic URL
        WebTarget path = client
                .target(configuration.getRest().getHost())
                .path("webservices")
                .path("rest")
                .path("v1")
                .path(category1);

        // TODO we still have to check if there are multiple IDs, the limit is 200 pero query, this can be parallelized
        // Some WS do not have IDs such as 'create'
        if (StringUtils.isNotEmpty(id1)) {
            path = path.path(id1);
        }

        if (StringUtils.isNotEmpty(category2)) {
            path = path.path(category2);
        }

        if (StringUtils.isNotEmpty(id2)) {
            path = path.path(id2);
        }

        // Add the last URL part, the 'action'
        path = path.path(action);

        int numRequiredFeatures = params.getInt(QueryOptions.LIMIT, defaultLimit);
        int limit = Math.min(numRequiredFeatures, batchSize);

        int skip = params.getInt(QueryOptions.SKIP, DEFAULT_SKIP);

        QueryResponse<T> finalQueryResponse = null;
        QueryResponse<T> queryResponse;

        while (true) {
            params.put(QueryOptions.SKIP, skip);
            params.put(QueryOptions.LIMIT, limit);
            params.put(QueryOptions.TIMEOUT, timeout);

            if (!action.equals("upload")) {
                queryResponse = callRest(path, params, clazz, method);
            } else {
                queryResponse = callUploadRest(path, params, clazz);
            }
            int numResults = queryResponse.getResponse().isEmpty() ? 0 : queryResponse.getResponse().get(0).getNumResults();

            if (finalQueryResponse == null) {
                finalQueryResponse = queryResponse;
            } else {
                if (numResults > 0) {
                    finalQueryResponse.getResponse().get(0).getResult().addAll(queryResponse.getResponse().get(0).getResult());
                    finalQueryResponse.getResponse().get(0).setNumResults(finalQueryResponse.getResponse().get(0).getResult().size());
                }
            }

            int numTotalResults = queryResponse.getResponse().isEmpty() ? 0 : finalQueryResponse.getResponse().get(0).getNumResults();
            if (numResults < limit || numTotalResults >= numRequiredFeatures || numResults == 0) {
                break;
            }

            // DO NOT CHANGE THE ORDER OF THE FOLLOWING CODE
            skip += numResults;
            if (skip + batchSize < numRequiredFeatures) {
                limit = batchSize;
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
    private <T> QueryResponse<T> callRest(WebTarget path, Map<String, Object> params, Class clazz, String method) throws IOException {

        String jsonString = "{}";
        if (method.equalsIgnoreCase(GET)) {
            // TODO we still have to check the limit of the query, and keep querying while there are more results
            if (params != null) {
                for (String s : params.keySet()) {
                    Object o = params.get(s);
                    if (o instanceof Collection) {
                        String value = ((Collection<?>) o).stream().map(Object::toString).collect(Collectors.joining(","));
                        path = path.queryParam(s, value);
                    } else {
                        path = path.queryParam(s, o);
                    }
                }
            }

            logger.debug("GET URL: " + path.getUri().toURL());
            jsonString = path.request()
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.sessionId)
                    .get().readEntity(String.class);
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

            if (params != null) {
                for (String s : params.keySet()) {
                    if (!s.equals("body")) {
                        path = path.queryParam(s, params.get(s));
                    }
                }
            }

//            ObjectMap json = new ObjectMap("body", params.get("body"));

            logger.debug("POST URL: " + path.getUri().toURL());
            Response body = path.request()
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.sessionId)
                    .post(Entity.json(params.get("body")));
            jsonString = body.readEntity(String.class);
//            jsonString = path.request().post(Entity.json(params.get("body")), String.class);
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
    private <T> QueryResponse<T> callUploadRest(WebTarget path, Map<String, Object> params, Class<T> clazz) throws IOException {

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

        jsonString = path.request()
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.sessionId)
                .post(Entity.entity(multipart, multipart.getMediaType()), String.class);

        formDataMultiPart.close();
        multipart.close();

        return parseResult(jsonString, clazz);
    }

    private <T> QueryResponse<T> parseResult(String json, Class<T> clazz) throws IOException {
        if (json != null && !json.isEmpty()) {
            ObjectReader reader = jsonObjectMapper
                    .readerFor(jsonObjectMapper.getTypeFactory().constructParametrizedType(QueryResponse.class, QueryResult.class, clazz));
            try {
                return reader.readValue(json);
            } catch (JsonParseException e) {
                if (json.startsWith("<html>")) {
                    if (json.contains("504 Gateway Time-out")) {
                        return new QueryResponse<>("", 0, "", "Error 504 Gateway Time-out. The server didn't respond in time.", null,
                                Collections.emptyList());
                    }
                }
                throw e;
            }
        } else {
            return new QueryResponse<>();
        }
    }

    private ObjectMap createIfNull(ObjectMap objectMap) {
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

    public String getUserId(ObjectMap options) throws CatalogException {
        String userId = this.userId;
        if (options != null && options.containsKey("userId")) {
            userId = options.getString("userId");
        }
        if (userId == null || userId.isEmpty()) {
            throw new CatalogException("Missing user id");
        }
        return userId;
    }

    public AbstractParentClient setUserId(String userId) {
        this.userId = userId;
        return this;
    }
}
