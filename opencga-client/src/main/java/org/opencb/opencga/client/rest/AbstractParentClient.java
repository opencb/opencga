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
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.config.ClientConfiguration;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 04/05/16.
 */
public abstract class AbstractParentClient<T> {

    protected Client client;

    private String sessionId;
    private ClientConfiguration configuration;

    protected String category;
    protected Class<T> clazz;

    protected static ObjectMapper jsonObjectMapper;

    private final static int BATCH_SIZE = 2000;
    private final static int DEFAULT_SKIP = 0;

    protected AbstractParentClient(String sessionId, ClientConfiguration configuration) {
        this.sessionId = sessionId;
        this.configuration = configuration;

        init();
    }

    private void init() {
        this.client = ClientBuilder.newClient();
        jsonObjectMapper = new ObjectMapper();
    }


    public QueryResponse<Long> count(Query query) throws IOException {
        return execute(category, "count", query, Long.class);
    }

    public QueryResponse<T> get(String id, QueryOptions options) throws CatalogException, IOException {
        return execute(category, id, "info", options, clazz);
    }

    public QueryResponse<T> search(Query query, QueryOptions options) throws IOException {
        return execute(category, "search", query, clazz);
    }

    public QueryResponse<T> update(String id, ObjectMap params) throws CatalogException, IOException {
        return execute(category, id, "update", params, clazz);
    }

    public QueryResponse<T> delete(String id, ObjectMap params) throws CatalogException, IOException {
        return execute(category, id, "delete", params, clazz);
    }



    protected <T> QueryResponse<T> execute(String category, String action, Map<String, Object> params, Class<T> clazz) throws IOException {
        return execute(category, null, action, params, clazz);
    }

    protected <T> QueryResponse<T> execute(String category, String id, String action, Map<String, Object> params, Class<T> clazz)
            throws IOException {

        System.out.println("configuration = " + configuration);
        // Build the basic URL
        WebTarget path = client
                .target(configuration.getRest().getHost())
                .path("webservices")
                .path("rest")
                .path("v1")
                .path(category);

        if (params == null) {
            params = new HashMap<>();
        }

        int numRequiredFeatures = (int) params.getOrDefault(QueryOptions.LIMIT, Integer.MAX_VALUE);
        int limit = Math.min(numRequiredFeatures, BATCH_SIZE);

        int skip = (int) params.getOrDefault(QueryOptions.SKIP, DEFAULT_SKIP);

        // Session ID is needed almost always, the only exceptions are 'create/user' and 'login'
        if (this.sessionId != null && !this.sessionId.isEmpty()) {
            path = path.queryParam("sid", this.sessionId);
        }

        QueryResponse<T> finalQueryResponse = null;
        QueryResponse<T> queryResponse;

        while (true) {
            params.put(QueryOptions.SKIP, skip);
            params.put(QueryOptions.LIMIT, limit);

            queryResponse = (QueryResponse<T>) callRest(path, id, action, params, clazz);
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

    protected QueryResponse<T> callRest(WebTarget path, String id, String action, Map<String, Object> params, Class clazz)
            throws IOException {

        // TODO we still have to check if there are multiple IDs, the limit is 200 pero query, this can be parallelized
        // Some WS do not have IDs such as 'create'
        if (id != null && !id.isEmpty()) {
            path = path.path(id);
        }

        // Add the last URL part, the 'action'
        path = path.path(action);

        // TODO we still have to check the limit of the query, and keep querying while there are more results
        if (params != null) {
            for (String s : params.keySet()) {
                path = path.queryParam(s, params.get(s));
            }
        }

        System.out.println("REST URL: " + path.getUri().toURL());
        String jsonString = path.request().get(String.class);
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

    protected void addParamsToObjectMap(ObjectMap objectMap, String key, Object value, Object ... params) {
        objectMap = createIfNull(objectMap);
        objectMap.put(key, value);
        if (params != null && params.length > 0) {
            for (int i = 0; i < params.length; i += 2) {
                objectMap.put(params[i].toString(), params[i+1]);
            }
        }
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
}
