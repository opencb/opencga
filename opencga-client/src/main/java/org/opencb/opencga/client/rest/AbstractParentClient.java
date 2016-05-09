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
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
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
class AbstractParentClient {

    protected Client client;

    private String sessionId;
    private ClientConfiguration configuration;

    protected static ObjectMapper jsonObjectMapper;

    private final static int DEFAULT_LIMIT = 2000;

    protected AbstractParentClient(String sessionId, ClientConfiguration configuration) {
        this.sessionId = sessionId;
        this.configuration = configuration;

        init();
    }

    private void init() {
        this.client = ClientBuilder.newClient();
        jsonObjectMapper = new ObjectMapper();
    }

    protected <T> QueryResponse<T> execute(String category, String id, String resource, Map<String, Object> params, Class<T> clazz)
            throws IOException {
        WebTarget path = client.target(configuration.getRest().getHost())
                .path("v1")
                .path(category)
                .path(id)
                .path(resource);

        if (params != null) {
            for (String s : params.keySet()) {
                path = path.queryParam(s, params.get(s));
            }
        }

        path = path.queryParam("sid", this.sessionId);

        System.out.println("REST URL: " + path.getUri().toURL());
        String jsonString = path.request().get(String.class);
        System.out.println("jsonString = " + jsonString);
        QueryResponse<T> queryResponse = parseResult(jsonString, clazz);
        System.out.println("queryResponse = " + queryResponse);
        return queryResponse;
    }

    public static <T> QueryResponse<T> parseResult(String json, Class<T> clazz) throws IOException {
        ObjectReader reader = jsonObjectMapper
                .reader(jsonObjectMapper.getTypeFactory().constructParametrizedType(QueryResponse.class, QueryResult.class, clazz));
        return reader.readValue(json);
    }

    protected Map<String, Object> createParamsMap(String key, Object value) {
        Map<String, Object> params= new HashMap<>(10);
        params.put(key, value);
        return params;
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
