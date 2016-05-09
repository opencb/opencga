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

import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.client.config.ClientConfiguration;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 04/05/16.
 */
class AbstractClient {

    protected Client client;
    protected ClientConfiguration configuration;

    protected AbstractClient(ClientConfiguration configuration) {
        this.configuration = configuration;

        init();
    }

    private void init() {
        this.client = ClientBuilder.newClient();

    }

    protected <T> QueryResult<T> get(String category, String id, String resource, Map<String, Object> params) {
        WebTarget path = client.target(configuration.getRest().getHost())
                .path(category)
                .path(id)
                .path(resource);

        params.keySet().stream()
                .forEach(s -> path.queryParam(s, params.get(s)));

        System.out.println(params);
        try {
            System.out.println("REST URL: " + path.getUri().toURL());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        QueryResponse queryResponse = path.request().get().readEntity(QueryResponse.class);
        System.out.println(queryResponse);
        return (QueryResult<T>) queryResponse.getResponse().get(0);
    }

//    protected <T> List<QueryResult<T>> get(String category, List<String> ids, String resource, Map<String, Object> params) {
//
//        WebTarget path = client.target(configuration.getRest().getHost()).path(category).path(ids.get(0)).path(resource);
//        params.keySet().stream()
//                .forEach(s -> path.queryParam(s, params.get(s)));
//
//        System.out.println(path.getUri());
//        QueryResponse queryResponse = path.request().get().readEntity(QueryResponse.class);
//        System.out.println(queryResponse);
//    }

    protected Map<String, Object> createParamsMap(String key, Object value) {
        Map<String, Object> params= new HashMap<>(5);
        params.put(key, value);
        return params;
    }
}
