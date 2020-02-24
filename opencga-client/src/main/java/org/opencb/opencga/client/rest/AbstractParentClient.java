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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

/**
 * Created by imedina on 04/05/16.
 */
public abstract class AbstractParentClient {

    protected Client client;

    private String token;
    private ClientConfiguration configuration;
    private boolean throwExceptionOnError = false;

    protected ObjectMapper jsonObjectMapper;

    private static int timeout = 10000;
    private static int batchSize = 2000;
    private static int defaultLimit = 2000;
    private static final int DEFAULT_SKIP = 0;
    protected static final String GET = "GET";
    protected static final String POST = "POST";
    protected static final String DELETE = "DELETE";

    protected Logger logger;

    protected AbstractParentClient(String token, ClientConfiguration configuration) {
        this.token = token;
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

    protected AbstractParentClient setThrowExceptionOnError(boolean throwExceptionOnError) {
        this.throwExceptionOnError = throwExceptionOnError;
        return this;
    }

    protected <T> VariantQueryResult<T> executeVariantQuery(String category, String action, Map<String, Object> params, String method,
                                                            Class<T> clazz) throws ClientException {
        RestResponse<T> queryResponse = execute(category, null, action, params, method, clazz);
        return (VariantQueryResult<T>) queryResponse.first();
    }

    protected <T> RestResponse<T> execute(String category, String action, Map<String, Object> params, String method, Class<T> clazz)
            throws ClientException {
        return execute(category, null, action, params, method, clazz);
    }

    protected <T> RestResponse<T> execute(String category, String id, String action, Map<String, Object> params, String method,
                                          Class<T> clazz) throws ClientException {
        return execute(category, id, null, null, action, params, method, clazz);
    }

    protected <T> RestResponse<T> execute(String category1, String id1, String category2, String id2, String action,
                                          Map<String, Object> paramsMap, String method, Class<T> clazz) throws ClientException {
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
                .path("v2")
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

        RestResponse<T> finalRestResponse = null;
        RestResponse<T> queryResponse;

        while (true) {
            params.put(QueryOptions.SKIP, skip);
            params.put(QueryOptions.LIMIT, limit);
            params.put(QueryOptions.TIMEOUT, timeout);

            if ("upload".equals(action)) {
                queryResponse = callUploadRest(path, params, clazz);
            } else if ("download".equals(action)) {
                String destinyPath = params.getString("OPENCGA_DESTINY");
                params.remove("OPENCGA_DESTINY");
                download(path, params, destinyPath);
                queryResponse = new RestResponse<>();
            } else {
                queryResponse = callRest(path, params, clazz, method);
            }
            int numResults = queryResponse.getResponses().isEmpty() ? 0 : queryResponse.getResponses().get(0).getNumResults();

            if (finalRestResponse == null) {
                finalRestResponse = queryResponse;
            } else {
                if (numResults > 0) {
                    finalRestResponse.getResponses().get(0).getResults().addAll(queryResponse.getResponses().get(0).getResults());
                    finalRestResponse.getResponses().get(0).setNumResults(finalRestResponse.getResponses().get(0).getResults().size());
                }
            }

            int numTotalResults = queryResponse.getResponses().isEmpty() ? 0 : finalRestResponse.getResponses().get(0).getNumResults();
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
        return finalRestResponse;
    }

    /**
     * Call to WS using get or post method.
     *
     * @param path   Path of the WS.
     * @param params Params to be passed to the WS.
     * @param clazz  Expected return class.
     * @param method Method by which the query will be done (GET or POST).
     * @return A queryResponse object containing the results of the query.
     * @throws ClientException if the path is wrong and cannot be converted to a proper url.
     */
    private <T> RestResponse<T> callRest(WebTarget path, ObjectMap params, Class<T> clazz, String method) throws ClientException {

        Response response;
        switch (method) {
            case DELETE:
            case GET:
                // TODO we still have to check the limit of the query, and keep querying while there are more results
                if (params != null) {
                    for (String key : params.keySet()) {
                        path = path.queryParam(key, params.getString(key));
                    }
                }

                Invocation.Builder header = path.request()
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token);
                if (method.equals(GET)) {
                    response = header.get();
                } else {
                    response = header.delete();
                }
                break;
            case POST:
                // TODO we still have to check the limit of the query, and keep querying while there are more results
                if (params != null) {
                    for (String key : params.keySet()) {
                        if (!key.equals("body")) {
                            path = path.queryParam(key, params.getString(key));
                        }
                    }
                }

                Object paramBody = (params == null || params.get("body") == null ? "" : params.get("body"));
                logger.debug("Body {}", paramBody);
                response = path.request()
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token)
                        .post(Entity.json(paramBody));
                break;
            default:
                throw new IllegalArgumentException("Unsupported REST method " + method);
        }

        int status = response.getStatus();
        String jsonString = response.readEntity(String.class);
        RestResponse<T> restResponse = parseResult(jsonString, clazz);

        checkErrors(restResponse, status, method, path);
        return restResponse;
    }

    /**
     * Call to download WS.
     *
     * @param path   Path of the WS.
     * @param params Params to be passed to the WS.
     * @param outputFilePath Path where the file will be written (downloaded).
     */
    private void download(WebTarget path, Map<String, Object> params, String outputFilePath) {
        if (Files.isDirectory(Paths.get(outputFilePath))) {
            outputFilePath += ("/" + new File(path.getUri().getPath().replace(":", "/")).getParentFile().getName());
        } else if (Files.notExists(Paths.get(outputFilePath).getParent())) {
            throw new RuntimeException("Output directory " + outputFilePath + " not found");
        }

        if (params != null) {
            for (String s : params.keySet()) {
                path = path.queryParam(s, params.get(s));
            }
        }

        Response response = path.request()
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token)
                .get();

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            ReadableByteChannel readableByteChannel = Channels.newChannel(response.readEntity(InputStream.class));
            try {
                FileOutputStream fileOutputStream = new FileOutputStream(outputFilePath);
                fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            } catch (IOException e) {
                throw new RuntimeException("Could not write file to " + outputFilePath, e);
            }
        } else {
            throw new RuntimeException("HTTP call failed. Response code is " + response.getStatus() + ". Error reported is "
                    + response.getStatusInfo());
        }
    }

    /**
     * Call to upload WS.
     *
     * @param path   Path of the WS.
     * @param params Params to be passed to the WS.
     * @param clazz  Expected return class.
     * @return A queryResponse object containing the results of the query.
     * @throws ClientException if the path is wrong and cannot be converted to a proper url.
     */
    private <T> RestResponse<T> callUploadRest(WebTarget path, Map<String, Object> params, Class<T> clazz) throws ClientException {
        String filePath = ((String) params.get("file"));
        params.remove("file");
        params.remove("body");

        path.register(MultiPartFeature.class);

        final FileDataBodyPart filePart = new FileDataBodyPart("file", new File(filePath));
        FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
        // Add the rest of the parameters to the form
        for (Map.Entry<String, Object> stringObjectEntry : params.entrySet()) {
            formDataMultiPart.field(stringObjectEntry.getKey(), stringObjectEntry.getValue().toString());
        }
        final FormDataMultiPart multipart = (FormDataMultiPart) formDataMultiPart.bodyPart(filePart);

        Response response = path.request()
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token)
                .post(Entity.entity(multipart, multipart.getMediaType()));
        int status = response.getStatus();
        String jsonString = response.readEntity(String.class);

        try {
            formDataMultiPart.close();
            multipart.close();
        } catch (IOException e) {
            throw new ClientException(e.getMessage(), e);
        }

        RestResponse<T> restResponse = parseResult(jsonString, clazz);
        checkErrors(restResponse, status, POST, path);
        return restResponse;
    }

    private <T> RestResponse<T> parseResult(String json, Class<T> clazz) throws ClientException {
        if (json != null && !json.isEmpty()) {
            ObjectReader reader = jsonObjectMapper
                    .readerFor(jsonObjectMapper.getTypeFactory().constructParametrizedType(RestResponse.class, DataResult.class, clazz));
            try {
                return reader.readValue(json);
            } catch (JsonParseException e) {
                if (json.startsWith("<html>")) {
                    if (json.contains("504 Gateway Time-out")) {
                        return new RestResponse<>("", 0, Collections.singletonList(new Event(Event.Type.ERROR, 504, "Gateway time-out",
                                "The server didn't respond in time.")), null, Collections.emptyList());
                    }
                }
                throw new ClientException(e.getMessage(), e);
            } catch (JsonProcessingException e) {
                throw new ClientException(e.getMessage(), e);
            }
        } else {
            return new RestResponse<>();
        }
    }

    private <T> void checkErrors(RestResponse<T> restResponse, int status, String method, WebTarget path) throws ClientException {
        if (status / 100 == 2) {
            // REST call succeed
            return;
        }
        URL url;
        try {
            url = path.getUri().toURL();
        } catch (MalformedURLException e) {
            throw new ClientException(e.getMessage(), e);
        }
        logger.debug(method + " URL: {}", url);
        if (restResponse != null && restResponse.getEvents() != null) {
            for (Event event : restResponse.getEvents()) {
                if (Event.Type.ERROR.equals(event.getType())) {
                    logger.debug("Error '{}' on {} {}", event.getMessage(), method, url);
                    if (throwExceptionOnError) {
                        throw new ClientException(event.getMessage());
                    }
                }
            }
        }
    }

    public AbstractParentClient setToken(String token) {
        this.token = token;
        return this;
    }

    public AbstractParentClient setConfiguration(ClientConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}
