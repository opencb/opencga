 /*
 * Copyright 2015-2020 OpenCB
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
 import org.apache.commons.collections4.CollectionUtils;
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
 import org.opencb.opencga.core.common.JacksonUtils;
 import org.opencb.opencga.core.response.RestResponse;
 import org.opencb.opencga.core.response.VariantQueryResult;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import javax.net.ssl.*;
 import javax.ws.rs.client.*;
 import javax.ws.rs.core.HttpHeaders;
 import javax.ws.rs.core.Response;
 import javax.ws.rs.ext.ContextResolver;
 import java.io.File;
 import java.io.FileOutputStream;
 import java.io.IOException;
 import java.io.InputStream;
 import java.net.URI;
 import java.nio.channels.Channels;
 import java.nio.channels.ReadableByteChannel;
 import java.nio.file.Files;
 import java.nio.file.Paths;
 import java.security.KeyManagementException;
 import java.security.NoSuchAlgorithmException;
 import java.security.SecureRandom;
 import java.security.cert.X509Certificate;
 import java.util.*;

/**
 * Created by imedina on 04/05/16.
 */
public abstract class AbstractParentClient {

    protected final Client client;

    private String token;
    private final ClientConfiguration configuration;
    private boolean throwExceptionOnError = false;

    protected final ObjectMapper jsonObjectMapper;

    private int timeout = 90000;
    private int batchSize = 2000;
    private int defaultLimit = 2000;
    private static final int DEFAULT_SKIP = 0;
    protected static final String GET = "GET";
    protected static final String POST = "POST";
    protected static final String DELETE = "DELETE";

    protected Logger logger;

    protected AbstractParentClient(String token, ClientConfiguration configuration) {
        Objects.requireNonNull(configuration);
        Objects.requireNonNull(configuration.getRest());
        this.logger = LoggerFactory.getLogger(this.getClass().toString());
        this.token = token;
        this.configuration = configuration;
        this.client = newClient(configuration);

        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        if (configuration.getRest().getTimeout() > 0) {
            timeout = configuration.getRest().getTimeout();
        }
        if (configuration.getRest().getQuery().getBatchSize() > 0) {
            batchSize = configuration.getRest().getQuery().getBatchSize();
        }
        if (configuration.getRest().getQuery().getLimit() > 0) {
            defaultLimit = configuration.getRest().getQuery().getLimit();
        }

    }

    private Client newClient(ClientConfiguration configuration) {
        ClientBuilder clientBuilder = ClientBuilder.newBuilder();
        clientBuilder.register(JacksonUtils.ObjectMapperProvider.class);

        if (configuration.getRest().isTlsAllowInvalidCertificates()) {
            logger.debug("Using custom SSLContext to allow invalid certificates");
            try {
                TrustManager[] trustAllCerts = new TrustManager[] {
                        new X509TrustManager() {
                            @Override
                            public X509Certificate[] getAcceptedIssuers() {
                                return new X509Certificate[0];
                            }

                            @Override
                            public void checkClientTrusted(X509Certificate[] certs, String authType) {
                            }

                            @Override
                            public void checkServerTrusted(X509Certificate[] certs, String authType) {
                            }
                        },
                };

                SSLContext sc = SSLContext.getInstance("TLS");
                sc.init(null, trustAllCerts, new SecureRandom());

                HostnameVerifier verifier = new HostnameVerifier() {
                    private String hostname = URI.create(configuration.getRest().getHost()).getHost();

                    @Override
                    public boolean verify(String hostname, SSLSession sslSession) {
                        logger.debug("Verify hostname = " + hostname);
                        return this.hostname.equals(hostname);
                    }
                };
                clientBuilder
                        .sslContext(sc)
                        .hostnameVerifier(verifier);
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                throw new RuntimeException(e);
            }
        }
        return clientBuilder.build();
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
        List<String> ids;
        if (StringUtils.isNotEmpty(id1)) {
            ids = Arrays.asList(id1.split(","));
        } else {
            ids = Collections.emptyList();
        }
        return execute(category1, ids, category2, id2, action, paramsMap, method, clazz);
    }

    private <T> RestResponse<T> execute(String category1, List<String> id1, String category2, String id2, String action,
                                          Map<String, Object> paramsMap, String method, Class<T> clazz) throws ClientException {
        ObjectMap params;
        if (paramsMap == null) {
            params = new ObjectMap();
        } else {
            params = new ObjectMap(paramsMap);
        }
        params.put(QueryOptions.TIMEOUT, timeout);

        client.property(ClientProperties.CONNECT_TIMEOUT, 1000);
        client.property(ClientProperties.READ_TIMEOUT, timeout);

        int skip;
        int limit;
        int batchSize;
        if (CollectionUtils.isEmpty(id1)) {
            skip = params.getInt(QueryOptions.SKIP, DEFAULT_SKIP);
            limit = params.getInt(QueryOptions.LIMIT, defaultLimit);
            batchSize = this.batchSize;

            if (limit == 0) {
                limit = defaultLimit;
            }
        } else {
            // Ignore input SKIP and LIMIT from Params
            skip = 0;
            limit = id1.size();
            // Hardcoded OpenCGA IDs limit
            // See org.opencb.opencga.server.rest.OpenCGAWSServer.MAX_ID_SIZE
            batchSize = 100;

            params.remove(QueryOptions.SKIP);
            params.remove(QueryOptions.LIMIT);
        }

        RestResponse<T> finalRestResponse = null;
        int finalNumResults = 0;
        int batchNumResults;
        // Call REST in batches
        do {
            // Update the batch limit
            int batchLimit = Math.min(batchSize, limit - finalNumResults);

            // Build URL
            WebTarget path = client
                    .target(configuration.getRest().getHost())
                    .path("webservices")
                    .path("rest")
                    .path("v2")
                    .path(category1);

            // Select batch. Either by ID or with limit/skip
            if (CollectionUtils.isNotEmpty(id1)) {
                // Select batch of IDs
                path = path.path(String.join(",", id1.subList(skip, skip + batchLimit)));
                // FIXME: This should not be needed!
                params.put(QueryOptions.LIMIT, batchLimit);
            } else {
                // Select batch with skip/limit
                params.put(QueryOptions.SKIP, skip);
                params.put(QueryOptions.LIMIT, batchLimit);
            }
            if (StringUtils.isNotEmpty(category2)) {
                path = path.path(category2);
            }
            if (StringUtils.isNotEmpty(id2)) {
                path = path.path(id2);
            }
            path = path.path(action);

            // Call REST
            RestResponse<T> batchRestResponse = callRest(path, params, clazz, method, action);
            batchNumResults = batchRestResponse.allResultsSize();

            if (finalRestResponse == null) {
                finalRestResponse = batchRestResponse;
            } else {
                // Merge results
                if (batchNumResults > 0) {
                    finalRestResponse.first().getResults().addAll(batchRestResponse.getResponses().get(0).getResults());
                    finalRestResponse.first().setNumResults(finalRestResponse.first().getResults().size());
                }
                finalRestResponse.getEvents().addAll(batchRestResponse.getEvents());
                finalRestResponse.first().getEvents().addAll(batchRestResponse.first().getEvents());
            }

            skip += batchNumResults;
            finalNumResults += batchNumResults;
        } while (batchNumResults >= batchSize && finalNumResults < limit);
        return finalRestResponse;
    }

    private <T> RestResponse<T> callRest(WebTarget path, ObjectMap params, Class<T> clazz, String method, String action)
            throws ClientException {
        RestResponse<T> batchRestResponse;
        switch (action) {
            case "upload":
                batchRestResponse = callUploadRest(path, params, clazz);
                break;
            case "download":
                String destinyPath = params.getString("OPENCGA_DESTINY");
                params.remove("OPENCGA_DESTINY");
                callRestDownload(path, params, destinyPath);
                batchRestResponse = new RestResponse<>();
                break;
            default:
                batchRestResponse = callRest(path, params, clazz, method);
                break;
        }
        return batchRestResponse;
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

                logger.debug("{} URL: {}", method, path.getUri());
                Invocation.Builder header = path.request().header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token);
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

                Object paramBody = (params != null && params.get("body") != null) ? params.get("body") : "";
                logger.debug("{} URL: {}, Body {}", method, path.getUri(), paramBody);
                response = path.request()
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token)
                        .post(Entity.json(paramBody));
                break;
            default:
                throw new IllegalArgumentException("Unsupported REST method " + method);
        }
        RestResponse<T> restResponse = parseResult(response, clazz);
        checkErrors(restResponse, response.getStatusInfo(), method, path);
        return restResponse;
    }

    /**
     * Call to download WS.
     *
     * @param path   Path of the WS.
     * @param params Params to be passed to the WS.
     * @param outputFilePath Path where the file will be written (downloaded).
     */
    private void callRestDownload(WebTarget path, Map<String, Object> params, String outputFilePath) {
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

        logger.debug(POST + " URL: {}", path.getUri());
        Response response = path.request()
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token)
                .post(Entity.entity(multipart, multipart.getMediaType()));
        RestResponse<T> restResponse = parseResult(response, clazz);

        try {
            formDataMultiPart.close();
            multipart.close();
        } catch (IOException e) {
            throw new ClientException(e.getMessage(), e);
        }

        checkErrors(restResponse, response.getStatusInfo(), POST, path);
        return restResponse;
    }

    private <T> RestResponse<T> parseResult(Response response, Class<T> clazz) throws ClientException {
        String json = response.readEntity(String.class);
        if (StringUtils.isNotEmpty(json) && json.startsWith("<")) {
            return new RestResponse<>("", 0, Collections.singletonList(
                    new Event(Event.Type.ERROR,
                            response.getStatusInfo().getStatusCode(),
                            response.getStatusInfo().getFamily().toString(),
                            response.getStatusInfo().getReasonPhrase())), null, Collections.emptyList());
        } else if (StringUtils.isNotEmpty(json)) {
            ObjectReader reader = jsonObjectMapper
                    .readerFor(jsonObjectMapper.getTypeFactory().constructParametrizedType(RestResponse.class, DataResult.class, clazz));
            try {
                return reader.readValue(json);
            } catch (JsonParseException e) {
                throw new ClientException(e.getMessage(), e);
            } catch (JsonProcessingException e) {
                throw new ClientException(e.getMessage(), e);
            }
        } else {
            return new RestResponse<>();
        }
    }

    private <T> void checkErrors(RestResponse<T> restResponse, Response.StatusType status, String method, WebTarget path)
            throws ClientException {
        // TODO: Check response status
//        if (Response.Status.Family.SUCCESSFUL.equals(status.getFamily())) {
//            // REST call succeed
//            return;
//        }

        if (restResponse != null && restResponse.getEvents() != null) {
            for (Event event : restResponse.getEvents()) {
                if (Event.Type.ERROR.equals(event.getType())) {
                    if (throwExceptionOnError) {
                        logger.error("Server error '{}' on {} {}", event.getMessage(), method, path.getUri());
                        throw new ClientException("Got server error '" + event.getMessage() + "'");
                    } else {
                        logger.debug("Server error '{}' on {} {}", event.getMessage(), method, path.getUri());
                    }
                }
            }
        }
    }

    public AbstractParentClient setToken(String token) {
        this.token = token;
        return this;
    }
}
