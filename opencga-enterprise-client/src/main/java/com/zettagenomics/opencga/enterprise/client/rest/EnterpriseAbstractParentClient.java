package com.zettagenomics.opencga.enterprise.client.rest;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.response.RestResponse;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public abstract class EnterpriseAbstractParentClient extends AbstractParentClient {

    protected EnterpriseAbstractParentClient(String token, ClientConfiguration clientConfiguration) {
        super(token, clientConfiguration);
    }

    private void addCookies(Invocation.Builder builder) {
        if (clientConfiguration.getAttributes() != null) {
            Object cookies = clientConfiguration.getAttributes().get("cookies");
            if (cookies instanceof Map) {
                Map<String, String> cookiesMap = (Map<String, String>) cookies;
                for (Map.Entry<String, String> entry : cookiesMap.entrySet()) {
                    builder.cookie(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    @Override
    protected <T> RestResponse<T> callRest(WebTarget path, ObjectMap params, Class<T> clazz, String method)
            throws ClientException {
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

//                privateLogger.debug("{} URL: {}", method, path.getUri());
                Invocation.Builder header = path.request().header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token);
                addCookies(header);
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
//                privateLogger.debug("{} URL: {}, Body {}", method, path.getUri(), paramBody);
                Invocation.Builder builder = path.request()
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token);
                addCookies(builder);
                response = builder.post(Entity.json(paramBody));
                break;
            default:
                throw new IllegalArgumentException("Unsupported REST method " + method);
        }
        RestResponse<T> restResponse = parseResult(response, clazz);
        checkErrors(restResponse, response.getStatusInfo(), method, path);
        return restResponse;
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
    @Override
    protected <T> RestResponse<T> callUploadRest(WebTarget path, Map<String, Object> params, Class<T> clazz) throws ClientException {
        String filePath = ((String) params.get("file"));
        params.remove("file");
        params.remove("body");

        path.property(ClientProperties.READ_TIMEOUT, 5400000);
        client.property(ClientProperties.READ_TIMEOUT, 5400000);
        path.register(MultiPartFeature.class);
        path.property(ClientProperties.REQUEST_ENTITY_PROCESSING,
                RequestEntityProcessing.CHUNKED);
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

        RestResponse<T> restResponse = null;
        if (response.getStatus() == 302){
            restResponse = parseResult(response, clazz);
        }

        try {
            formDataMultiPart.close();
            multipart.close();
        } catch (IOException e) {
            throw new ClientException(e.getMessage(), e);
        }

        checkErrors(restResponse, response.getStatusInfo(), POST, path);
        return restResponse;
    }

    public EnterpriseAbstractParentClient setEnterpriseConfiguration(EnterpriseConfiguration enterpriseConfiguration) {
        return this;
    }


}
