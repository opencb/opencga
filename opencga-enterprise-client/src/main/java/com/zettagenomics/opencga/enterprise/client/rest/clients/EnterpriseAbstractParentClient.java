package com.zettagenomics.opencga.enterprise.client.rest.clients;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.response.RestResponse;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public abstract class EnterpriseAbstractParentClient extends AbstractParentClient {

    private EnterpriseConfiguration enterpriseConfiguration;
    private Map<String, String> ssoCookies;

    protected EnterpriseAbstractParentClient(String token, ClientConfiguration clientConfiguration) {
        super(token, clientConfiguration);
    }

    public Map<String, String> getSsoCookies() {
        return ssoCookies;
    }

    public EnterpriseAbstractParentClient setSsoCookies(Map<String, String> ssoCookies) {
        this.ssoCookies = ssoCookies;
        return this;
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
    private <T> RestResponse<T> callUploadRest(WebTarget path, Map<String, Object> params, Class<T> clazz) throws ClientException {
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

        privateLogger.debug(POST + " URL: {}", path.getUri());
        Response response = path.request()
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + this.token)
                .post(Entity.entity(multipart, multipart.getMediaType()));
        if (response.getStatus() == 302)
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

}
