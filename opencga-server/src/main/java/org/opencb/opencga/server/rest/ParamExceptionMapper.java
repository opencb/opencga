package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.Collections;

import static org.opencb.opencga.core.common.JacksonUtils.getExternalOpencgaObjectMapper;

//@Provider
public class ParamExceptionMapper implements ExceptionMapper<WebApplicationException> {

    @Context
    private HttpServletRequest request;

    @Context
    private HttpServletResponse response;

    @Context
    private ResourceInfo resourceInfo;

    @Context
    private UriInfo uriInfo;

    @Override
    public Response toResponse(WebApplicationException exception) {
        ObjectMap params = new ObjectMap();
        for (String key : uriInfo.getQueryParameters().keySet()) {
            params.put(key, uriInfo.getQueryParameters().getFirst(key));
        }

        String apiVersion = OpenCGAWSServer.CURRENT_VERSION;
        if (uriInfo.getPathParameters().containsKey("apiVersion")) {
             apiVersion = uriInfo.getPathParameters().getFirst("apiVersion");
        }

        RestResponse<OpenCGAResult> restResponse = new RestResponse<>(params, Collections.emptyList());
        restResponse.setApiVersion(apiVersion);

        String requestDescription;
        try {
            requestDescription = request.getMethod() + ": " + uriInfo.getAbsolutePath().toString()
                    + ", " + getExternalOpencgaObjectMapper().writeValueAsString(params);
        } catch (JsonProcessingException e) {
            requestDescription = "Error parsing request description: " + e.getMessage();
        }

        Response response = OpenCGAWSServer.createBadRequestResponse(exception.getCause().getMessage(), restResponse);
        OpenCGAWSServer.logResponse(response.getStatusInfo(), restResponse, 0, requestDescription);
        return response;
    }

}