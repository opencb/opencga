package org.opencb.opencga.server.rest;

import org.opencb.commons.datastore.core.ObjectMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import static org.opencb.opencga.core.common.JacksonUtils.getExternalOpencgaObjectMapper;

@Provider
public class ParamExceptionMapper implements ExceptionMapper<Exception> {

    @Context
    private HttpServletRequest request;

    @Context
    private HttpServletResponse response;

    @Context
    private ResourceInfo resourceInfo;

    @Context
    private UriInfo uriInfo;

    @Override
    public Response toResponse(Exception exception) {
        ObjectMap params = new ObjectMap();
        for (String key : uriInfo.getQueryParameters().keySet()) {
            params.put(key, uriInfo.getQueryParameters().getFirst(key));
        }

        String apiVersion = OpenCGAWSServer.CURRENT_VERSION;
        if (uriInfo.getPathParameters().containsKey("apiVersion")) {
             apiVersion = uriInfo.getPathParameters().getFirst("apiVersion");
        }

        Object startTimeAttribute = this.request.getSession().getAttribute("startTime");
        int startTime;
        if (startTimeAttribute instanceof Number) {
            startTime = ((Number) startTimeAttribute).intValue();
        } else {
            startTime = ((int) this.request.getSession().getCreationTime());
        }

        Object requestDescriptionAttribute = this.request.getSession().getAttribute("requestDescription");
        String requestDescription;
        if (requestDescriptionAttribute instanceof String) {
            requestDescription = ((String) requestDescriptionAttribute);
        } else {
            try {
                requestDescription = request.getMethod() + ": " + uriInfo.getAbsolutePath().toString()
                        + ", " + getExternalOpencgaObjectMapper().writeValueAsString(params);
            } catch (Exception e) {
                requestDescription = "Error parsing request description: " + e.getMessage();
            }
        }

        return OpenCGAWSServer.createErrorResponse(exception, startTime, apiVersion, requestDescription, params, uriInfo);
    }

}