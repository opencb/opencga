package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.core.tools.annotations.ApiParam;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/{apiVersion}/federations")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Federations", description = "Methods for working with Federations")
public class FederationWebService extends EnterpriseOpenCGAWSServer {

    public FederationWebService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    

    /******************************************************************
     * REDIRECT METHODS
     ******************************************************************/
    @POST
    @Path("/redirect")
    @ApiOperation(value = "Redirect a POST call", hidden = true)
    public Response redirectPost(
            @ApiParam(value = "Original URL") @QueryParam("url") String url,
            @ApiParam(value = "JSON containing the POST object") Object body) {
        Map<String, Object> queryParams = uriInfo.getQueryParameters().entrySet().stream()
                .filter(e -> !e.getKey().equals("url"))
                .filter(e -> !e.getKey().equals("method"))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
        try {
            return createResponse(EnterpriseFactory.getEnterpriseFederationManager().redirect(url, queryParams, body, "POST", token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/redirect")
    @ApiOperation(value = "Redirect a GET call", hidden = true)
    public Response redirectGet(
            @ApiParam(value = "Original URL") @QueryParam("url") String url) {
        Map<String, Object> queryParams = uriInfo.getQueryParameters().entrySet().stream()
                .filter(e -> !e.getKey().equals("url"))
                .filter(e -> !e.getKey().equals("method"))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
        try {
            return createResponse(EnterpriseFactory.getEnterpriseFederationManager().redirect(url, queryParams, null, "GET", token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/redirect")
    @ApiOperation(value = "Redirect a DELETE call", hidden = true)
    public Response redirectDelete(
            @ApiParam(value = "Original URL") @QueryParam("url") String url) {
        Map<String, Object> queryParams = uriInfo.getQueryParameters().entrySet().stream()
                .filter(e -> !e.getKey().equals("url"))
                .filter(e -> !e.getKey().equals("method"))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
        try {
            return createResponse(EnterpriseFactory.getEnterpriseFederationManager().redirect(url, queryParams, null, "DELETE", token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    /******************************************************************
     * END REDIRECT METHODS
     ******************************************************************/



}
