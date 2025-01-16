package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.federation.FederationClientParams;
import org.opencb.opencga.core.models.federation.FederationServerCreateParams;
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

    @POST
    @Path("/create")
    @ApiOperation(value = "Create a new Federation")
    public Response create(
            @ApiParam(value = "JSON containing the new Federation object") FederationServerCreateParams createParams) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().createFederation(createParams, token));
    }

    @POST
    @Path("/connect")
    @ApiOperation(value = "Connect to a Federation server")
    public Response connect(
            @ApiParam(value = "JSON containing the Federation server configuration") FederationClientParams createParams) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().connect(createParams, token));
    }

    @POST
    @Path("/synchronize")
    @ApiOperation(value = "Synchronize data from a known Federation server")
    public Response synchronize(
            @ApiParam(value = "Federation client id to be synchronized") @QueryParam("id") String federationClientId) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().sync(federationClientId, token));
    }

    @POST
    @Path("/reset")
    @ApiOperation(value = "Reset the credentials of a federation")
    public Response reset(
            @ApiParam(value = "Federation server id to reset") @QueryParam("id") String federationServerId) {
        return null;
//        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().sync(federationClientId, token));
    }

    @POST
    @Path("/login")
    @ApiOperation(value = "Login a federated user")
    public Response login(
            @ApiParam(value = "Federation server id to reset") @QueryParam("id") String federationServerId) {
        return null;
//        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().sync(federationClientId, token));
    }

    @POST
    @Path("/firstConnection")
    @ApiOperation(value = "First connection established with the Federation Server to update the secret key and extend the user " +
            "expiration date.", hidden = true)
    public Response firstConnection() {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().resetSecretKey(token));
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
