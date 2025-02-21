package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import com.zettagenomics.opencga.enterprise.core.models.federation.FederationClientUpdateParams;
import com.zettagenomics.opencga.enterprise.core.models.federation.FederationServerCreateParams;
import com.zettagenomics.opencga.enterprise.core.models.federation.FederationServerUpdateParams;
import com.zettagenomics.opencga.enterprise.core.models.federation.FederationUserParams;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.federation.FederationClientParams;
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
    @Path("/server/create")
    @ApiOperation(value = "Share a resource with another XetaBase instance.")
    public Response create(
            @ApiParam(value = "JSON containing the new Federation object", required = true) FederationServerCreateParams createParams) {
        return run(() -> {
            String url = httpServletRequest.getRequestURL().toString();
            url = url.substring(0, url.indexOf("/webservices"));
            return EnterpriseFactory.getEnterpriseFederationManager().createFederation(url, createParams, token);
        });
    }

    @POST
    @Path("/server/{id}/reset")
    @ApiOperation(value = "Reset the credentials of a federation server.")
    public Response reset(
            @ApiParam(value = "Federation server id to reset", required = true) @PathParam("id") String federationServerId) {
        return run(() -> {
            String url = httpServletRequest.getRequestURL().toString();
            url = url.substring(0, url.indexOf("/webservices"));
            return EnterpriseFactory.getEnterpriseFederationManager().reset(url, federationServerId, token);
        });
    }

    @POST
    @Path("/server/{id}/update")
    @ApiOperation(value = "Update some fields from a Federation server.")
    public Response updateServer(
            @ApiParam(value = "Federation server id") @PathParam("id") String id,
            @ApiParam(value = "JSON containing the Federation server parameters to be updated", required = true) FederationServerUpdateParams params
    ) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().update(id, params, token));
    }

    @DELETE
    @Path("/server/{id}/delete")
    @ApiOperation(value = "Delete a federation server.")
    public Response deleteServer(
            @ApiParam(value = "Federation server id", required = true) @PathParam("id") String id
    ) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().deleteFederationServer(id, token));
    }

    @POST
    @Path("/client/connect")
    @ApiOperation(value = "Connect to a shared XetaBase instance.")
    public Response connect(
            @ApiParam(value = "JSON containing the Federation server configuration", required = true) FederationClientParams createParams) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().connect(createParams, token));
    }

    @POST
    @Path("/client/{id}/synchronize")
    @ApiOperation(value = "Synchronize data from a known Federation server")
    public Response synchronize(
            @ApiParam(value = "Federation client id to be synchronized", required = true) @PathParam("id") String federationClientId) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().sync(federationClientId, token));
    }

    @POST
    @Path("/client/{id}/update")
    @ApiOperation(value = "Update some fields from a Federation client.")
    public Response updateClient(
            @ApiParam(value = "Federation client id") @PathParam("id") String id,
            @ApiParam(value = "JSON containing the Federation client parameters to be updated", required = true) FederationClientUpdateParams params
    ) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().update(id, params, token));
    }

    @POST
    @Path("/client/study/users/update")
    @ApiOperation(value = "Grant/Deny access to federated studies to users.")
    public Response shareFederatedStudy(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Action to be performed: ADD access or REMOVE access.", allowableValues = "ADD,REMOVE", defaultValue = "ADD")
                @QueryParam("action") String action,
            @ApiParam(value = "JSON containing the list of users to which this action will be applied.", required = true) FederationUserParams params
    ) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().shareStudy(studyStr, action, params, token));
    }

    @GET
    @Path("/client/study/users/list")
    @ApiOperation(value = "Show the list of users with access to the federated study.")
    public Response federatedStudyAccess(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr
    ) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().showUsers(studyStr, token));
    }

    @DELETE
    @Path("/client/{id}/delete")
    @ApiOperation(value = "Delete a federation client.")
    public Response deleteClient(
            @ApiParam(value = "Federation client id", required = true) @PathParam("id") String id
    ) {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().deleteFederationClient(id, token));
    }

    @POST
    @Path("/firstConnection")
    @ApiOperation(value = "First connection established with the Federation Server to update the secret key and extend the user " +
            "expiration date.", hidden = true)
    public Response firstConnection() {
        return run(() -> EnterpriseFactory.getEnterpriseFederationManager().resetSecurityKey(token));
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
