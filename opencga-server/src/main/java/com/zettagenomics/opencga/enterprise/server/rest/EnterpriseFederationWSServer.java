package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import org.opencb.opencga.core.models.federation.FederationClientUpdateParams;
import org.opencb.opencga.core.models.federation.FederationServerCreateParams;
import org.opencb.opencga.core.models.federation.FederationServerUpdateParams;
import org.opencb.opencga.core.models.federation.FederationUserParams;
import com.zettagenomics.opencga.enterprise.server.commons.EnterpriseParamConstants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.federation.FederationClientParams;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.core.tools.annotations.ApiParam;
import org.opencb.opencga.server.rest.OpenCGAWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

@Path("/{apiVersion}/federations")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Federations", description = "Methods for working with Federations")
public class EnterpriseFederationWSServer extends OpenCGAWSServer {

    public EnterpriseFederationWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                        @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        EnterpriseFactory.init(catalogManager, opencgaHome);
    }

    @POST
    @Path("/server/create")
    @ApiOperation(value = "Share a resource with another XetaBase instance.")
    public Response create(
            @ApiParam(name = "body", value = EnterpriseParamConstants.FEDERATION_CREATE_DESCRIPTION, required = true) FederationServerCreateParams createParams) {
        return run(() -> {
            String url = httpServletRequest.getRequestURL().toString();
            url = url.substring(0, url.indexOf("/webservices"));
            return catalogManager.getFederationManager().createFederation(url, createParams, token);
        });
    }

    @POST
    @Path("/server/{id}/reset")
    @ApiOperation(value = "Reset the credentials of a federation server.")
    public Response reset(
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_RESET_DESCRIPTION, required = true) @PathParam("id") String federationServerId) {
        return run(() -> {
            String url = httpServletRequest.getRequestURL().toString();
            url = url.substring(0, url.indexOf("/webservices"));
            return catalogManager.getFederationManager().reset(url, federationServerId, token);
        });
    }

    @POST
    @Path("/server/{id}/update")
    @ApiOperation(value = "Update some fields from a Federation server.")
    public Response updateServer(
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_SERVER_ID_DESCRIPTION) @PathParam("id") String id,
            @ApiParam(name = "body", value = EnterpriseParamConstants.FEDERATION_UPDATE_SERVER_DESCRIPTION, required = true) FederationServerUpdateParams params
    ) {
        return run(() -> catalogManager.getFederationManager().update(id, params, token));
    }

    @DELETE
    @Path("/server/{id}/delete")
    @ApiOperation(value = "Delete a federation server.")
    public Response deleteServer(
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_SERVER_ID_DESCRIPTION, required = true) @PathParam("id") String id
    ) {
        return run(() -> catalogManager.getFederationManager().deleteFederationServer(id, token));
    }

    @POST
    @Path("/client/connect")
    @ApiOperation(value = "Connect to a shared XetaBase instance.")
    public Response connect(
            @ApiParam(name = "body", value = EnterpriseParamConstants.FEDERATION_CONNECT_DESCRIPTION, required = true) FederationClientParams createParams) {
        return run(() -> catalogManager.getFederationManager().connect(createParams, token));
    }

    @POST
    @Path("/client/{id}/synchronize")
    @ApiOperation(value = "Synchronize data from a known Federation server")
    public Response synchronize(
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_CLIENT_ID_SYNC, required = true) @PathParam("id") String federationClientId) {
        return run(() -> catalogManager.getFederationManager().sync(federationClientId, token));
    }

    @POST
    @Path("/client/{id}/update")
    @ApiOperation(value = "Update some fields from a Federation client.")
    public Response updateClient(
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_CLIENT_ID_DESCRIPTION) @PathParam("id") String id,
            @ApiParam(name = "body", value = EnterpriseParamConstants.FEDERATION_UPDATE_CLIENT_DESCRIPTION, required = true) FederationClientUpdateParams params
    ) {
        return run(() -> catalogManager.getFederationManager().update(id, params, token));
    }

    @POST
    @Path("/client/study/users/update")
    @ApiOperation(value = "Grant/Deny access to federated studies to users.")
    public Response shareFederatedStudy(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_SHARE_ACTION_DESCRIPTION, allowableValues = "ADD,REMOVE", defaultValue = "ADD")
                @QueryParam("action") String action,
            @ApiParam(name = "body", value = EnterpriseParamConstants.FEDERATION_SHARE_USERS_DESCRIPTION, required = true) FederationUserParams params
    ) {
        return run(() -> catalogManager.getFederationManager().shareStudy(studyStr, action, params, token));
    }

    @GET
    @Path("/client/study/users/list")
    @ApiOperation(value = "Show the list of users with access to the federated study.")
    public Response federatedStudyAccess(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr
    ) {
        return run(() -> catalogManager.getFederationManager().showUsers(studyStr, token));
    }

    @DELETE
    @Path("/client/{id}/delete")
    @ApiOperation(value = "Delete a federation client.")
    public Response deleteClient(
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_CLIENT_ID_DESCRIPTION, required = true) @PathParam("id") String id
    ) {
        return run(() -> catalogManager.getFederationManager().deleteFederationClient(id, token));
    }

    @POST
    @Path("/firstConnection")
    @ApiOperation(value = "First connection established with the Federation Server to update the secret key and extend the user " +
            "expiration date.", hidden = true)
    public Response firstConnection() {
        return run(() -> catalogManager.getFederationManager().resetSecurityKey(token));
    }

    /******************************************************************
     * REDIRECT METHODS
     ******************************************************************/
    @POST
    @Path("/redirect")
    @ApiOperation(value = "Redirect a POST call", hidden = true)
    public Response redirectPost(
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_REDIRECT_URL_DESCRIPTION) @QueryParam("url") String url,
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_REDIRECT_BODY_DESCRIPTION) Object body) {
        Map<String, Object> queryParams = uriInfo.getQueryParameters().entrySet().stream()
                .filter(e -> !e.getKey().equals("url"))
                .filter(e -> !e.getKey().equals("method"))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
        try {
            return createResponse(catalogManager.getFederationManager().redirect(url, queryParams, body, "POST", token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/redirect")
    @ApiOperation(value = "Redirect a GET call", hidden = true)
    public Response redirectGet(
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_REDIRECT_URL_DESCRIPTION) @QueryParam("url") String url) {
        Map<String, Object> queryParams = uriInfo.getQueryParameters().entrySet().stream()
                .filter(e -> !e.getKey().equals("url"))
                .filter(e -> !e.getKey().equals("method"))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
        try {
            return createResponse(catalogManager.getFederationManager().redirect(url, queryParams, null, "GET", token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/redirect")
    @ApiOperation(value = "Redirect a DELETE call", hidden = true)
    public Response redirectDelete(
            @ApiParam(value = EnterpriseParamConstants.FEDERATION_REDIRECT_URL_DESCRIPTION) @QueryParam("url") String url) {
        Map<String, Object> queryParams = uriInfo.getQueryParameters().entrySet().stream()
                .filter(e -> !e.getKey().equals("url"))
                .filter(e -> !e.getKey().equals("method"))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
        try {
            return createResponse(catalogManager.getFederationManager().redirect(url, queryParams, null, "DELETE", token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    /******************************************************************
     * END REDIRECT METHODS
     ******************************************************************/


}
