package org.opencb.opencga.server;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.User;
import org.opencb.opencga.catalog.core.db.CatalogManagerException;
import org.opencb.opencga.catalog.core.io.CatalogIOManagerException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/users")
@Api(value = "users", description = "users")
public class UserWSServer extends OpenCGAWSServer {

    public UserWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @Produces("text/plain")
    @ApiOperation(value = "Just to create the api")

    public Response createUser(
            @ApiParam(value = "userId", required = true) @QueryParam("userId") String userId,
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
            @ApiParam(value = "email", required = true) @QueryParam("email") String email,
            @ApiParam(value = "organization", required = true) @QueryParam("organization") String organization,
            @ApiParam(value = "role", required = true) @QueryParam("role") String role,
            @ApiParam(value = "password", required = true) @QueryParam("password") String password,
            @ApiParam(value = "status", required = true) @QueryParam("status") String status) {
        User user = new User(userId, name, email, password, organization, role, status);
        QueryResult queryResult;
        try {
            queryResult = catalogManager.createUser(user);
            return createOkResponse(queryResult);

        } catch (CatalogManagerException | CatalogIOManagerException | JsonProcessingException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }

    @GET
    @Path("/{userId}/login")
    @Produces("text/plain")
    @ApiOperation(value = "User login")

    public Response login(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "password", required = false) @QueryParam("password") String password){
        QueryResult queryResult;
        try {
            if (userId.toLowerCase().equals("anonymous")) {
                queryResult = catalogManager.loginAsAnonymous(sessionIp);
            }
            else{
                queryResult = catalogManager.login(userId, password, sessionIp);
            }
            return createOkResponse(queryResult);
        } catch (CatalogManagerException | IOException | CatalogIOManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{userId}/logout")
    @Produces("text/plain")
    @ApiOperation(value = "User login")
    public Response logout(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId
            ) throws IOException {
        try {
            QueryResult result;
            if (userId.toLowerCase().equals("anonymous")) {
                result = catalogManager.logoutAnonymous(sessionId);
            } else {
                result = catalogManager.logout(userId, sessionId);
            }
            return createOkResponse(result);
        } catch (CatalogManagerException | IOException | CatalogIOManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }
    @GET
    @Path("/{userId}/info")
    @Produces("text/plain")
    @ApiOperation(value = "User info")
    public Response getInfo(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "lastActivity", required = false) @QueryParam("lastActivity") String lastActivity) throws IOException {
        try {
            QueryResult result = catalogManager.getUser(userId, lastActivity, sessionId);
            return createOkResponse(result);
        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}