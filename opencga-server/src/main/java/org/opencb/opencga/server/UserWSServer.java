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

    public Response echoGet(
            @ApiParam(value = "id", required = true) @QueryParam("id") String id,
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
            @ApiParam(value = "email", required = true) @QueryParam("email") String email,
            @ApiParam(value = "organization", required = true) @QueryParam("organization") String organization,
            @ApiParam(value = "role", required = true) @QueryParam("role") String role,
            @ApiParam(value = "password", required = true) @QueryParam("password") String password,
            @ApiParam(value = "status", required = true) @QueryParam("status") String status
    ) {
        User user = new User(id, name, email, password, organization, role, status);
        QueryResult queryResult;
        try {
            queryResult = catalogManager.createUser(user);
            return createOkResponse(queryResult);

        } catch (CatalogManagerException | CatalogIOManagerException | JsonProcessingException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }
}