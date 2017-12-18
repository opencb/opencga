package org.opencb.opencga.server.rest.admin;


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/{apiVersion}/admin/database")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Admin - Database", position = 4, description = "Methods for database administrative operations")
public class DatabaseWSServer extends OpenCGAWSServer {

    public DatabaseWSServer(UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public DatabaseWSServer(String version, UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @DELETE
    @Path("/clean")
    @ApiOperation(value = "Clean database from removed entries", notes = "Completely remove all 'removed' entries from the database")
    public Response clean() {
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Get basic database stats")
    public Response stats() {
        return createErrorResponse(new NotImplementedException());
    }

    @POST
    @Path("/jwt")
    @ApiOperation(value = "Change JWT secret key or algorithm")
    public Response jwt() {
        return createErrorResponse(new NotImplementedException());
    }

}
