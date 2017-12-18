package org.opencb.opencga.server.rest.admin;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/{apiVersion}/admin/audit")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Admin - Audit", position = 4, description = "Methods to audit")
public class AuditWSServer extends OpenCGAWSServer {


    public AuditWSServer(UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public AuditWSServer(String version, UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "Query the audit database")
    public Response query() {
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group by...")
    public Response groupBy() {
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Get some stats from the audit database")
    public Response stats() {
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/login")
    @ApiOperation(value = "Get an overview of the users that have logged in during a period of time")
    public Response login() {
        return createErrorResponse(new NotImplementedException());
    }

}
