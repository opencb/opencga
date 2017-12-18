package org.opencb.opencga.server.rest.admin;


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/{apiVersion}/admin/tools")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Admin - Tools", position = 4, description = "Methods to manage tools")
public class ToolWSServer extends OpenCGAWSServer {

    public ToolWSServer(UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public ToolWSServer(String version, UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/install")
    @ApiOperation(value = "Install a new tool in OpenCGA")
    public Response install() {
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/list")
    @ApiOperation(value = "List the available tools in OpenCGA")
    public Response list() {
        return createErrorResponse(new NotImplementedException());
    }

    @GET
    @Path("/show")
    @ApiOperation(value = "Show one tool manifest")
    public Response show() {
        return createErrorResponse(new NotImplementedException());
    }
}
