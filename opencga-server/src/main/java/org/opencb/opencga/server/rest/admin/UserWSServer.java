package org.opencb.opencga.server.rest.admin;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/{apiVersion}/admin/users")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Admin - Users", position = 4, description = "Methods to manage user accounts")
public class UserWSServer extends OpenCGAWSServer {

    public UserWSServer(UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public UserWSServer(String version, UriInfo uriInfo, HttpServletRequest httpServletRequest, HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create a new user account")
    public Response create() {
        return createErrorResponse(new NotImplementedException());
    }

    @POST
    @Path("/import")
    @ApiOperation(value = "Import users or groups from LDAP")
    public Response ldapImport() {
        return createErrorResponse(new NotImplementedException());
    }

    @POST
    @Path("/sync")
    @ApiOperation(value = "Synchronise groups of users with LDAP groups")
    public Response ldapSync() {
        return createErrorResponse(new NotImplementedException());
    }

//    @POST
//    @Path("/root")
//    @ApiOperation(value = "Root?")
//    public Response root() {
//        return createErrorResponse(new NotImplementedException());
//    }

}
