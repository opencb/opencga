package org.opencb.opencga.server.rest.admin;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;
import java.io.IOException;

@Path("/{apiVersion}/admin/users")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Admin - Users", position = 4, description = "Methods to manage user accounts")
public class UserWSServer extends OpenCGAWSServer {

    public UserWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new user", response = User.class, notes = "Account type can only be one of 'guest' (default) or 'full'")
    public Response create(@ApiParam(value = "JSON containing the parameters", required = true) UserCreatePOST user) {
        try {
            if (!user.checkValidParams()) {
                createErrorResponse(new CatalogException("id, name, email or password not present"));
            }

            if (user.account == null) {
                user.account = Account.GUEST;
            }

            QueryResult queryResult = catalogManager.getUserManager()
                    .create(user.id, user.name, user.email, user.password, user.organization, null, user.account, queryOptions, sessionId);

            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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

    public static class UserCreatePOST extends org.opencb.opencga.server.rest.UserWSServer.UserCreatePOST {
        public String account;
    }

}
