package org.opencb.opencga.server.rest.admin;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.opencb.commons.datastore.core.ObjectMap;
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
import java.util.List;

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
    public Response create(@ApiParam(value = "JSON containing the parameters", required = true) UserCreateParams user) {
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
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Import users or a group of users from LDAP", response = User.class,
            notes = "One of <b>users</b> or <b>group</b> need to be filled in at least. <br>"
                    + "Optionally, <b>study</b> and <b>studyGroup</b> can be passed. If so, a group with name <b>studyGroup</b> will be "
                    + "created in the study <b>study</b> containing the list of users imported. <br>"
                    + "<b>authenticationOriginId</b> will correspond to the authentication origin id defined in the main Catalog "
                    + "configuration. <br>"
                    + "<b>type</b> will be one of 'guest' or 'full'. If not provided, it will be considered 'guest' by default."
    )
    public Response ldapImport(@ApiParam(value = "JSON containing the parameters", required = true) LDAPImportParams ldapParams) {
        try {
            ObjectMap params = new ObjectMap();
            params.putIfNotNull("users", ldapParams.users);
            params.putIfNotNull("group", ldapParams.group);
            params.putIfNotNull("study", ldapParams.study);
            params.putIfNotNull("study-group", ldapParams.studyGroup);
//            params.putIfNotNull("expirationDate", executor.expDate);

            return createOkResponse(catalogManager.getUserManager().importFromExternalAuthOrigin(ldapParams.authenticationOriginId,
                    ldapParams.account, params, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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

    public static class UserCreateParams extends org.opencb.opencga.server.rest.UserWSServer.UserCreatePOST {
        public String account;
    }

    public static class LDAPImportParams {
        public List<String> users;
        public String group;
        public String study;
        public String studyGroup;
        public String account;
        public String authenticationOriginId;
    }

}
