package org.opencb.opencga.server.rest.admin;

import io.swagger.annotations.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.Group;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;

@Path("/{apiVersion}/admin/users")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Admin", position = 4, description = "Administrator webservices")
public class AdminWSServer extends OpenCGAWSServer {

    public AdminWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    //******************************** USERS **********************************//

    @POST
    @Path("/users/create")
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
    @Path("/users/import")
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
    @Path("/users/sync")
    @ApiOperation(value = "Synchronise groups of users with LDAP groups", response = Group.class,
        notes = "Mandatory fields: <b>authOriginId</b>, <b>study</b>, <b>from</b> and <b>to</b><br>"
                + "<ul>"
                + "<li><b>authOriginId</b>: Authentication origin id defined in the main Catalog configuration.</li>"
                + "<li><b>study</b>: Study [[user@]project:]study where the group of users will be synced with the LDAP group.</li>"
                + "<li><b>from</b>: LDAP group to be synced with a catalog group.</li>"
                + "<li><b>to</b>: Catalog group that will be synced with the LDAP group.</li>"
                + "<li><b>force</b>: Boolean to force the synchronisation with already existing Catalog groups that are not yet "
                +   "synchronised with any other group.</li>"
                + "</ul>"
    )
    public Response ldapSync(@ApiParam(value = "JSON containing the parameters", required = true) LDAPSyncParams ldapParams) {
        try {
            return createOkResponse(catalogManager.getStudyManager().syncGroupWith(ldapParams.study, ldapParams.from, ldapParams.to,
                    ldapParams.authenticationOriginId, ldapParams.force, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    //******************************** AUDIT **********************************//

    //    @GET
//    @Path("/audit/query")
//    @ApiOperation(value = "Query the audit database")
//    public Response query() {
//        return createErrorResponse(new NotImplementedException());
//    }

    @GET
    @Path("/audit/groupBy")
    @ApiOperation(value = "Group by operation")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(
            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
            @QueryParam("fields") String fields,
            @ApiParam(value = "Resource to be grouped by.", required = true) @QueryParam("resource") AuditRecord.Resource resource,
            @ApiParam(value = "Action performed") @DefaultValue("") @QueryParam("action") String action,
            @ApiParam(value = "Object before update") @DefaultValue("") @QueryParam("before") String before,
            @ApiParam(value = "Object after update") @DefaultValue("") @QueryParam("after") String after,
            @ApiParam(value = "Date <,<=,>,>=(Format: yyyyMMddHHmmss) and yyyyMMddHHmmss-yyyyMMddHHmmss") @DefaultValue("")
            @QueryParam("date") String date) {
        try {


            return createOkResponse(catalogManager.getAuditManager().groupBy(query, fields, queryOptions, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }

    }

    //    @GET
//    @Path("/audit/stats")
//    @ApiOperation(value = "Get some stats from the audit database")
//    public Response stats() {
//        return createErrorResponse(new NotImplementedException());
//    }


    //******************************** TOOLS **********************************//

//    @POST
//    @Path("/tools/install")
//    @ApiOperation(value = "Install a new tool in OpenCGA")
//    public Response install() {
//        return createErrorResponse(new NotImplementedException());
//    }
//
//    @GET
//    @Path("/tools/list")
//    @ApiOperation(value = "List the available tools in OpenCGA")
//    public Response list() {
//        return createErrorResponse(new NotImplementedException());
//    }
//
//    @GET
//    @Path("/tools/show")
//    @ApiOperation(value = "Show one tool manifest")
//    public Response show() {
//        return createErrorResponse(new NotImplementedException());
//    }

    //******************************** DATABASE **********************************//

//    @DELETE
//    @Path("/database/clean")
//    @ApiOperation(value = "Clean database from removed entries", notes = "Completely remove all 'removed' entries from the database")
//    public Response clean() {
//        return createErrorResponse(new NotImplementedException());
//    }
//
//    @GET
//    @Path("/database/stats")
//    @ApiOperation(value = "Get basic database stats")
//    public Response stats() {
//        return createErrorResponse(new NotImplementedException());
//    }
//
//    @POST
//    @Path("/database/jwt")
//    @ApiOperation(value = "Change JWT secret key or algorithm")
//    public Response jwt() {
//        return createErrorResponse(new NotImplementedException());
//    }

    public static class UserCreateParams extends org.opencb.opencga.server.rest.UserWSServer.UserCreatePOST {
        public String account;
    }

    public static class LDAPImportParams {
        public String authenticationOriginId;
        public List<String> users;
        public String group;
        public String study;
        public String studyGroup;
        public String account;
    }

    public static class LDAPSyncParams {
        public String authenticationOriginId;
        public String from;
        public String to;
        public String study;
        public boolean force = false;
    }

}
