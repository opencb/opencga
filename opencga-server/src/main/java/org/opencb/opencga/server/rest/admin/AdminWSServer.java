package org.opencb.opencga.server.rest.admin;

import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.MetaDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.PanelManager;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.Group;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.opencb.opencga.server.rest.PanelWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;

@Path("/{apiVersion}/admin")
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
    @ApiOperation(value = "Create a new user", response = User.class, notes = "Account type can only be one of 'GUEST' (default) or 'FULL'")
    public Response create(@ApiParam(value = "JSON containing the parameters", required = true) UserCreateParams user) {
        try {
            if (!user.checkValidParams()) {
                createErrorResponse(new CatalogException("id, name, email or password not present"));
            }

            if (user.type == null) {
                user.type = Account.Type.GUEST;
            }

            DataResult queryResult = catalogManager.getUserManager()
                    .create(user.id, user.name, user.email, user.password, user.organization, null, user.type, token);

            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/users/import")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Import users or a group of users from LDAP or AAD", response = User.class,
            notes = "<b>id</b> contain a list of resource ids (users or applications) or a single group id to be imported and "
                    + "<b>resourceType</b> contains the type of resource present in the 'id' field (USER, GROUP or APPLICATION). <br>"
                    + "Optionally, <b>study</b> and <b>studyGroup</b> can be passed. If so, a group with name <b>studyGroup</b> will be "
                    + "created in the study <b>study</b> containing the list of users imported. <br>"
                    + "<b>authenticationOriginId</b> will correspond to the authentication origin id defined in the main Catalog "
                    + "configuration. <br>"
                    + "<b>type</b> will be one of 'guest' or 'full'. If not provided, it will be considered 'guest' by default."
    )
    public Response remoteImport(@ApiParam(value = "JSON containing the parameters", required = true) RemoteImportParams remoteParams) {
        try {
            if (remoteParams.resourceType == null) {
                throw new CatalogException("Missing mandatory 'resourceType' field.");
            }
            if (ListUtils.isEmpty(remoteParams.id)) {
                throw new CatalogException("Missing mandatory 'id' field.");
            }

            if (remoteParams.resourceType == ResourceType.USER || remoteParams.resourceType == ResourceType.APPLICATION) {
                catalogManager.getUserManager().importRemoteEntities(remoteParams.authenticationOriginId, remoteParams.id,
                        remoteParams.resourceType == ResourceType.APPLICATION, remoteParams.studyGroup, remoteParams.study, token);
            } else if (remoteParams.resourceType == ResourceType.GROUP) {
                if (remoteParams.id.size() > 1) {
                    throw new CatalogException("More than one group found in 'id'. Only one group is accepted at a time");
                }

                catalogManager.getUserManager().importRemoteGroupOfUsers(remoteParams.authenticationOriginId, remoteParams.id.get(0),
                        remoteParams.studyGroup, remoteParams.study, false, token);
            } else {
                throw new CatalogException("Unknown resourceType '" + remoteParams.resourceType + "'");
            }

            return createOkResponse("OK");
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
                    + "synchronised with any other group.</li>"
                    + "</ul>"
    )
    public Response externalSync(@ApiParam(value = "JSON containing the parameters", required = true) SyncParams syncParams) {
        try {
            return createOkResponse(catalogManager.getStudyManager().syncGroupWith(syncParams.study, syncParams.from, syncParams.to,
                    syncParams.authenticationOriginId, syncParams.force, token));
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
            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(
            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
            @QueryParam("fields") String fields,
            @ApiParam(value = "Entity to be grouped by.", required = true) @QueryParam("entity") Enums.Resource resource,
            @ApiParam(value = "Action performed") @DefaultValue("") @QueryParam("action") String action,
            @ApiParam(value = "Object before update") @DefaultValue("") @QueryParam("before") String before,
            @ApiParam(value = "Object after update") @DefaultValue("") @QueryParam("after") String after,
            @ApiParam(value = "Date <,<=,>,>=(Format: yyyyMMddHHmmss) and yyyyMMddHHmmss-yyyyMMddHHmmss") @DefaultValue("")
            @QueryParam("date") String date) {
        try {


            return createOkResponse(catalogManager.getAuditManager().groupBy(query, fields, queryOptions, token));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }

    }

    @POST
    @Path("/catalog/indexStats")
    @ApiOperation(value = "Sync Catalog into the Solr")
    public Response syncSolr() {
        try {
            return createOkResponse(catalogManager.getStudyManager().indexCatalogIntoSolr(token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/catalog/install")
    @ApiOperation(value = "Install OpenCGA database", notes = "Creates and initialises the OpenCGA database <br>"
            + "<ul>"
            + "<il><b>secretKey</b>: Secret key needed to authenticate through OpenCGA (JWT)</il><br>"
            + "<il><b>password</b>: Password that will be set to perform future administrative operations over OpenCGA</il><br>"
            + "<il><b>email</b>: Administrator's email address.</il><br>"
            + "<il><b>organization</b>: Administrator's organization.</il><br>"
            + "<ul>")
    public Response install(
            @ApiParam(value = "JSON containing the mandatory parameters", required = true) InstallParams installParams) {
        try {
            catalogManager.installCatalogDB(installParams.secretKey, installParams.password, installParams.email,
                    installParams.organization);
            return createOkResponse(DataResult.empty());
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @POST
    @Path("/catalog/panel")
    @ApiOperation(value = "Handle global panels")
    public Response diseasePanels(
            @ApiParam(value = "Import panels from PanelApp (GEL)", defaultValue = "false") @QueryParam("panelApp") boolean importPanels,
            @ApiParam(value = "Flag indicating to overwrite installed panels in case of an ID conflict", defaultValue = "false")
                @QueryParam("overwrite") boolean overwrite,
            @ApiParam(value = "Comma separated list of global panel ids to delete")
                @QueryParam("delete") String panelsToDelete,
            @ApiParam(value = "Panel parameters to be installed") PanelWSServer.PanelPOST panelPost) {
        try {
            if (importPanels) {
                catalogManager.getPanelManager().importPanelApp(token, overwrite);
            } else if (StringUtils.isEmpty(panelsToDelete)) {
                catalogManager.getPanelManager().create(panelPost.toPanel(), overwrite, token);
            } else {
                String[] panelIds = panelsToDelete.split(",");
                for (String panelId : panelIds) {
                    catalogManager.getPanelManager().delete(panelId, token);
                }
            }

            return createOkResponse(catalogManager.getPanelManager().count(PanelManager.INSTALLATION_PANELS,
                    new Query(), token));
        } catch (Exception e) {
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

    @POST
    @Path("/catalog/jwt")
    @ApiOperation(value = "Change JWT secret key")
    public Response jwt(@ApiParam(value = "JSON containing the parameters", required = true) JWTParams jwtParams) {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(MetaDBAdaptor.SECRET_KEY, jwtParams.secretKey);
        try {
            catalogManager.updateJWTParameters(params, token);
            return createOkResponse(DataResult.empty());
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class UserCreateParams extends org.opencb.opencga.server.rest.UserWSServer.UserCreatePOST {
        public Account.Type type;
    }

    public static class RemoteImportParams {
        public String authenticationOriginId;
        public List<String> id;
        public ResourceType resourceType;
        public String study;
        public String studyGroup;
    }

    public enum ResourceType {
        USER,
        GROUP,
        APPLICATION
    }

    public static class SyncParams {
        public String authenticationOriginId;
        public String from;
        public String to;
        public String study;
        public boolean force = false;
    }

    public static class JWTParams {
        public String secretKey;
    }

    public static class InstallParams {
        public String secretKey;
        public String password;
        public String email;
        public String organization;
    }

}
