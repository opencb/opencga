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
import org.opencb.opencga.core.models.admin.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelCreateParams;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.server.rest.OpenCGAWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Map;

import static org.opencb.opencga.core.models.admin.UserImportParams.ResourceType.*;

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

            if (user.getType() == null) {
                user.setType(Account.Type.GUEST);
            }

            OpenCGAResult<User> queryResult = catalogManager.getUserManager()
                    .create(user.getId(), user.getName(), user.getEmail(), user.getPassword(), user.getOrganization(), null, user.getType(), token);

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
    public Response remoteImport(@ApiParam(value = "JSON containing the parameters", required = true) UserImportParams remoteParams) {
        try {
            if (remoteParams.getResourceType() == null) {
                throw new CatalogException("Missing mandatory 'resourceType' field.");
            }
            if (ListUtils.isEmpty(remoteParams.getId())) {
                throw new CatalogException("Missing mandatory 'id' field.");
            }

            if (remoteParams.getResourceType() == USER || remoteParams.getResourceType() == APPLICATION) {
                catalogManager.getUserManager().importRemoteEntities(remoteParams.getAuthenticationOriginId(), remoteParams.getId(),
                        remoteParams.getResourceType() == APPLICATION, remoteParams.getStudyGroup(), remoteParams.getStudy(), token);
            } else if (remoteParams.getResourceType() == GROUP) {
                if (remoteParams.getId().size() > 1) {
                    throw new CatalogException("More than one group found in 'id'. Only one group is accepted at a time");
                }

                catalogManager.getUserManager().importRemoteGroupOfUsers(remoteParams.getAuthenticationOriginId(), remoteParams.getId().get(0),
                        remoteParams.getStudyGroup(), remoteParams.getStudy(), false, token);
            } else {
                throw new CatalogException("Unknown resourceType '" + remoteParams.getResourceType() + "'");
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
    public Response externalSync(@ApiParam(value = "JSON containing the parameters", required = true) GroupSyncParams syncParams) {
        try {
            return createOkResponse(catalogManager.getStudyManager().syncGroupWith(syncParams.getStudy(), syncParams.getFrom(), syncParams.getTo(),
                    syncParams.getAuthenticationOriginId(), syncParams.isForce(), token));
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
    @ApiOperation(value = "Group by operation", response = Map.class)
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
    @ApiOperation(value = "Sync Catalog into the Solr", response = Boolean.class)
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
            @ApiParam(value = "JSON containing the mandatory parameters", required = true) InstallationParams installParams) {
        try {
            catalogManager.installCatalogDB(installParams.getSecretKey(), installParams.getPassword(), installParams.getEmail(),
                    installParams.getOrganization());
            return createOkResponse(DataResult.empty());
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @POST
    @Path("/catalog/panel")
    @ApiOperation(value = "Handle global panels", response = Panel.class)
    public Response diseasePanels(
            @ApiParam(value = "Import panels from PanelApp (GEL)", defaultValue = "false") @QueryParam("panelApp") boolean importPanels,
            @ApiParam(value = "Flag indicating to overwrite installed panels in case of an ID conflict", defaultValue = "false")
                @QueryParam("overwrite") boolean overwrite,
            @ApiParam(value = "Comma separated list of global panel ids to delete")
                @QueryParam("delete") String panelsToDelete,
            @ApiParam(value = "Panel parameters to be installed") PanelCreateParams panelPost) {
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
        params.putIfNotNull(MetaDBAdaptor.SECRET_KEY, jwtParams.getSecretKey());
        try {
            catalogManager.updateJWTParameters(params, token);
            return createOkResponse(DataResult.empty());
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
