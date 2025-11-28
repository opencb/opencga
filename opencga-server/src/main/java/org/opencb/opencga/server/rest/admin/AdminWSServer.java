/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.server.rest.admin;

import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.resource.ResourceFetcherTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.Acl;
import org.opencb.opencga.core.models.admin.*;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.models.resource.ResourceFetcherToolParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserCreateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.rest.analysis.AnalysisWebService;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;
import static org.opencb.opencga.core.models.admin.UserImportParams.ResourceType.*;

@Path("/{apiVersion}/admin")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Admin", position = 4, description = "Administrator webservices")
public class AdminWSServer extends AnalysisWebService { //OpenCGAWSServer {

    public AdminWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    //******************************** USERS **********************************//

    @GET
    @Path("/organizations/list")
    @ApiOperation(value = "List current Organizations", response = String.class)
    public Response organizationList() {
        try {
            return createOkResponse(catalogManager.getAdminManager().getOrganizationIds(token));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }

    }

    @GET
    @Path("/users/search")
    @ApiOperation(value = "User search method", response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response userSearch(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.USER_DESCRIPTION) @QueryParam(ParamConstants.USER) String user,
            @ApiParam(value = ParamConstants.USER_AUTHENTICATION_ORIGIN_DESCRIPTION) @QueryParam(ParamConstants.USER_AUTHENTICATION_ORIGIN) String authentication) {
        try {
            return createOkResponse(catalogManager.getAdminManager().userSearch(organizationId, query, queryOptions, token));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }

    }

    @POST
    @Path("/users/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new user", response = User.class)
    public Response create(
            @ApiParam(value = "JSON containing the parameters", required = true) UserCreateParams user
    ) {
        try {
            if (!user.checkValidParams()) {
                return createErrorResponse(new CatalogException("id, name, email or password not present"));
            }

            OpenCGAResult<User> queryResult = catalogManager.getUserManager()
                    .create(user.getId(), user.getName(), user.getEmail(), user.getPassword(), user.getOrganization(), null, token);

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
    public Response remoteImport(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = "JSON containing the parameters", required = true) UserImportParams remoteParams
    ) {
        try {
            if (remoteParams.getResourceType() == null) {
                throw new CatalogException("Missing mandatory 'resourceType' field.");
            }
            if (ListUtils.isEmpty(remoteParams.getId())) {
                throw new CatalogException("Missing mandatory 'id' field.");
            }

            if (remoteParams.getResourceType() == USER || remoteParams.getResourceType() == APPLICATION) {
                catalogManager.getUserManager().importRemoteEntities(organizationId, remoteParams.getAuthenticationOriginId(), remoteParams.getId(),
                        remoteParams.getResourceType() == APPLICATION, remoteParams.getStudyGroup(), remoteParams.getStudy(), token);
            } else if (remoteParams.getResourceType() == GROUP) {
                if (remoteParams.getId().size() > 1) {
                    throw new CatalogException("More than one group found in 'id'. Only one group is accepted at a time");
                }

                catalogManager.getUserManager().importRemoteGroupOfUsers(organizationId, remoteParams.getAuthenticationOriginId(), remoteParams.getId().get(0),
                        remoteParams.getStudyGroup(), remoteParams.getStudy(), false, token);
            } else {
                throw new CatalogException("Unknown resourceType '" + remoteParams.getResourceType() + "'");
            }

            return createOkResponse("OK");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/users/permissions")
    @ApiOperation(value = "User permissions", notes = "Effective permissions assigned to the users for a given list of entries.",
            response = Acl.class)
    public Response effectivePermissions(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.ENTRY_ID_LIST_DESCRIPTION) @QueryParam(ParamConstants.ENTRY_ID_LIST) String entryIdList,
            @ApiParam(value = ParamConstants.PERMISSION_LIST_DESCRIPTION) @QueryParam(ParamConstants.PERMISSION_LIST) String permissionList,
            @ApiParam(value = ParamConstants.CATEGORY_DESCRIPTION) @QueryParam(ParamConstants.CATEGORY) String category) {
        try {
            return createOkResponse(catalogManager.getAdminManager().getEffectivePermissions(studyStr, getIdList(entryIdList),
                    getIdListOrEmpty(permissionList), category, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }

    }

    @POST
    @Path("/users/{user}/groups/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Add or remove users from existing groups", response = Group.class)
    public Response updateGroups(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.USER_DESCRIPTION) @PathParam(ParamConstants.USER) String user,
            @ApiParam(value = "Action to be performed: ADD or REMOVE user to/from groups", allowableValues = "ADD,REMOVE",
                    defaultValue = "ADD") @QueryParam("action") ParamUtils.AddRemoveAction action,
            @ApiParam(value = "JSON containing the parameters", required = true) UserUpdateGroup updateParams) {
        try {
            return createOkResponse(catalogManager.getAdminManager().updateGroups(organizationId, user, updateParams.getStudyIds(), updateParams.getGroupIds(), action, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @POST
    @Path("/users/sync")
    @ApiOperation(value = "[DEPRECATED] Moved to /users/sync", response = Group.class,
            notes = "Synchronise a group of users from an authentication origin with a group in a study from catalog.<br>"
                    + "Mandatory fields: <b>authOriginId</b>, <b>study</b><br>"
                    + "<ul>"
                    + "<li><b>authOriginId</b>: Authentication origin id defined in the main Catalog configuration.</li>"
                    + "<li><b>study</b>: Study [[organization@]project:]study where the list of users will be associated to.</li>"
                    + "<li><b>from</b>: Group defined in the authenticated origin to be synchronised.</li>"
                    + "<li><b>to</b>: Group in a study that will be synchronised.</li>"
                    + "<li><b>syncAll</b>: Flag indicating whether to synchronise all the groups present in the study with"
                    + " their corresponding authenticated groups automatically. --from and --to parameters will not be needed when the flag"
                    + "is active..</li>"
                    + "<li><b>type</b>: User account type of the users to be imported (guest or full).</li>"
                    + "<li><b>force</b>: Boolean to force the synchronisation with already existing Catalog groups that are not yet "
                    + "synchronised with any other group.</li>"
                    + "</ul>"
    )
    public Response externalSync(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = "JSON containing the parameters", required = true) DeprecatedGroupSyncParams syncParams
    ) {
        try {
            // TODO: These two methods should return an OpenCGAResult containing at least the number of changes
            if (syncParams.isSyncAll()) {
                catalogManager.getUserManager().syncAllUsersOfExternalGroup(organizationId, syncParams.getStudy(), syncParams.getAuthenticationOriginId(), token);
            } else {
                catalogManager.getUserManager().importRemoteGroupOfUsers(organizationId, syncParams.getAuthenticationOriginId(), syncParams.getFrom(),
                        syncParams.getTo(), syncParams.getStudy(), true, token);
            }
            return createOkResponse(OpenCGAResult.empty(Group.class));
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
    @Path("/catalog/install")
    @ApiOperation(value = "Install OpenCGA database", notes = "Creates and initialises the OpenCGA database <br>"
            + "<ul>"
            + "<il><b>secretKey</b>: Secret key needed to authenticate through OpenCGA (JWT)</il><br>"
            + "<il><b>password</b>: Password that will be set to perform future administrative operations over OpenCGA</il><br>"
            + "<il><b>email</b>: Administrator's email address.</il><br>"
            + "<ul>")
    public Response install(
            @ApiParam(value = "JSON containing the mandatory parameters", required = true) InstallationParams installParams) {
        try {
            catalogManager.installCatalogDB("HS256", installParams.getSecretKey(), installParams.getPassword(), installParams.getEmail(), false);
            return createOkResponse(DataResult.empty());
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
    @Path("/token")
    @ApiOperation(value = "Obtain a valid token for a user", hidden = true)
    public Response token(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = "Token parameters", required = true) TokenParams jwtParams
    ) {
        try {
            String newToken = jwtParams.getExpiration() != null
                    ? catalogManager.getUserManager().getToken(organizationId, jwtParams.getUserId(), jwtParams.getAttributes(), jwtParams.getExpiration(), token)
                    : catalogManager.getUserManager().getNonExpiringToken(organizationId, jwtParams.getUserId(), jwtParams.getAttributes(), token);
            AuthenticationResponse authResponse = new AuthenticationResponse(newToken);
            OpenCGAResult<AuthenticationResponse> opencgaResponse = new OpenCGAResult<>(0, Collections.emptyList(), 1,
                    Collections.singletonList(authResponse), 1);
            return createOkResponse(opencgaResponse);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    //******************************** RESOURCES **********************************//

    @POST
    @Path("/resource/fetch")
    @ApiOperation(value = ResourceFetcherTool.DESCRIPTION, response = Job.class)
    public Response fetchResources(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.JOB_SCHEDULED_START_TIME_DESCRIPTION) @QueryParam(ParamConstants.JOB_SCHEDULED_START_TIME)
                    String scheduledStartTime,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.SUBMIT_JOB_PRIORITY_PARAM)
                    String jobPriority,
            @ApiParam(value = ParamConstants.JOB_DRY_RUN_DESCRIPTION) @QueryParam(ParamConstants.JOB_DRY_RUN) Boolean dryRun,
            @ApiParam(value = ResourceFetcherToolParams.DESCRIPTION, required = true) ResourceFetcherToolParams params) {
        return submitJob(ParamConstants.ADMIN_STUDY_FQN, JobType.NATIVE_TOOL, ResourceFetcherTool.ID, params, jobName, jobDescription, dependsOn,
                jobTags, scheduledStartTime, jobPriority, dryRun);
    }
}
