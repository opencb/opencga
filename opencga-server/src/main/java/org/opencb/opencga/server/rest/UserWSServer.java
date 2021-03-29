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

package org.opencb.opencga.server.rest;

import io.swagger.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;


@Path("/{apiVersion}/users")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Users", description = "Methods for working with 'users' endpoint")
public class UserWSServer extends OpenCGAWSServer {


    public UserWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new user", response = User.class)
    public Response createUserPost(@ApiParam(value = "JSON containing the parameters", required = true) UserCreateParams user) {
        try {
            ObjectUtils.defaultIfNull(user, new UserCreateParams());

            if (!user.checkValidParams()) {
                createErrorResponse(new CatalogException("id, name, email or password not present"));
            }

            OpenCGAResult<User> queryResult = catalogManager.getUserManager()
                    .create(user.getId(), user.getName(), user.getEmail(), user.getPassword(), user.getOrganization(), null,
                            Account.AccountType.GUEST, null);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{users}/info")
    @ApiOperation(value = "Return the user information including its projects and studies", response = User.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
    })
    public Response getInfo(@ApiParam(value = ParamConstants.USERS_DESCRIPTION, required = true) @PathParam("users") String userIds) {
        try {
            List<String> userList = getIdList(userIds);
            OpenCGAResult<User> result = catalogManager.getUserManager().get(userList, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @POST
    @Path("/{user}/login")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get identified and gain access to the system",
            notes = "Login method is implemented using JSON Web Tokens that use the standard RFC 7519. The provided tokens are not " +
                    "stored by OpenCGA so there is not a logout method anymore. Tokens are provided with an expiration time that, once " +
                    "finished, will no longer be valid.\nIf password is provided it will attempt to login the user. If no password is " +
                    "provided and a valid token is given, a new token will be provided extending the expiration time.",
            response = AuthenticationResponse.class, hidden = true)
    public Response deprecatedLogin(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = "JSON containing the parameter 'password'") LoginParams login) {
        try {
            if (login == null) {
                login = new LoginParams();
            }
            AuthenticationResponse authenticationResponse;
            if (StringUtils.isNotEmpty(login.getPassword())) {
                authenticationResponse = catalogManager.getUserManager().login(userId, login.getPassword());
            } else if (StringUtils.isNotEmpty(this.token)) {
                authenticationResponse = catalogManager.getUserManager().refreshToken(this.token);
            } else {
                throw new Exception("Neither a password nor a token was provided.");
            }

            OpenCGAResult<AuthenticationResponse> response = new OpenCGAResult<>(0, Collections.emptyList(), 1,
                    Collections.singletonList(authenticationResponse), 1);

            return createOkResponse(response);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/login")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get identified and gain access to the system",
            notes = "If user and password are provided it will attempt to authenticate the user. If user and password are not provided and "
                    + " a valid refresh token is provided, it will generate a new access token.", response = AuthenticationResponse.class)
    public Response login(
            @ApiParam(value = "JSON containing the authentication parameters") LoginParams login) {
        try {
            if (login == null) {
                login = new LoginParams();
            }
            AuthenticationResponse authenticationResponse;
            if (StringUtils.isNotEmpty(login.getPassword()) && StringUtils.isNotEmpty(login.getUser())) {
                if (StringUtils.isNotEmpty(login.getRefreshToken())) {
                    throw new Exception("Only 'user' and 'password' fields or 'refreshToken' field are allowed at the same time");
                }
                authenticationResponse = catalogManager.getUserManager().login(login.getUser(), login.getPassword());
            } else if (StringUtils.isNotEmpty(login.getRefreshToken())) {
                if (StringUtils.isNotEmpty(login.getPassword()) || StringUtils.isNotEmpty(login.getUser())) {
                    throw new Exception("Only 'user' and 'password' fields or 'refreshToken' field are allowed at the same time");
                }
                authenticationResponse = catalogManager.getUserManager().refreshToken(login.getRefreshToken());
            } else {
                throw new Exception("Neither 'user' and 'password' for login nor 'refreshToken' for refreshing token were provided.");
            }

            OpenCGAResult<AuthenticationResponse> response = new OpenCGAResult<>(0, Collections.emptyList(), 1,
                    Collections.singletonList(authenticationResponse), 1);

            return createOkResponse(response);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @POST
    @Path("/{user}/password")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Change the password of a user",
            notes = "Only for local users. Not available for users belonging to external authentication origins.", response = User.class,
            hidden = true)
    public Response deprecatedChangePasswordPost(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = "JSON containing the change of password parameters", required = true) PasswordChangeParams params) {
        try {
            catalogManager.getUserManager().changePassword(userId, params.getPassword(), params.getNewPassword());
            return createOkResponse(DataResult.empty());
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/password")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Change the password of a user",
            notes = "Only for local users. Not available for users belonging to external authentication origins.", response = User.class)
    public Response changePassword(
            @ApiParam(value = "JSON containing the change of password parameters", required = true) PasswordChangeParams params) {
        try {
            catalogManager.getUserManager().changePassword(params.getUser(), params.getPassword(), params.getNewPassword());
            return createOkResponse(DataResult.empty());
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/reset-password")
    @ApiOperation(value = "Reset password", hidden = true,
            notes = "Reset the user's password and send a new random one to the e-mail stored in catalog.", response = User.class)
    public Response resetPassword(@ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId) {
        try {
            OpenCGAResult<User> result = catalogManager.getUserManager().resetPassword(userId, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/projects")
    @ApiOperation(value = "Retrieve the projects of the user", notes = "Retrieve the list of projects and studies belonging to the user"
            + " performing the query. This will not fetch shared projects. To get those, please use /projects/search web service.",
            response = Project.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query")
    })
    public Response getAllProjects(@ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId) {
        try {
            ParamUtils.checkIsSingleID(userId);
            query.remove("user");
            query.put(ProjectDBAdaptor.QueryParams.USER_ID.key(), userId);
            return createOkResponse(catalogManager.getProjectManager().get(query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes", response = User.class)
    public Response updateByPost(@ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
                                 @ApiParam(value = "JSON containing the params to be updated.", required = true)
                                         UserUpdateParams parameters) {
        try {
            ObjectUtils.defaultIfNull(parameters, new UserUpdateParams());

            ObjectMap params = new ObjectMap(getUpdateObjectMapper().writeValueAsString(parameters));
            OpenCGAResult<User> result = catalogManager.getUserManager().update(userId, params, null, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/configs/update")
    @ApiOperation(value = "Add or remove a custom user configuration", response = Map.class,
            notes = "Some applications might want to store some configuration parameters containing the preferences of the user. "
                    + "The aim of this is to provide a place to store this things for every user.")
    public Response updateConfiguration(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = "Action to be performed: ADD or REMOVE a group", allowableValues = "ADD,REMOVE", defaultValue = "ADD")
                @QueryParam("action") ParamUtils.AddRemoveAction action,
            @ApiParam(value = "JSON containing anything useful for the application such as user or default preferences. " +
                    "When removing, only the id will be necessary.", required = true) ConfigUpdateParams params) {
        try {
            if (action == null) {
                action = ParamUtils.AddRemoveAction.ADD;
            }
            if (action == ParamUtils.AddRemoveAction.ADD) {
                return createOkResponse(catalogManager.getUserManager().setConfig(userId, params.getId(), params.getConfiguration(), token));
            } else {
                return createOkResponse(catalogManager.getUserManager().deleteConfig(userId, params.getId(), token));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/configs")
    @ApiOperation(value = "Fetch a user configuration", response = Map.class)
    public Response getConfigurations(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = "Unique name (typically the name of the application).") @QueryParam("name") String name) {
        try {
            ParamUtils.checkIsSingleID(userId);
            return createOkResponse(catalogManager.getUserManager().getConfig(userId, name, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/filters/update")
    @ApiOperation(value = "Add or remove a custom user filter", response = UserFilter.class,
            notes = "Users normally try to query the data using the same filters most of the times. The aim of this WS is to allow "
                    + "storing as many different filters as the user might want in order not to type the same filters.")
    public Response updateFilters(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = "Action to be performed: ADD or REMOVE a group", allowableValues = "ADD,REMOVE", defaultValue = "ADD")
                @QueryParam("action") ParamUtils.AddRemoveAction action,
            @ApiParam(value = "Filter parameters. When removing, only the 'name' of the filter will be necessary", required = true) UserFilter params) {
        try {
            if (action == null) {
                action = ParamUtils.AddRemoveAction.ADD;
            }
            if (action == ParamUtils.AddRemoveAction.ADD) {
                return createOkResponse(catalogManager.getUserManager().addFilter(userId, params.getId(), params.getDescription(),
                        params.getResource(), params.getQuery(), params.getOptions(), token));
            } else {
                return createOkResponse(catalogManager.getUserManager().deleteFilter(userId, params.getId(), token));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/filters/{filterId}/update")
    @ApiOperation(value = "Update a custom filter", response = UserFilter.class)
    public Response updateFilterPOST(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = "Filter id", required = true) @PathParam("filterId") String id,
            @ApiParam(value = "Filter parameters", required = true) FilterUpdateParams params) {
        try {
            return createOkResponse(catalogManager.getUserManager().updateFilter(userId, id,
                    new ObjectMap(getUpdateObjectMapper().writeValueAsString(params)), token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/filters")
    @ApiOperation(value = "Fetch user filters", response = UserFilter.class)
    public Response getFilterConfig(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = "Filter id. If provided, it will only fetch the specified filter") @QueryParam("id") String id) {
        try {
            ParamUtils.checkIsSingleID(userId);
            if (StringUtils.isNotEmpty(id)) {
                return createOkResponse(catalogManager.getUserManager().getFilter(userId, id, token));
            } else {
                return createOkResponse(catalogManager.getUserManager().getAllFilters(userId, token));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}