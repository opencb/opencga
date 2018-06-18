/*
 * Copyright 2015-2017 OpenCB
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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Account;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.User;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;


@Path("/{apiVersion}/users")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Users", position = 1, description = "Methods for working with 'users' endpoint")
public class UserWSServer extends OpenCGAWSServer {


    public UserWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new user", response = User.class)
    public Response createUserPost(@ApiParam(value = "JSON containing the parameters", required = true) UserCreatePOST user) {
        try {
            ObjectUtils.defaultIfNull(user, new UserCreatePOST());

            if (!user.checkValidParams()) {
                createErrorResponse(new CatalogException("id, name, email or password not present"));
            }

            QueryResult queryResult = catalogManager.getUserManager()
                    .create(user.id, user.name, user.email, user.password, user.organization, null, Account.FULL, queryOptions, null);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/info")
    @ApiOperation(value = "Return the user information including its projects and studies", response = User.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
    })
    public Response getInfo(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                            @ApiParam(value = "This parameter shows the last time the user information was modified. When "
                                    + "the value passed corresponds with the user's last activity registered, an empty result will be "
                                    + "returned meaning that the client already has the most up to date user information.", hidden = true)
                            @QueryParam("lastModified") String lastModified) {
        try {
            isSingleId(userId);
            QueryResult result = catalogManager.getUserManager().get(userId, lastModified, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/login")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get identified and gain access to the system",
            notes = "Login method is implemented using JSON Web Tokens that use the standard RFC 7519. The provided tokens are not " +
                    "stored by OpenCGA so there is not a logout method anymore. Tokens are provided with an expiration time that, once " +
                    "finished, will no longer be valid.\nIf password is provided it will attempt to login the user. If no password is " +
                    "provided and a valid token is given, a new token will be provided extending the expiration time.")
    public Response loginPost(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                              @ApiParam(value = "JSON containing the parameter 'password'") Map<String, String> map) {
        try {
            ObjectUtils.defaultIfNull(map, new HashMap<>());

            String token;
            if (map.containsKey("password")) {
                String password = map.get("password");
                token = catalogManager.getUserManager().login(userId, password);
            } else if (StringUtils.isNotEmpty(sessionId)) {
                token = catalogManager.getUserManager().refreshToken(userId, sessionId);
            } else {
                throw new Exception("Neither a password nor a token was provided.");
            }

            ObjectMap sessionMap = new ObjectMap()
                    .append("sessionId", token)
                    .append("id", token)
                    .append("token", token);

            QueryResult<ObjectMap> login = new QueryResult<>("You successfully logged in", 0, 1, 1,
                    "'sessionId' and 'id' deprecated", "", Arrays.asList(sessionMap));

            return createOkResponse(login);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/password")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Change the password of a user", notes = "It doesn't work if the user is authenticated against LDAP.")
    public Response changePasswordPost(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                                       @ApiParam(value = "JSON containing the params 'password' (old password) and 'npassword' (new "
                                               + "password)", required = true) ObjectMap params) {
        try {
            ObjectUtils.defaultIfNull(params, new ObjectMap());

            if (!params.containsKey("password") || !params.containsKey("npassword")) {
                throw new Exception("The json must contain the keys password and npassword.");
            }
            String password = params.getString("password");
            String nPassword = params.getString("npassword");
            catalogManager.getUserManager().changePassword(userId, password, nPassword);
            QueryResult result = new QueryResult("changePassword", 0, 0, 0, "", "", Collections.emptyList());
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/reset-password")
    @ApiOperation(value = "Reset password", hidden = true,
            notes = "Reset the user's password and send a new random one to the e-mail stored in catalog.")
    public Response resetPassword(@ApiParam(value = "User id", required = true) @PathParam("user") String userId) {
        try {
            QueryResult result = catalogManager.getUserManager().resetPassword(userId, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/projects")
    @ApiOperation(value = "Retrieve the projects of the user", notes = "Retrieve the list of projects and studies belonging to the user",
            response = Project[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to be skipped.", dataType = "integer", paramType = "query")
    })
    public Response getAllProjects(@ApiParam(value = "User id", required = true) @PathParam("user") String userId) {
        try {
            isSingleId(userId);
            query.remove("user");
            query.put(ProjectDBAdaptor.QueryParams.USER_ID.key(), userId);
            return createOkResponse(catalogManager.getProjectManager().get(query, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes", position = 9, response = User.class)
    public Response updateByPost(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                                 @ApiParam(name = "params", value = "JSON containing the params to be updated.", required = true)
                                         UserUpdatePOST parameters) {
        try {
            ObjectUtils.defaultIfNull(parameters, new UserUpdatePOST());

            ObjectMap params = new ObjectMap(jsonObjectMapper.writeValueAsString(parameters));
            QueryResult result = catalogManager.getUserManager().update(userId, params, null, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/delete")
    @ApiOperation(value = "Delete a user [WARNING]",
            notes = "Usage of this webservice might lead to unexpected behaviour and therefore is discouraged to use. Deletes are " +
                    "planned to be fully implemented and tested in version 1.4.0")
    public Response delete(@ApiParam(value = "Comma separated list of user ids", required = true) @PathParam("user") String userId) {
        try {
            List<QueryResult<User>> deletedUsers = catalogManager.getUserManager().delete(userId, queryOptions, sessionId);
            return createOkResponse(deletedUsers);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/configs/create")
    @ApiOperation(value = "Store a user configuration", notes = "Some applications might want to store some configuration parameters "
            + "containing the preferences of the user. The aim of this is to provide a place to store this things for every user.",
            response = Map.class)
    public Response setConfigurationPOST(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                                         @ApiParam(value = "Unique name (typically the name of the application)", required = true) @QueryParam("name")
                                                 String name,
                                         @ApiParam(name = "params", value = "JSON containing anything useful for the application such as user or default "
                                                 + "preferences", required = true) ObjectMap params) {
        try {
            ObjectUtils.defaultIfNull(params, new ObjectMap());

            return createOkResponse(catalogManager.getUserManager().setConfig(userId, name, params, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/configs/{name}/delete")
    @ApiOperation(value = "Delete a user configuration", response = Map.class)
    public Response deleteConfiguration(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                                        @ApiParam(value = "Unique name (typically the name of the application)", required = true)
                                        @PathParam("name") String name) {
        try {
            return createOkResponse(catalogManager.getUserManager().deleteConfig(userId, name, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/configs/{name}/info")
    @ApiOperation(value = "Fetch a user configuration [DEPRECATED]", response = Map.class,
            notes = "This webservice is deprecated. Users should use /{user}/configs webservice instead.")
    public Response getConfiguration(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                                     @ApiParam(value = "Unique name (typically the name of the application)", required = true)
                                     @PathParam("name") String name) {
        try {
            areSingleIds(userId, name);
            return createOkResponse(catalogManager.getUserManager().getConfig(userId, name, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/configs")
    @ApiOperation(value = "Fetch a user configuration", response = Map.class)
    public Response getConfigurations(
            @ApiParam(value = "User id", required = true) @PathParam("user") String userId,
            @ApiParam(value = "Unique name (typically the name of the application).", required = true) @QueryParam("name") String name) {
        try {
            areSingleIds(userId, name);
            return createOkResponse(catalogManager.getUserManager().getConfig(userId, name, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/configs/filters/create")
    @ApiOperation(value = "Store a custom filter", notes = "Users normally try to query the data using the same filters most of "
            + "the times. The aim of this WS is to allow storing as many different filters as the user might want in order not to type "
            + "the same filters.", response = User.Filter.class)
    public Response addFilterPOST(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                                  @ApiParam(name = "params", value = "Filter parameters", required = true) User.Filter params) {
        try {
            ObjectUtils.defaultIfNull(params, new User.Filter());

            return createOkResponse(catalogManager.getUserManager().addFilter(userId, params.getName(), params.getDescription(),
                    params.getBioformat(), params.getQuery(), params.getOptions(), sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class UpdateFilter {
        public File.Bioformat bioformat;
        public String description;
        public Query query;
        public QueryOptions options;
    }

    @POST
    @Path("/{user}/configs/filters/{name}/update")
    @ApiOperation(value = "Update a custom filter", response = User.Filter.class)
    public Response updateFilterPOST(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                                     @ApiParam(value = "Filter name", required = true) @PathParam("name") String name,
                                     @ApiParam(name = "params", value = "Filter parameters", required = true) UpdateFilter params) {
        try {
            ObjectUtils.defaultIfNull(params, new UpdateFilter());

            return createOkResponse(catalogManager.getUserManager().updateFilter(userId, name,
                    new ObjectMap(jsonObjectMapper.writeValueAsString(params)), sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/configs/filters/{name}/delete")
    @ApiOperation(value = "Delete a custom filter", response = User.Filter.class)
    public Response deleteFilter(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                                 @ApiParam(value = "Filter name", required = true) @PathParam("name") String name) {
        try {
            return createOkResponse(catalogManager.getUserManager().deleteFilter(userId, name, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/configs/filters/{name}/info")
    @ApiOperation(value = "Fetch a filter [DEPRECATED]", response = User.Filter.class,
            notes = "This webservice is deprecated. Users should use /{user}/configs/filters webservice instead.")
    public Response getFilter(@ApiParam(value = "User id", required = true) @PathParam("user") String userId,
                              @ApiParam(value = "Filter name", required = true) @PathParam("name") String name) {
        try {
            return createOkResponse(catalogManager.getUserManager().getFilter(userId, name, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/configs/filters/list")
    @ApiOperation(value = "Fetch all the filters of a user [DEPRECATED]", response = User.Filter.class,
            notes = "This webservice is deprecated. Users should use /{user}/configs/filters webservice instead.")
    public Response getFilters(@ApiParam(value = "User id", required = true) @PathParam("user") String userId) {
        try {
            return createOkResponse(catalogManager.getUserManager().getAllFilters(userId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/configs/filters")
    @ApiOperation(value = "Fetch user filters", response = User.Filter.class)
    public Response getFilterConfig(
            @ApiParam(value = "User id", required = true) @PathParam("user") String userId,
            @ApiParam(value = "Filter name. If provided, it will only fetch the specified filter") @QueryParam("name") String name) {
        try {
            isSingleId(userId);
            if (StringUtils.isNotEmpty(name)) {
                return createOkResponse(catalogManager.getUserManager().getFilter(userId, name, sessionId));
            } else {
                return createOkResponse(catalogManager.getUserManager().getAllFilters(userId, sessionId));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    protected static class UserUpdatePOST {
        public String name;
        public String email;
        public String organization;
        public Map<String, Object> attributes;
    }

    public static class UserCreatePOST {
        @Deprecated
        public String userId;
        @JsonProperty(required = true)
        public String id;
        @JsonProperty(required = true)
        public String name;
        @JsonProperty(required = true)
        public String email;
        @JsonProperty(required = true)
        public String password;
        public String organization;

        public boolean checkValidParams() {
            if (StringUtils.isNotEmpty(userId) && StringUtils.isEmpty(id)) {
                id = userId;
            }
            if (StringUtils.isEmpty("id") || StringUtils.isEmpty("name") || StringUtils.isEmpty("email")
                    || StringUtils.isEmpty("password")) {
                return false;
            }
            return true;
        }
    }

}