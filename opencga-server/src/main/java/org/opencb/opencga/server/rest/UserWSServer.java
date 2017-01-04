/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.Session;
import org.opencb.opencga.catalog.models.User;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@Path("/{version}/users")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Users", position = 1, description = "Methods for working with 'users' endpoint")
public class UserWSServer extends OpenCGAWSServer {


    public UserWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @Deprecated
    @ApiOperation(value = "Creates a new user", position = 1, response = User.class)
    public Response createUser(@ApiParam(value = "userId", required = true) @QueryParam("userId") String userId,
                               @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                               @ApiParam(value = "email", required = true) @QueryParam("email") String email,
                               @ApiParam(value = "password", required = true) @QueryParam("password") String password,
                               @ApiParam(value = "organization", required = false) @QueryParam("organization") String organization,
                               @ApiParam(value = "[PENDING] Create a default project after creating the user", required = false, defaultValue = "false")
                                   @QueryParam("createDefaultProject") boolean defaultProject) {
        try {
            queryOptions.remove("password");
            QueryResult queryResult = catalogManager.createUser(userId, name, email, password, organization, null, queryOptions);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/info")
    @ApiOperation(value = "Return the user information including its projects and studies", position = 2, response = User.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response getInfo(@ApiParam(value = "User ID", required = true) @PathParam("user") String userId,
                            @ApiParam(value = "If matches with the user's last activity, return an empty QueryResult") @QueryParam("lastModified") String lastModified) {
        try {
            QueryResult result = catalogManager.getUser(userId, lastModified, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{user}/login")
    @ApiOperation(value = "User login returns a valid session ID token [DEPRECATED]", position = 3)
    public Response login(@ApiParam(value = "userId", required = true) @PathParam("user") String userId,
                          @ApiParam(value = "password", required = true) @QueryParam("password") String password) {
        sessionIp = httpServletRequest.getRemoteAddr();
        QueryResult<Session> queryResult;
        try {
            queryOptions.remove("password"); //Remove password from query options
//            if (userId.equalsIgnoreCase("anonymous")) {
//                queryResult = catalogManager.loginAsAnonymous(sessionIp);
//            } else {
//                queryResult = catalogManager.login(userId, password, sessionIp);
//            }
            queryResult = catalogManager.login(userId, password, sessionIp);
            ObjectMap sessionMap = new ObjectMap();
            sessionMap.append("sessionId", queryResult.first().getId())
                    .append("id", queryResult.first().getId())
                    .append("ip", queryResult.first().getIp())
                    .append("date", queryResult.first().getDate());

            QueryResult<ObjectMap> login = new QueryResult<>("login", queryResult.getDbTime(), 1, 1, queryResult.getWarningMsg(),
                    queryResult.getErrorMsg(), Arrays.asList(sessionMap));

            return createOkResponse(login);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/logout")
    @ApiOperation(value = "User logout method", position = 4)
    public Response logout(@ApiParam(value = "userId", required = true) @PathParam("user") String userId) {
        try {
            QueryResult result;
            if (userId.equalsIgnoreCase("anonymous")) {
                result = catalogManager.logoutAnonymous(sessionId);
            } else {
                result = catalogManager.logout(userId, sessionId);
            }
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{user}/change-password")
    @ApiOperation(value = "User password change [DEPRECATED]", position = 5)
    public Response changePassword(@ApiParam(value = "userId", required = true) @PathParam("user") String userId,
                                   @ApiParam(value = "Old password", required = true) @QueryParam("password") String password,
                                   @ApiParam(value = "New password", required = true) @QueryParam("npassword") String nPassword) {
        try {
            queryOptions.remove("password");
            queryOptions.remove("npassword");
            QueryResult result = catalogManager.changePassword(userId, password, nPassword);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{user}/change-email")
    @ApiOperation(value = "User email change", position = 6, notes = "Deprecated method. Moved to update.")
    public Response changeEmail(@ApiParam(value = "userId", required = true) @PathParam("user") String userId,
                                @ApiParam(value = "New email", required = true) @QueryParam("nemail") String nEmail) {
        try {
            QueryResult result = catalogManager.changeEmail(userId, nEmail, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/reset-password")
    @ApiOperation(value = "Reset password", position = 7, notes = "Reset the user password and send a new one to the e-mail stored in catalog.")
    public Response resetPassword(@ApiParam(value = "userId", required = true) @PathParam("user") String userId) {
        try {
            QueryResult result = catalogManager.resetPassword(userId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{user}/projects")
    @ApiOperation(value = "Return projects", position = 8, notes = "Return all the projects and studies belonging to the user", response = Project[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query")
    })
    public Response getAllProjects(@ApiParam(value = "userId", required = true) @PathParam("user") String userId,
                                   @ApiParam(value = "shared", required = false, defaultValue = "false") @QueryParam("shared") boolean shared) {
        try {
            QueryResult queryResult;
            if (!shared) {
                queryResult = catalogManager.getAllProjects(userId, queryOptions, sessionId);
            } else {
                queryResult = catalogManager.getProjectManager().getSharedProjects(userId, queryOptions, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{user}/update")
    @ApiOperation(value = "Update some user attributes using GET method", position = 9, response = User.class)
    public Response update(@ApiParam(value = "userId", required = true) @PathParam("user") String userId,
                           @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "email", required = false) @QueryParam("email") String email,
                           @ApiParam(value = "organization", required = false) @QueryParam("organization") String organization,
                           @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes) {
        try {
            ObjectMap objectMap = new ObjectMap();
            objectMap.putIfNotNull("name", name);
            objectMap.putIfNotNull("email", email);
            objectMap.putIfNotNull("organization", organization);
            objectMap.putIfNotNull("attributes", attributes);

            QueryResult result = catalogManager.modifyUser(userId, objectMap, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes using POST method", position = 9, response = User.class)
    public Response updateByPost(@ApiParam(value = "userId", required = true) @PathParam("user") String userId,
                                 @ApiParam(name = "params", value = "Parameters to modify", required = true) Map<String, Object> params) {
        try {
            ObjectMap objectMap = new ObjectMap(params);
            QueryResult result = catalogManager.modifyUser(userId, objectMap, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{userId}/delete")
    @ApiOperation(value = "Delete an user [NO TESTED]", position = 10)
    public Response delete(@ApiParam(value = "userIds", required = true) @PathParam("userId") String userId) {
        try {
            List<QueryResult<User>> deletedUsers = catalogManager.getUserManager().delete(userId, queryOptions, sessionId);
            return createOkResponse(deletedUsers);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new user", position = 1, response = User.class)
    public Response createUserPost(@ApiParam(value = "Json containing the params 'userId', 'name', 'email', 'password', 'organization'", required = true) Map<String, String> map) {
        try {
            if (!map.containsKey("userId") || !map.containsKey("name") || !map.containsKey("email") || !map.containsKey("password")) {
                createErrorResponse(new CatalogException("userId, name, email or password not present"));
            }

            String userId = map.get("userId");
            String name = map.get("name");
            String email = map.get("email");
            String password = map.get("password");
            String organization = map.containsKey("organization") ? map.get("organization") : "";

            QueryResult queryResult = catalogManager.createUser(userId, name, email, password, organization, null, queryOptions);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/login")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "User login returns a valid session ID token", position = 3)
    public Response loginPost(@ApiParam(value = "userId", required = true) @PathParam("user") String userId,
                              @ApiParam(value = "Json containing the param 'password'", required = true) Map<String, String> map) {
        sessionIp = httpServletRequest.getRemoteAddr();
        QueryResult<Session> queryResult;
        try {
            if (!map.containsKey("password")) {
                throw new Exception("The json does not contain the key password.");
            }
            String password = map.get("password");
            queryResult = catalogManager.login(userId, password, sessionIp);

            ObjectMap sessionMap = new ObjectMap();
            sessionMap.append("sessionId", queryResult.first().getId())
                    .append("id", queryResult.first().getId())
                    .append("ip", queryResult.first().getIp())
                    .append("date", queryResult.first().getDate());

            QueryResult<ObjectMap> login = new QueryResult<>("login", queryResult.getDbTime(), 1, 1, queryResult.getWarningMsg(),
                    queryResult.getErrorMsg(), Arrays.asList(sessionMap));

//            if (userId.equalsIgnoreCase("anonymous")) {
//                queryResult = catalogManager.loginAsAnonymous(sessionIp);
//            } else {
//                queryResult = catalogManager.login(userId, password, sessionIp);
//            }
            return createOkResponse(login);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{userId}/change-password")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "User password change", position = 5)
    public Response changePasswordPost(@ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
                                   @ApiParam(value = "Json containing the params 'password' and 'npassword'", required = true) Map<String, String> map) {
        try {
            if (!map.containsKey("password") || !map.containsKey("password")) {
                throw new Exception("The json must contain the keys password and npassword.");
            }
            String password = map.get("password");
            String nPassword = map.get("npassword");
            QueryResult result = catalogManager.changePassword(userId, password, nPassword);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{userId}/configs/create")
    @ApiOperation(value = "Create or update a user configuration", response = Map.class)
    public Response setConfiguration(@ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
                    @ApiParam(value = "Configuration name (typically the name of the application)", required = true) @QueryParam("name") String name,
                    @ApiParam(name = "params", value = "Configuration", required = true) ObjectMap params) {
        try {
            return createOkResponse(catalogManager.getUserManager().setConfig(userId, sessionId, name, params));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{userId}/configs/{name}/delete")
    @ApiOperation(value = "Delete a user configuration", response = Map.class)
    public Response deleteConfiguration(@ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
                                     @ApiParam(value = "Configuration name (typically the name of the application)", required = true) @PathParam("name") String name) {
        try {
            return createOkResponse(catalogManager.getUserManager().deleteConfig(userId, sessionId, name));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{userId}/configs/{name}/info")
    @ApiOperation(value = "Fetch a user configuration", response = Map.class)
    public Response getConfiguration(@ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
                                     @ApiParam(value = "Configuration name (typically the name of the application)", required = true) @PathParam("name") String name) {
        try {
            return createOkResponse(catalogManager.getUserManager().getConfig(userId, sessionId, name));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{userId}/configs/filters/create")
    @ApiOperation(value = "Store a custom filter", response = User.Filter.class)
    public Response addFilter(@ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
                           @ApiParam(name = "params", value = "Filter parameters", required = true) User.Filter params) {
        try {
            return createOkResponse(catalogManager.getUserManager().addFilter(userId, sessionId, params.getName(), params.getDescription(),
                    params.getBioformat(), params.getQuery(), params.getOptions()));
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
    @Path("/{userId}/configs/filters/{name}/update")
    @ApiOperation(value = "Update a custom filter", response = User.Filter.class)
    public Response updateFilter(@ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
                              @ApiParam(value = "Filter name", required = true) @PathParam("name") String name,
                              @ApiParam(name = "params", value = "Filter parameters", required = true) UpdateFilter params) {
        try {
            return createOkResponse(catalogManager.getUserManager().updateFilter(userId, sessionId, name,
                    new ObjectMap(jsonObjectMapper.writeValueAsString(params))));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{userId}/configs/filters/{name}/delete")
    @ApiOperation(value = "Delete a custom filter", response = User.Filter.class)
    public Response deleteFilter(@ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
                                 @ApiParam(value = "Filter name", required = true) @PathParam("name") String name) {
        try {
            return createOkResponse(catalogManager.getUserManager().deleteFilter(userId, sessionId, name));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{userId}/configs/filters/{name}/info")
    @ApiOperation(value = "Fetch filter", response = User.Filter.class)
    public Response getFilter(@ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
                                 @ApiParam(value = "Filter name", required = true) @PathParam("name") String name) {
        try {
            return createOkResponse(catalogManager.getUserManager().getFilter(userId, sessionId, name));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{userId}/configs/filters/list")
    @ApiOperation(value = "Fetch all the filters", response = User.Filter.class)
    public Response getFilters(@ApiParam(value = "userId", required = true) @PathParam("userId") String userId) {
        try {
            return createOkResponse(catalogManager.getUserManager().getAllFilters(userId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}