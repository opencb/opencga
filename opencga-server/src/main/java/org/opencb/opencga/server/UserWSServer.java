/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.server;


import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Map;

@Path("/users")
@Api(value = "users", description = "Users web service", position = 1)
public class UserWSServer extends OpenCGAWSServer {

    public UserWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @Produces("application/json")
    @ApiOperation(value = "Creates a new User")

    public Response createUser(
            @ApiParam(value = "userId", required = true) @QueryParam("userId") String userId,
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
            @ApiParam(value = "email", required = true) @QueryParam("email") String email,
            @ApiParam(value = "organization", required = true) @QueryParam("organization") String organization,
            @ApiParam(value = "password", required = true) @QueryParam("password") String password) {
        QueryResult queryResult;
        try {
            queryResult = catalogManager.createUser(userId, name, email, password, organization, this.getQueryOptions());
            return createOkResponse(queryResult);

        } catch (CatalogException  e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }

    @GET
    @Path("/{userId}/login")
    @Produces("application/json")
    @ApiOperation(value = "User login")

    public Response login(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "password", required = false) @QueryParam("password") String password) {
        QueryResult queryResult;
        try {
            if (userId.toLowerCase().equals("anonymous")) {
                queryResult = catalogManager.loginAsAnonymous(sessionIp);
            } else {
                queryResult = catalogManager.login(userId, password, sessionIp);
            }
            return createOkResponse(queryResult);
        } catch (CatalogException | IOException  e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{userId}/logout")
    @Produces("application/json")
    @ApiOperation(value = "User login")
    public Response logout(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId
    ) throws IOException {
        try {
            QueryResult result;
            if (userId.toLowerCase().equals("anonymous")) {
                result = catalogManager.logoutAnonymous(sessionId);
            } else {
                result = catalogManager.logout(userId, sessionId);
            }
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{userId}/change-password")
    @Produces("application/json")
    @ApiOperation(value = "User password change")
    public Response changePassword(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "password", required = true) @QueryParam("password") String password,
            @ApiParam(value = "npassword", required = true) @QueryParam("npassword") String nPassword1
    ) throws IOException {
        try {
            QueryResult result = catalogManager.changePassword(userId, password, nPassword1, sessionId);
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{userId}/change-email")
    @Produces("application/json")
    @ApiOperation(value = "User email change")
    public Response changeEmail(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "nemail", required = true) @QueryParam("nemail") String nEmail
    ) throws IOException {
        try {
            QueryResult result = catalogManager.changeEmail(userId, nEmail, sessionId);
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{userId}/reset-password")
    @Produces("application/json")
    @ApiOperation(value = "User email change")
    public Response resetPassword(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "email", required = true) @QueryParam("email") String email
    ) throws IOException {
        try {
            QueryResult result = catalogManager.resetPassword(userId, email);
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{userId}/modify")
    @Produces("application/json")
    @ApiOperation(value = "User modify")
    public Response modifyUser(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "name", required = false) @QueryParam("name") String name,
            @ApiParam(value = "email", required = false) @QueryParam("email") String email,
            @ApiParam(value = "organization", required = false) @QueryParam("organization") String organization,
            @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes,
            @ApiParam(value = "configs", required = false) @QueryParam("configs") String configs)
            throws IOException {
        try {
            ObjectMap objectMap = new ObjectMap();
            if (name != null) {
                objectMap.put("name", name);
            }
            if (email != null) {
                objectMap.put("email", email);
            }
            if (organization != null) {
                objectMap.put("organization", organization);
            }
            if (attributes != null) {
                objectMap.put("attributes", attributes);
            }
            if (configs != null) {
                objectMap.put("configs", configs);
            }

            QueryResult result = catalogManager.modifyUser(userId, objectMap, sessionId);
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @POST
    @Path("/{userId}/modify")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "User modify")
    public Response modifyUserPOST(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value="params", required = true) Map<String, Object> params) {
        try {
            ObjectMap objectMap = new ObjectMap(params);

            QueryResult result = catalogManager.modifyUser(userId, objectMap, sessionId);
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }


    }

    @GET
    @Path("/{userId}/info")
    @Produces("application/json")
    @ApiOperation(value = "User info")
    public Response getInfo(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "lastActivity", required = false) @QueryParam("lastActivity") String lastActivity) throws IOException {
        try {
            QueryResult result = catalogManager.getUser(userId, lastActivity, this.getQueryOptions(), sessionId);
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }


    @GET
    @Path("/{userId}/projects")
    @Produces("application/json")
    @ApiOperation(value = "Project information")

    public Response getAllProjects(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId
    ) {
        QueryResult queryResult;
        try {
            queryResult = catalogManager.getAllProjects(userId, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}