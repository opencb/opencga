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

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.jasig.cas.client.authentication.AttributePrincipal;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.user.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URI;
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

    @GET
    @Path("/search")
    @ApiOperation(value = "User search method", response = User.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType =
                    "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType =
                    "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType =
                    "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.USER_ID_DESCRIPTION) @QueryParam(ParamConstants.USER_ID_PARAM) String userId,
            @ApiParam(value = ParamConstants.USER_AUTHENTICATION_ORIGIN_DESCRIPTION) @QueryParam(ParamConstants.USER_AUTHENTICATION_ORIGIN) String authentication) {
        return run(() -> {
            query.remove(ParamConstants.ORGANIZATION);
            return catalogManager.getUserManager().search(organizationId, query, queryOptions, token);
        });
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
    public Response getInfo(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.USERS_DESCRIPTION, required = true) @PathParam("users") String userIds
    ) {
        try {
            List<String> userList = getIdList(userIds);
            OpenCGAResult<User> result = catalogManager.getUserManager().get(organizationId, userList, queryOptions, token);
            return createOkResponse(result);
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
            OpenCGAResult<AuthenticationResponse> authenticationResponse;
            if (StringUtils.isNotEmpty(login.getPassword()) && StringUtils.isNotEmpty(login.getUser())) {
                if (StringUtils.isNotEmpty(login.getRefreshToken())) {
                    throw new Exception("Only 'user' and 'password' fields or 'refreshToken' field are allowed at the same time");
                }
                authenticationResponse = catalogManager.getUserManager().login(login.getOrganization(), login.getUser(), login.getPassword());
            } else if (StringUtils.isNotEmpty(login.getRefreshToken())) {
                if (StringUtils.isNotEmpty(login.getPassword()) || StringUtils.isNotEmpty(login.getUser())) {
                    throw new Exception("Only 'user' and 'password' fields or 'refreshToken' field are allowed at the same time");
                }
                authenticationResponse = catalogManager.getUserManager().refreshToken(login.getRefreshToken());
            } else {
                throw new Exception("Neither 'user' and 'password' for login nor 'refreshToken' for refreshing token were provided.");
            }

            return createOkResponse(authenticationResponse);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/anonymous")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get an anonymous token to gain access to the system", response = AuthenticationResponse.class)
    public Response anonymous(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @QueryParam(ParamConstants.ORGANIZATION) String organizationId) {
        try {
            OpenCGAResult<AuthenticationResponse> authenticationResponse = catalogManager.getUserManager().loginAnonymous(organizationId);
            return createOkResponse(authenticationResponse);
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
            catalogManager.getUserManager().changePassword(params.getOrganizationId(), params.getUser(), params.getPassword(), params.getNewPassword());
            return createOkResponse(DataResult.empty());
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{user}/password/reset")
    @ApiOperation(value = "[DEPRECATED]", notes = "[DEPRECATED] This WS has been moved to /organizations/password/reset.", response = User.class)
    public Response resetPassword(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId) {
        try {
            OpenCGAResult<?> result = catalogManager.getOrganizationManager().resetUserPassword(userId, token);
            return createOkResponse(result, "The new password has been sent to the user's email.");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{user}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some user attributes", response = User.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updateByPost(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing the params to be updated.", required = true) UserUpdateParams parameters) {
        try {
            ObjectUtils.defaultIfNull(parameters, new UserUpdateParams());

            ObjectMap params = new ObjectMap(getUpdateObjectMapper().writeValueAsString(parameters));
            OpenCGAResult<User> result = catalogManager.getUserManager().update(userId, params, queryOptions, token);
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
                return createOkResponse(catalogManager.getUserManager().setConfig(userId, params.getId(), params.getConfiguration(),
                        token));
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

    @GET
    @Path("/sso/login")
    @ApiOperation(httpMethod = "GET", value = "Single Sign On.", response = AuthenticationResponse.class)
    public Response singleSignOn(@ApiParam(value = ParamConstants.SSO_CALLBACK_URL_DESCRIPTION) @QueryParam("url") String service) {
        if (StringUtils.isEmpty(service)) {
            return createErrorResponse(new CatalogParameterException("Missing mandatory field 'service'"));
        }

        URI targetURIForRedirection;
        try {
            StringBuilder queryParams = new StringBuilder();
            if (!service.endsWith("?")) {
                queryParams.append("?");
            }
            // Add session id
            queryParams.append("jsessionid").append("=").append(httpServletRequest.getSession().getId());

            AttributePrincipal principal = (AttributePrincipal) httpServletRequest.getUserPrincipal();
            String token = catalogManager.getUserManager().ssoLogin(principal.getName(), principal.getAttributes());
            // Add user and token
            queryParams.append("&").append("token").append("=").append(token);
            queryParams.append("&").append("user").append("=").append(principal.getName());

            targetURIForRedirection = new URI(service + queryParams);
            logger.debug("Redirecting /sso call to {}", targetURIForRedirection);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return Response.temporaryRedirect(targetURIForRedirection).build();
    }

    @GET
    @Path("/sso/logout")
    @ApiOperation(httpMethod = "GET", value = "Logout from Single Sign On.", response = AuthenticationResponse.class)
    public Response singleSignOnLogout(
            @ApiParam(value = ParamConstants.SSO_CALLBACK_URL_DESCRIPTION) @QueryParam("url") String service,
            @ApiParam(value = ParamConstants.SSO_LOGOUT_DESCRIPTION, hidden = true, defaultValue = "false") @QueryParam("logout") boolean logout
    ) {
        Configuration ssoConfiguration = catalogManager.getConfiguration();
        if (ssoConfiguration.getSso() == null || !ssoConfiguration.getSso().isActive()) {
            return createErrorResponse(new CatalogException("SSO is not enabled."));
        }
        if (StringUtils.isEmpty(ssoConfiguration.getSso().getCasServerPrefixUrl())) {
            return createErrorResponse(new CatalogException("Server error: SSO server prefix url is not properly set."));
        }

        // Logout is performed in 2 steps. Users must call to /logout without any query parameter. This will redirect to
        // CAS logout WS with a callback url to this same WS adding the query parameter "logout=true". When it reaches
        // the WS with the query parameter, it will return the html redirecting to the original callback url and remove
        // local cookies.
        if (!logout) {
            // Get the public address
            String serverName = ssoConfiguration.getSso().getServerName();
            // Get the internal address
            String internalUrlCalled = uriInfo.getAbsolutePath().toString();
            int i = internalUrlCalled.indexOf("/opencga");
            // Remove preceded section of internal url and replace it for serverName configuration url.
            // serverName should always contain the external public url users call, whereas internalUrlCalled will
            // normally be the internal url (when in use with a reverse proxy)
            String originalUrl = serverName + internalUrlCalled.substring(i);

            UriBuilder uriBuilder = UriBuilder.fromPath(ssoConfiguration.getSso().getCasServerPrefixUrl());
            uriBuilder.path("logout");
            UriBuilder callbackUri = UriBuilder.fromPath(originalUrl);

            callbackUri.queryParam("logout", true);
            callbackUri.queryParam("url", service);
            uriBuilder.queryParam("service", callbackUri.build());

            logger.debug("Callback uri: {}", callbackUri.build());

            URI uri = uriBuilder.build();
            logger.debug("Redirecting to {}", uri.getPath());
            return Response.temporaryRedirect(uri).build();
        } else {
            StringBuilder htmlBuilder = new StringBuilder()
                    .append("<html lang=\"en\">\n")
                    .append("  <head>\n")
                    .append("    <meta charset=\"UTF-8\" />\n")
                    .append("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />\n")
                    .append("    <link href=\"https://unpkg.com/lowcss/dist/low.css\" rel=\"stylesheet\" />\n")
                    .append("  </head>\n")
                    .append("  <body class=\"font-inter leading-normal m-0 p-0 h-screen\">\n")
                    .append("    <div class=\"flex items-center justify-center w-full h-full\">\n")
                    .append("      <div class=\"w-full maxw-lg rounded-lg text-center p-12 bg-green-100\">\n")
                    .append("        <div>Successfully logged out from CAS.</div>\n");
            if (StringUtils.isNotEmpty(service)) {
                htmlBuilder.append("        <div>Redirecting in <span id=\"time\">3</span> secs...</div>\n");
            }
            htmlBuilder
                    .append("      </div>\n")
                    .append("    </div>\n");
            if (StringUtils.isNotEmpty(service)) {
                htmlBuilder
                        .append("    <script type=\"text/javascript\">\n")
                        .append("      let time = 3;\n")
                        .append("      const timer = window.setInterval(() => {\n")
                        .append("        time = time - 1;\n")
                        .append("        document.getElementById(\"time\").textContent = time;\n")
                        .append("        if (time === 0) {\n")
                        .append("          window.clearInterval(timer);\n")
                        .append("          window.location = '").append(service).append("';\n")
                        .append("        }\n")
                        .append("      }, 1000);\n")
                        .append("    </script>\n");
            }
            htmlBuilder
                    .append("  </body>\n")
                    .append("</html>");

            logger.info("CAS logout requested. Invalidating session '{}'", httpServletRequest.getSession().getId());
            httpServletRequest.getSession().invalidate();

            return Response.ok(htmlBuilder.toString(), MediaType.TEXT_HTML_TYPE).build();
        }
    }
}