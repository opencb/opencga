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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.monitor.HealthCheckResponse;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pfurio on 05/05/17.
 */
@Path("/{apiVersion}/meta")
@Produces("application/json")
@Api(value = "Meta", description = "Meta RESTful Web Services API")
public class MetaWSServer extends OpenCGAWSServer {

    public MetaWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/about")
    @ApiOperation(httpMethod = "GET", value = "Returns info about current OpenCGA code.")
    public Response getAbout() {
        Map<String, String> info = new HashMap<>(5);
        info.put("Program", "OpenCGA (OpenCB)");
        info.put("Version", GitRepositoryState.get().getBuildVersion());
        info.put("Git branch", GitRepositoryState.get().getBranch());
        info.put("Git commit", GitRepositoryState.get().getCommitId());
        info.put("Description", "Big Data platform for processing and analysing NGS data");
        QueryResult queryResult = new QueryResult();
        queryResult.setId("about");
        queryResult.setDbTime(0);
        queryResult.setResult(Collections.singletonList(info));

        return createOkResponse(queryResult);
    }

    @GET
    @Path("/ping")
    @ApiOperation(httpMethod = "GET", value = "Ping Opencga webservices.")
    public Response ping() {

        QueryResult queryResult = new QueryResult();
        queryResult.setId("pong");
        queryResult.setDbTime(0);

        return createOkResponse(queryResult);
    }

    @GET
    @Path("/health")
    @ApiOperation(httpMethod = "GET", value = "Reports on the overall system status based on the status of such things "
            + "as database connections and the ability to access other APIs.", response = HealthCheckResponse.class)
    public Response status(
            @ApiParam(value = "API token for health check. When passed all of the dependencies and their status will be displayed. "
                    + "The dependencies will be checked if this parameter is not used, but they won't be part of the response.")
            @QueryParam("token") String token) {
        try {
            if (StringUtils.isEmpty(sessionId) && StringUtils.isNotEmpty(token)) {
                sessionId = token;
            }

            HealthCheckResponse healthCheckResponse = catalogManager.healthCheck(httpServletRequest.getRequestURI(), sessionId);
            if (healthCheckResponse.getStatus() == HealthCheckResponse.Status.OK) {
                return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(healthCheckResponse), MediaType.APPLICATION_JSON_TYPE));
            } else {
                return buildResponse(
                        Response.ok(jsonObjectWriter.writeValueAsString(healthCheckResponse), MediaType.APPLICATION_JSON_TYPE)
                                .status(Response.Status.SERVICE_UNAVAILABLE)
                );
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/status")
    @ApiOperation(httpMethod = "GET", value = "Database status.")
    public Response status() {
        try {
            QueryResult queryResult = new QueryResult();
            queryResult.setId("Status");
            queryResult.setDbTime(0);
            HealthCheckResponse healthCheckResponse = catalogManager.healthCheck(httpServletRequest.getRequestURI(), "");
            queryResult.setResult(Collections.singletonList(new ObjectMap("ok", healthCheckResponse.getComponents().contains("MongoDB"))));
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
