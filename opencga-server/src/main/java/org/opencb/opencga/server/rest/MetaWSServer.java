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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.DataModelsUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.core.tools.annotations.ApiParam;
import org.opencb.opencga.server.OpenCGAHealthCheckMonitor;
import org.opencb.opencga.server.generator.RestApiParser;
import org.opencb.opencga.server.generator.commons.ApiCommonsImpl;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.openapi.models.Swagger;
import org.opencb.opencga.server.generator.openapi.JsonOpenApiGenerator;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.*;

/**
 * Created by pfurio on 05/05/17.
 */
@Path("/{apiVersion}/meta")
@Produces("application/json")
@Api(value = "Meta", description = "Meta RESTful Web Services API")
public class MetaWSServer extends OpenCGAWSServer {

    public MetaWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/about")
    @ApiOperation(httpMethod = "GET", value = "Returns info about current OpenCGA code.", response = Map.class)
    public Response getAbout() {
        Map<String, String> info = new HashMap<>(5);
        info.put("Program", "OpenCGA (OpenCB)");
        info.put("Version", GitRepositoryState.getInstance().getBuildVersion());
        info.put("Git branch", GitRepositoryState.getInstance().getBranch());
        info.put("Git commit", GitRepositoryState.getInstance().getCommitId());
        info.put("Description", "Big Data platform for processing and analysing NGS data");
        OpenCGAResult queryResult = new OpenCGAResult();
        queryResult.setTime(0);
        queryResult.setResults(Collections.singletonList(info));

        return createOkResponse(queryResult);
    }

    @GET
    @Path("/ping")
    @ApiOperation(httpMethod = "GET", value = "Ping Opencga webservices.", response = String.class)
    public Response ping() {
        OpenCGAResult<String> queryResult = new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList("pong"), 1);
        return createOkResponse(queryResult);
    }


    @GET
    @Path("/fail")
    @ApiOperation(httpMethod = "GET", value = "Ping Opencga webservices.", response = Map.class)
    public Response fail() {
        throw new RuntimeException("Do fail!");
    }

    @GET
    @Path("/status")
    @ApiOperation(httpMethod = "GET", value = "Database status.", response = Map.class)
    public Response status() {
        OpenCGAResult<OpenCGAHealthCheckMonitor.HealthCheckStatus> queryResult = healthCheckMonitor.getStatus();
        OpenCGAHealthCheckMonitor.HealthCheckStatus status = queryResult.first();

        if (status.isHealthy()) {
            logger.debug("HealthCheck : " + status);
            return createOkResponse(queryResult);
        } else {
            logger.error("HealthCheck : " + status);
            return createErrorResponse(status.getErrorMessage(), queryResult);
        }
    }

    @GET
    @Path("/model")
    @ApiOperation(value = "Opencga model webservices.", response = String.class)
    public Response model(@ApiParam(value = "Model description") @QueryParam("model") String modelStr) {

        return run(() -> new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(DataModelsUtils.dataModelToJsonString(modelStr, false)), 1));

    }

    @GET
    @Path("/api")
    @ApiOperation(value = "API", response = List.class)
    public Response api(@ApiParam(value = "List of categories to get API from") @QueryParam("category") String categoryStr, @QueryParam("summary") boolean summary) {
        Map<String, Class<?>> classMap = new LinkedHashMap<>();
        classMap.put("organizations", OrganizationWSServer.class);
        classMap.put("users", UserWSServer.class);
        classMap.put("projects", ProjectWSServer.class);
        classMap.put("studies", StudyWSServer.class);
        classMap.put("files", FileWSServer.class);
        classMap.put("jobs", JobWSServer.class);
        classMap.put("samples", SampleWSServer.class);
        classMap.put("individuals", IndividualWSServer.class);
        classMap.put("families", FamilyWSServer.class);
        classMap.put("cohorts", CohortWSServer.class);
        classMap.put("panels", PanelWSServer.class);
        classMap.put("alignment", AlignmentWebService.class);
        classMap.put("variant", VariantWebService.class);
        classMap.put("clinical", ClinicalWebService.class);
        classMap.put("variantOperations", VariantOperationWebService.class);
        classMap.put("meta", MetaWSServer.class);
        classMap.put("admin", AdminWSServer.class);
//        classMap.put("ga4gh", Ga4ghWSServer.class);

        List<Class<?>> classes = new ArrayList<>();
        // Check if some categories have been selected
        if (StringUtils.isNotEmpty(categoryStr)) {
            for (String category : categoryStr.split(",")) {
                Class<?> clazz = classMap.get(category);
                if (clazz != null) {
                    classes.add(clazz);
                } else {
                    return createErrorResponse("meta/api",
                            "Category '" + category + "' not found. Available categories: " + String.join(",", classMap.keySet()));
                }
            }
        } else {
            // Get API for all categories
            for (String category : classMap.keySet()) {
                classes.add(classMap.get(category));
            }
        }
        RestApi restApi = new RestApiParser().parse(classes, summary);
        return createOkResponse(new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(restApi.getCategories()), 1));
    }


    @GET
    @Path("/openapi")
    @ApiOperation(value = "Opencga openapi json", response = String.class)
    public String openApi(@ApiParam(value = "List of categories to get API from") @QueryParam("token") String token, @QueryParam("environment") String environment) {
        JsonOpenApiGenerator generator = new JsonOpenApiGenerator();
        Swagger swagger = generator.generateJsonOpenApi(new ApiCommonsImpl(), token, environment);
        String swaggerJson ="ERROR: Swagger could not be generated";
        ObjectMapper mapper = new ObjectMapper();
        try {
            swaggerJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(swagger).replace("{apiVersion}", "v2");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return swaggerJson;
    }
}
