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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by pfurio on 05/05/17.
 */
@Path("/{apiVersion}/meta")
@Produces("application/json")
@Api(value = "Meta", description = "Meta RESTful Web Services API")
public class MetaWSServer extends OpenCGAWSServer {

    private final String OKAY = "OK";
    private final String NOT_OKAY = "KO";
    private final String SOLR = "Solr";
    private final String VARIANT_STORAGE = "VariantStorage";
    private final String CATALOG_MONGO_DB = "CatalogMongoDB";
    private static String healthCheckErrorMessage = null;
    private static LocalTime lastAccess = LocalTime.now();
    private static HashMap<String, String> healthCheckResults = new HashMap<>();

    public MetaWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/about")
    @ApiOperation(httpMethod = "GET", value = "Returns info about current OpenCGA code.", response = Map.class)
    public Response getAbout() {
        Map<String, String> info = new HashMap<>(5);
        info.put("Program", "OpenCGA (OpenCB)");
        info.put("Version", GitRepositoryState.get().getBuildVersion());
        info.put("Git branch", GitRepositoryState.get().getBranch());
        info.put("Git commit", GitRepositoryState.get().getCommitId());
        info.put("Description", "Big Data platform for processing and analysing NGS data");
        OpenCGAResult queryResult = new OpenCGAResult();
        queryResult.setTime(0);
        queryResult.setResults(Collections.singletonList(info));

        return createOkResponse(queryResult);
    }

    @GET
    @Path("/ping")
    @ApiOperation(httpMethod = "GET", value = "Ping Opencga webservices.", response = Map.class)
    public Response ping() {

        OpenCGAResult queryResult = new OpenCGAResult();
        queryResult.setTime(0);

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

        OpenCGAResult<Map<String, String>> queryResult = new OpenCGAResult<>();
        StopWatch stopWatch = StopWatch.createStarted();

        long elapsedTime = Duration.between(lastAccess, LocalTime.now()).getSeconds();

        if (!isHealthy() || elapsedTime > configuration.getHealthCheck().getInterval()) {
            logger.debug("Update HealthCheck cache status");
            updateHealthCheck();
        } else {
            logger.debug("HealthCheck results from cache at " + lastAccess.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            queryResult.setEvents(Collections.singletonList(new Event(Event.Type.WARNING, "HealthCheck results from cache at "
                    + lastAccess.format(DateTimeFormatter.ofPattern("HH:mm:ss")))));
        }

        queryResult.setTime(((int) stopWatch.getTime(TimeUnit.MILLISECONDS)));
        queryResult.setResults(Collections.singletonList(healthCheckResults));

        if (isHealthy()) {
            logger.debug("HealthCheck : " + healthCheckResults.toString());
            return createOkResponse(queryResult);
        } else {
            logger.error("HealthCheck : " + healthCheckResults.toString());
            return createErrorResponse(healthCheckErrorMessage, queryResult);
        }
    }

    private synchronized void updateHealthCheck() {
        String storageEngineId;
        StringBuilder errorMsg = new StringBuilder();

        Map<String, String> newHealthCheckResults = new HashMap<>();

        try {
            if (catalogManager.getDatabaseStatus()) {
                newHealthCheckResults.put(CATALOG_MONGO_DB, OKAY);
            } else {
                newHealthCheckResults.put(CATALOG_MONGO_DB, NOT_OKAY);
            }
        } catch (Exception e) {
            newHealthCheckResults.put(CATALOG_MONGO_DB, NOT_OKAY);
            errorMsg.append(e.getMessage());
        }

        try {
            storageEngineId = storageEngineFactory.getVariantStorageEngine().getStorageEngineId();
            newHealthCheckResults.put("VariantStorageId", storageEngineId);
        } catch (Exception e) {
            errorMsg.append(" No storageEngineId is set in configuration or Unable to initiate storage Engine, ").append(e.getMessage()).append(", ");
            newHealthCheckResults.put(VARIANT_STORAGE, NOT_OKAY);
        }

        try {
            storageEngineFactory.getVariantStorageEngine().testConnection();
            newHealthCheckResults.put(VARIANT_STORAGE, OKAY);
        } catch (Exception e) {
            newHealthCheckResults.put(VARIANT_STORAGE, NOT_OKAY);
            errorMsg.append(e.getMessage());
        }

        if (storageEngineFactory.getStorageConfiguration().getSearch().isActive()) {
            try {
                if (variantManager.isSolrAvailable()) {
                    newHealthCheckResults.put(SOLR, OKAY);
                } else {
                    errorMsg.append(", unable to connect with solr, ");
                    newHealthCheckResults.put(SOLR, NOT_OKAY);
                }
            } catch (Exception e) {
                newHealthCheckResults.put(SOLR, NOT_OKAY);
                errorMsg.append(e.getMessage());
            }
        } else {
            newHealthCheckResults.put(SOLR, "solr not active in storage-configuration!");
        }

        if (errorMsg.length() == 0) {
            healthCheckErrorMessage = null;
        } else {
            healthCheckErrorMessage = errorMsg.toString();
        }
        healthCheckResults.clear();
        healthCheckResults.putAll(newHealthCheckResults);
        lastAccess = LocalTime.now();
    }

    @GET
    @Path("/api")
    @ApiOperation(value = "API", response = Map.class)
    public Response api(@ApiParam(value = "List of categories to get API from") @QueryParam("category") String categoryStr) {
        List<LinkedHashMap<String, Object>> api = new ArrayList<>(20);
        Map<String, Class> classes = new LinkedHashMap<>();
        classes.put("users", UserWSServer.class);
        classes.put("projects", ProjectWSServer.class);
        classes.put("studies", StudyWSServer.class);
        classes.put("files", FileWSServer.class);
        classes.put("jobs", JobWSServer.class);
        classes.put("samples", SampleWSServer.class);
        classes.put("individuals", IndividualWSServer.class);
        classes.put("families", FamilyWSServer.class);
        classes.put("cohorts", CohortWSServer.class);
        classes.put("panels", PanelWSServer.class);
        classes.put("alignment", AlignmentWebService.class);
        classes.put("variant", VariantWebService.class);
        classes.put("clinical", ClinicalWebService.class);
        classes.put("variantOperations", VariantOperationWebService.class);
        classes.put("meta", MetaWSServer.class);
        classes.put("ga4gh", Ga4ghWSServer.class);
        classes.put("admin", AdminWSServer.class);

        if (StringUtils.isNotEmpty(categoryStr)) {
            for (String category : categoryStr.split(",")) {
                api.add(getHelp(classes.get(category)));
            }
        } else {
            // Get API for all categories
            for (String category : classes.keySet()) {
                api.add(getHelp(classes.get(category)));
            }
        }

        return createOkResponse(new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(api), 1));
    }

    private LinkedHashMap<String, Object> getHelp(Class clazz) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();

        map.put("name", ((Api) clazz.getAnnotation(Api.class)).value());
        map.put("path", ((Path) clazz.getAnnotation(Path.class)).value());

        List<LinkedHashMap<String, Object>> endpoints = new ArrayList<>(20);
        for (Method method : clazz.getMethods()) {
            Path pathAnnotation = method.getAnnotation(Path.class);
            String httpMethod = "GET";
            if (method.getAnnotation(POST.class) != null ) {
                httpMethod = "POST";
            } else {
                if (method.getAnnotation(DELETE.class) != null) {
                    httpMethod = "DELETE";
                }
            }
            ApiOperation apiOperationAnnotation = method.getAnnotation(ApiOperation.class);
            if (pathAnnotation != null && apiOperationAnnotation != null && !apiOperationAnnotation.hidden()) {
                LinkedHashMap<String, Object> endpoint = new LinkedHashMap<>();
                endpoint.put("path", map.get("path") + pathAnnotation.value());
                endpoint.put("method", httpMethod);
                endpoint.put("response", StringUtils.substringAfterLast(apiOperationAnnotation.response().getName().replace("Void", ""), "."));

                String responseClass = apiOperationAnnotation.response().getName().replace("Void", "");
                endpoint.put("responseClass", responseClass.endsWith(";") ? responseClass : responseClass + ";");
                endpoint.put("notes", apiOperationAnnotation.notes());
                endpoint.put("description", apiOperationAnnotation.value());

                ApiImplicitParams apiImplicitParams = method.getAnnotation(ApiImplicitParams.class);
                List<LinkedHashMap<String, Object>> parameters = new ArrayList<>();
                if (apiImplicitParams != null) {
                    for (ApiImplicitParam apiImplicitParam : apiImplicitParams.value()) {
                        LinkedHashMap<String, Object> parameter = new LinkedHashMap<>();
                        parameter.put("name", apiImplicitParam.name());
                        parameter.put("param", apiImplicitParam.paramType());
                        parameter.put("type", apiImplicitParam.dataType());
                        parameter.put("typeClass", "java.lang." + StringUtils.capitalize(apiImplicitParam.dataType()));
                        parameter.put("allowedValues", apiImplicitParam.allowableValues());
                        parameter.put("required", apiImplicitParam.required());
                        parameter.put("defaultValue", apiImplicitParam.defaultValue());
                        parameter.put("description", apiImplicitParam.value());
                        parameters.add(parameter);
                    }
                }

                Parameter[] methodParameters = method.getParameters();
                if (methodParameters != null) {
                    for (Parameter methodParameter : methodParameters) {
                        ApiParam apiParam = methodParameter.getAnnotation(ApiParam.class);
                        if (apiParam != null && !apiParam.hidden()) {
                            LinkedHashMap<String, Object> parameter = new LinkedHashMap<>();
                            if (methodParameter.getAnnotation(PathParam.class) != null) {
                                parameter.put("name", methodParameter.getAnnotation(PathParam.class).value());
                                parameter.put("param", "path");
                            } else {
                                if (methodParameter.getAnnotation(QueryParam.class) != null) {
                                    parameter.put("name", methodParameter.getAnnotation(QueryParam.class).value());
                                    parameter.put("param", "query");
                                } else {
                                    parameter.put("name", "body");
                                    parameter.put("param", "body");
                                }
                            }

                            // Get type in lower case except for 'body' param
                            String type = methodParameter.getType().getName();
                            String typeClass = type;
                            if (typeClass.contains(".")) {
                                String[] split = typeClass.split("\\.");
                                type = split[split.length - 1];
                                if (!parameter.get("param").equals("body")) {
                                    type = type.toLowerCase();

                                    // Complex type different from body are enums
                                    if (type.contains("$")) {
                                        type = "enum";
                                    }
                                } else {
                                    type = "object";
                                }
                            }
                            parameter.put("type", type);
                            parameter.put("typeClass", typeClass.endsWith(";") ? typeClass : typeClass + ";");
                            parameter.put("allowedValues", apiParam.allowableValues());
                            parameter.put("required", apiParam.required());
                            parameter.put("defaultValue", apiParam.defaultValue());
                            parameter.put("description", apiParam.value());
                            parameters.add(parameter);
                        }
                    }
                }
                endpoint.put("parameters", parameters);
                endpoints.add(endpoint);
            }
        }

        Collections.sort(endpoints, Comparator.comparing(endpoint -> (String) endpoint.get("path")));
        map.put("endpoints", endpoints);
        return map;
    }

    private boolean isHealthy() {
        return healthCheckResults.isEmpty() ? false : !healthCheckResults.values().stream().anyMatch(x -> x.equals(NOT_OKAY));
    }
}
