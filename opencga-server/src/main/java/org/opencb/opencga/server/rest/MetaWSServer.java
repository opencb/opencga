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

import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.core.tools.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.server.json.RestApiParser;
import org.opencb.opencga.server.json.beans.Category;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
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
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by pfurio on 05/05/17.
 */
@Path("/{apiVersion}/meta")
@Produces("application/json")
@Api(value = "Meta", description = "Meta RESTful Web Services API")
public class MetaWSServer extends OpenCGAWSServer {

    private static final AtomicReference<String> healthCheckErrorMessage = new AtomicReference<>();
    private static final AtomicReference<LocalTime> lastAccess = new AtomicReference<>(LocalTime.now());
    private static final Map<String, String> healthCheckResults = new ConcurrentHashMap<>();
    private final String OKAY = "OK";
    private final String NOT_OKAY = "KO";
    private final String SOLR = "Solr";
    private final String VARIANT_STORAGE = "VariantStorage";
    private final String CATALOG_MONGO_DB = "CatalogMongoDB";

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

        OpenCGAResult<Map<String, String>> queryResult = new OpenCGAResult<>();
        StopWatch stopWatch = StopWatch.createStarted();

        if (shouldUpdateStatus()) {
            logger.debug("Update HealthCheck cache status");
            updateHealthCheck();
        } else {
            logger.debug("HealthCheck results from cache at " + lastAccess.get().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            queryResult.setEvents(Collections.singletonList(new Event(Event.Type.INFO, "HealthCheck results from cache at "
                    + lastAccess.get().format(DateTimeFormatter.ofPattern("HH:mm:ss")))));
        }

        queryResult.setTime(((int) stopWatch.getTime(TimeUnit.MILLISECONDS)));
        queryResult.setResults(Collections.singletonList(healthCheckResults));

        if (isHealthy()) {
            logger.debug("HealthCheck : " + healthCheckResults.toString());
            return createOkResponse(queryResult);
        } else {
            logger.error("HealthCheck : " + healthCheckResults.toString());
            return createErrorResponse(healthCheckErrorMessage.get(), queryResult);
        }
    }

    private boolean shouldUpdateStatus() {
        if (!isHealthy()) {
            // Always update if not healthy
            return true;
        }
        // If healthy, only update every "healthCheck.interval" seconds
        long elapsedTime = Duration.between(lastAccess.get(), LocalTime.now()).getSeconds();
        return elapsedTime > configuration.getHealthCheck().getInterval();
    }

    private synchronized void updateHealthCheck() {
        if (!shouldUpdateStatus()) {
            // Skip update!
            return;
        }
        StringBuilder errorMsg = new StringBuilder();

        Map<String, String> newHealthCheckResults = new HashMap<>();
        newHealthCheckResults.put(CATALOG_MONGO_DB, "");
        newHealthCheckResults.put(VARIANT_STORAGE, "");
        newHealthCheckResults.put(SOLR, "");

        StopWatch totalTime = StopWatch.createStarted();
        StopWatch catalogMongoDBTime = StopWatch.createStarted();
        try {
            if (catalogManager.getCatalogDatabaseStatus()) {
                newHealthCheckResults.put(CATALOG_MONGO_DB, OKAY);
            } else {
                newHealthCheckResults.put(CATALOG_MONGO_DB, NOT_OKAY);
            }
        } catch (Exception e) {
            newHealthCheckResults.put(CATALOG_MONGO_DB, NOT_OKAY);
            errorMsg.append(e.getMessage());
        }
        catalogMongoDBTime.stop();

        StopWatch storageTime = StopWatch.createStarted();
        try {
            storageEngineFactory.getVariantStorageEngine().testConnection();
            newHealthCheckResults.put("VariantStorageId", storageEngineFactory.getVariantStorageEngine().getStorageEngineId());
            newHealthCheckResults.put(VARIANT_STORAGE, OKAY);
        } catch (Exception e) {
            newHealthCheckResults.put(VARIANT_STORAGE, NOT_OKAY);
            errorMsg.append(e.getMessage());
//            errorMsg.append(" No storageEngineId is set in configuration or Unable to initiate storage Engine, ").append(e.getMessage()
//            ).append(", ");
        }
        storageTime.stop();

        StopWatch solrEngineTime = StopWatch.createStarted();
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
        solrEngineTime.stop();

        if (totalTime.getTime(TimeUnit.SECONDS) > 5) {
            logger.warn("Slow OpenCGA status: Updated time: {}. Catalog: {} , Storage: {} , Solr: {}",
                    totalTime.getTime(TimeUnit.MILLISECONDS) / 1000.0,
                    catalogMongoDBTime.getTime(TimeUnit.MILLISECONDS) / 1000.0,
                    storageTime.getTime(TimeUnit.MILLISECONDS) / 1000.0,
                    solrEngineTime.getTime(TimeUnit.MILLISECONDS) / 1000.0
            );
        }

        if (errorMsg.length() == 0) {
            healthCheckErrorMessage.set(null);
        } else {
            healthCheckErrorMessage.set(errorMsg.toString());
        }

        healthCheckResults.putAll(newHealthCheckResults);
        lastAccess.set(LocalTime.now());
    }

    @GET
    @Path("/api")
    @ApiOperation(value = "API", response = List.class)
    public Response api(@ApiParam(value = "List of categories to get API from") @QueryParam("category") String categoryStr) {

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
        List<Class> JSONClasses = new ArrayList<>();
        if (StringUtils.isNotEmpty(categoryStr)) {
            for (String category : categoryStr.split(",")) {
                JSONClasses.add(classes.get(category));
            }
        } else {
            // Get API for all categories
            for (String category : classes.keySet()) {
                JSONClasses.add(classes.get(category));
            }
        }
        //List<LinkedHashMap<String, Object>> api = JSONManager.getHelp(JSONClasses);
        List<Category> api = RestApiParser.getCategories(JSONClasses);
        return createOkResponse(new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(api), 1));
    }

    private boolean isHealthy() {
        return healthCheckResults.isEmpty() ? false : !healthCheckResults.values().stream().anyMatch(x -> x.equals(NOT_OKAY));
    }
}
