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
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
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

    private final String OKAY = "OK";
    private final String NOT_OKAY = "KO";
    private final String SOLR = "Solr";
    private final String VARIANT_STORAGE = "VariantStorage";
    private final String CATALOG_MONGO_DB = "CatalogMongoDB";
    private static LocalTime lastAccess = LocalTime.now();
    private static HashMap<String, String> healthCheckResults = new HashMap<>();

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
        DataResult queryResult = new DataResult();
        queryResult.setTime(0);
        queryResult.setResults(Collections.singletonList(info));

        return createOkResponse(queryResult);
    }

    @GET
    @Path("/ping")
    @ApiOperation(httpMethod = "GET", value = "Ping Opencga webservices.")
    public Response ping() {

        DataResult queryResult = new DataResult();
        queryResult.setTime(0);

        return createOkResponse(queryResult);
    }

    @GET
    @Path("/status")
    @ApiOperation(httpMethod = "GET", value = "Database status.")
    public Response status() {

        DataResult queryResult = new DataResult();
        queryResult.setTime(0);

        String storageEngineId;
        StringBuilder errorMsg = new StringBuilder();
        long elapsedTime = Duration.between(lastAccess, LocalTime.now()).getSeconds();

        if (!isHealthy() || elapsedTime > configuration.getHealthCheck().getInterval()) {
            logger.info("HealthCheck results without cache!");
            lastAccess = LocalTime.now();
            healthCheckResults.clear();

            try {
                if (catalogManager.getDatabaseStatus()) {
                    healthCheckResults.put(CATALOG_MONGO_DB, OKAY);
                } else {
                    healthCheckResults.put(CATALOG_MONGO_DB, NOT_OKAY);
                }
            } catch (Exception e) {
                healthCheckResults.put(CATALOG_MONGO_DB, NOT_OKAY);
                errorMsg.append(e.getMessage());
            }

            try {
                storageEngineId = storageEngineFactory.getVariantStorageEngine().getStorageEngineId();
                healthCheckResults.put("VariantStorageId", storageEngineId);
            } catch (Exception e) {
                errorMsg.append(" No storageEngineId is set in configuration or Unable to initiate storage Engine, ").append(e.getMessage()).append(", ");
                healthCheckResults.put(VARIANT_STORAGE, NOT_OKAY);
            }

            try {
                storageEngineFactory.getVariantStorageEngine().testConnection();
                healthCheckResults.put(VARIANT_STORAGE, OKAY);
            } catch (Exception e) {
                healthCheckResults.put(VARIANT_STORAGE, NOT_OKAY);
                errorMsg.append(e.getMessage());
            }

            if (storageEngineFactory.getStorageConfiguration().getSearch().isActive()) {
                if (variantManager.isSolrAvailable()) {
                    healthCheckResults.put(SOLR, OKAY);
                } else {
                    errorMsg.append(", unable to connect with solr, ");
                    healthCheckResults.put(SOLR, NOT_OKAY);
                }
            } else {
                healthCheckResults.put(SOLR, "solr not active in storage-configuration!");
            }
        } else {
            logger.info("HealthCheck results from cache at " + lastAccess.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            queryResult.setEvents(Collections.singletonList(new Event(Event.Type.WARNING, "HealthCheck results from cache at "
                    + lastAccess.format(DateTimeFormatter.ofPattern("HH:mm:ss")))));
        }

        queryResult.setResults(Arrays.asList(healthCheckResults));

        if (isHealthy()) {
            logger.info("HealthCheck : " + healthCheckResults.toString());
            return createOkResponse(queryResult);
        } else {
            logger.error("HealthCheck : " + healthCheckResults.toString());
            return createErrorResponse(errorMsg.toString(), queryResult);
        }
    }

    private boolean isHealthy() {
        return healthCheckResults.isEmpty() ? false : !healthCheckResults.values().stream().anyMatch(x -> x.equals(NOT_OKAY));
    }
}
