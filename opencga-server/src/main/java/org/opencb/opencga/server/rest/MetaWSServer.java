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
import org.opencb.commons.datastore.core.QueryResult;
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
    @Path("/status")
    @ApiOperation(httpMethod = "GET", value = "Database status.")
    public Response status() {

        QueryResult queryResult = new QueryResult();
        queryResult.setId("Status");
        queryResult.setDbTime(0);

        String storageEngineId;
        StringBuilder errorMsg = new StringBuilder();

        if (healthCheckResults.size() == 0 || !isHealthy() ||
                Duration.between(lastAccess, LocalTime.now()).getSeconds() > configuration.getHealthCheck().getInterval()) {

            logger.info("HealthCheck results without cache!");
            lastAccess = LocalTime.now();
            healthCheckResults.clear();

            try {
                if (catalogManager.getDatabaseStatus()) {
                    healthCheckResults.put("CatalogMongoDB", "OK");
                } else {
                    healthCheckResults.put("CatalogMongoDB", "KO");
                }
            } catch (Exception e) {
                healthCheckResults.put("CatalogMongoDB", "KO");
                errorMsg.append(e.getMessage());
            }

            try {
                storageEngineId = storageEngineFactory.getVariantStorageEngine().getStorageEngineId();
                healthCheckResults.put("VariantStorageId", storageEngineId);
            } catch (Exception e) {
                errorMsg.append(" No storageEngineId is set in configuration or Unable to initiate storage Engine, ").append(e.getMessage()).append(", ");
                healthCheckResults.put("VariantStorage", "KO");
            }

            try {
                storageEngineFactory.getVariantStorageEngine().testConnection();
                healthCheckResults.put("VariantStorage", "OK");
            } catch (Exception e) {
                healthCheckResults.put("VariantStorage", "KO");
                errorMsg.append(e.getMessage());
            }

            if (storageEngineFactory.getStorageConfiguration().getSearch().isActive()) {
                if (variantManager.isSolrAvailable()) {
                    healthCheckResults.put("Solr", "OK");
                } else {
                    errorMsg.append(", unable to connect with solr, ");
                    healthCheckResults.put("Solr", "KO");
                }
            } else {
                errorMsg.append(" solr is not active in storage configuration!");
                healthCheckResults.put("Solr", "");
            }
        } else {
            logger.info("HealthCheck results from cache!");
            queryResult.setWarningMsg("HealthCheck results from cache!");
        }

        queryResult.setResult(Arrays.asList(healthCheckResults));
        if (isHealthy()) {
            logger.info("HealthCheck : " + healthCheckResults.toString());
            return createOkResponse(queryResult);
        } else {
            logger.error("HealthCheck : " + healthCheckResults.toString());
            return createErrorResponse(errorMsg.toString(), queryResult);
        }
    }

    private boolean isHealthy() {
        if (healthCheckResults.size() > 0) {
            return !(healthCheckResults.get("CatalogMongoDB").equalsIgnoreCase("KO") ||
                    healthCheckResults.get("VariantStorage").equalsIgnoreCase("KO") ||
                    healthCheckResults.get("Solr").equalsIgnoreCase("KO"));
        } else {
            return false;
        }
    }
}
