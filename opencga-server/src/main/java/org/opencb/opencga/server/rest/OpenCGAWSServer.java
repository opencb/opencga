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

package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Splitter;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.json.AlignmentDifferenceJsonMixin;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.io.json.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.GenotypeJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@ApplicationPath("/")
@Path("/{version}")
@Produces(MediaType.APPLICATION_JSON)
public class OpenCGAWSServer {

    @DefaultValue("v1")
    @PathParam("version")
    @ApiParam(name = "version", value = "OpenCGA major version", allowableValues = "v1", defaultValue = "v1")
    protected String version;

//    @DefaultValue("")
//    @QueryParam("exclude")
//    @ApiParam(name = "exclude", value = "Fields excluded in response. Whole JSON path.")
    protected String exclude;

//    @DefaultValue("")
//    @QueryParam("include")
//    @ApiParam(name = "include", value = "Only fields included in response. Whole JSON path.")
    protected String include;

//    @DefaultValue("-1")
//    @QueryParam("limit")
//    @ApiParam(name = "limit", value = "Maximum number of documents to be returned.")
    protected int limit;

//    @DefaultValue("0")
//    @QueryParam("skip")
//    @ApiParam(name = "skip", value = "Number of documents to be skipped when querying for data.")
    protected long skip;

    @DefaultValue("")
    @QueryParam("sid")
    @ApiParam(value = "Session Id")
    protected String sessionId;

    protected UriInfo uriInfo;
    protected HttpServletRequest httpServletRequest;
    protected MultivaluedMap<String, String> params;

    protected String sessionIp;

    protected long startTime;

    protected Query query;
    protected QueryOptions queryOptions;

    protected static ObjectWriter jsonObjectWriter;
    protected static ObjectMapper jsonObjectMapper;

    protected static Logger logger; // = LoggerFactory.getLogger(this.getClass());

//    @DefaultValue("true")
//    @QueryParam("metadata")
//    protected boolean metadata;


    protected static AtomicBoolean initialized;

    protected static CatalogConfiguration catalogConfiguration;
    protected static CatalogManager catalogManager;

    protected static StorageConfiguration storageConfiguration;
    protected static StorageManagerFactory storageManagerFactory;

    private static final int DEFAULT_LIMIT = 2000;
    private static final int MAX_LIMIT = 5000;

    static {
        initialized = new AtomicBoolean(false);

        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixIn(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        jsonObjectWriter = jsonObjectMapper.writer();


        //Disable MongoDB useless logging
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.cluster").setLevel(Level.WARN);
        org.apache.log4j.Logger.getLogger("org.mongodb.driver.connection").setLevel(Level.WARN);
    }


    public OpenCGAWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        this(uriInfo.getPathParameters().getFirst("version"), uriInfo, httpServletRequest);
    }

    public OpenCGAWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest)
            throws IOException, VersionException {
        this.version = version;
        this.uriInfo = uriInfo;
        this.httpServletRequest = httpServletRequest;

        this.params = uriInfo.getQueryParameters();

        // This is only executed the first time to initialize configuration and some variables
        if (initialized.compareAndSet(false, true)) {
            init();
        }

        query = new Query();
        queryOptions = new QueryOptions();

        parseParams();

        // take the time for calculating the whole duration of the call
        startTime = System.currentTimeMillis();
    }

    private void init() {
        logger = LoggerFactory.getLogger("org.opencb.opencga.server.rest.OpenCGAWSServer");
        logger.info("========================================================================");
        logger.info("| Starting OpenCGA REST server, initializing OpenCGAWSServer");
        logger.info("| This message must appear only once.");

        // We must load the configuration files and init catalogManager, storageManagerFactory and Logger only the first time.
        // We first read 'config-dir' parameter passed
        ServletContext context = httpServletRequest.getServletContext();
        String configDirString = context.getInitParameter("config-dir");
        if (StringUtils.isEmpty(configDirString)) {
            // If not environment variable then we check web.xml parameter
            if (StringUtils.isNotEmpty(context.getInitParameter("OPENCGA_HOME"))) {
                configDirString = context.getInitParameter("OPENCGA_HOME") + "/conf";
            } else if (StringUtils.isNotEmpty(System.getenv("OPENCGA_HOME"))) {
                // If not exists then we try the environment variable OPENCGA_HOME
                configDirString = System.getenv("OPENCGA_HOME") + "/conf";
            } else {
                logger.error("No valid configuration directory provided!");
            }
        }

        // Check and execute the init methods
        java.nio.file.Path configDirPath = Paths.get(configDirString);
        if (configDirPath != null && Files.exists(configDirPath) && Files.isDirectory(configDirPath)) {
            logger.info("|  * Configuration folder: '{}'", configDirPath.toString());
            initOpenCGAObjects(configDirPath);

            // Required for reading the analysis.properties file.
            // TODO: Remove when analysis.properties is totally migrated to configuration.yml
            Config.setOpenCGAHome(configDirPath.getParent().toString());

            // TODO use configuration.yml for getting the server.log, for now is hardcoded
            logger.info("|  * Server logfile: " + configDirPath.getParent().resolve("logs").resolve("server.log"));
            initLogger(configDirPath.getParent().resolve("logs"));
        } else {
            logger.error("No valid configuration directory provided: '{}'", configDirPath.toString());
        }

        logger.info("========================================================================\n");
    }

    /**
     * This method loads OpenCGA configuration files and initialize CatalogManager and StorageManagerFactory.
     * This must be only executed once.
     * @param configDir directory containing the configuration files
     */
    private void initOpenCGAObjects(java.nio.file.Path configDir) {
        try {
            logger.info("|  * Catalog configuration file: '{}'", configDir.toFile().getAbsolutePath() + "/catalog-configuration.yml");
            catalogConfiguration = CatalogConfiguration
                    .load(new FileInputStream(new File(configDir.toFile().getAbsolutePath() + "/catalog-configuration.yml")));
            catalogManager = new CatalogManager(catalogConfiguration);
            // TODO think about this
            if (!catalogManager.existsCatalogDB()) {
//                logger.info("|  * Catalog database created: '{}'", catalogConfiguration.getDatabase().getDatabase());
                logger.info("|  * Catalog database created: '{}'", catalogManager.getCatalogDatabase());
                catalogManager.installCatalogDB();
            }

            logger.info("|  * Storage configuration file: '{}'", configDir.toFile().getAbsolutePath() + "/storage-configuration.yml");
            storageConfiguration = StorageConfiguration
                    .load(new FileInputStream(new File(configDir.toFile().getAbsolutePath() + "/storage-configuration.yml")));
            storageManagerFactory = StorageManagerFactory.get(storageConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CatalogException e) {
            logger.error("Error while creating CatalogManager", e);
        }
    }

    private void initLogger(java.nio.file.Path logs) {
        try {
            org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();
            PatternLayout layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} [%t] %-5p %c{1}:%L - %m%n");
            String logFile = logs.resolve("server.log").toString();
            RollingFileAppender rollingFileAppender = new RollingFileAppender(layout, logFile, true);
            rollingFileAppender.setThreshold(Level.DEBUG);
            rollingFileAppender.setMaxFileSize("20MB");
            rollingFileAppender.setMaxBackupIndex(10);
            rootLogger.setLevel(Level.TRACE);
            rootLogger.addAppender(rollingFileAppender);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Builds the query and the queryOptions based on the query parameters.
     *
     * @param params Map of parameters.
     * @param getParam Method that returns the QueryParams object based on the key.
     * @param query Query where parameters parsing the getParam function will be inserted.
     * @param queryOptions QueryOptions where parameters not parsing the getParam function will be inserted.
     */
    protected static void parseQueryParams(Map<String, List<String>> params,
                                           Function<String, org.opencb.commons.datastore.core.QueryParam> getParam,
                                           ObjectMap query, QueryOptions queryOptions) {
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            String param = entry.getKey();
            int indexOf = param.indexOf('.');
            param = indexOf > 0 ? param.substring(0, indexOf) : param;

            if (getParam.apply(param) != null) {
                query.put(entry.getKey(), entry.getValue().get(0));
            } else {
                queryOptions.add(param, entry.getValue().get(0));
            }

            // Exceptions
            if (param.equalsIgnoreCase("status")) {
                query.put("status.name", entry.getValue().get(0));
                query.remove("status");
                queryOptions.remove("status");
            }

            if (param.equalsIgnoreCase("sid")) {
                query.remove("sid");
                queryOptions.remove("sid");
            }
        }
        logger.debug("parseQueryParams: Query {}, queryOptions {}", query.safeToString(), queryOptions.safeToString());
    }

    private void parseParams() throws VersionException {
        // If by any reason 'version' is null we try to read it from the URI path, if not present an Exeception is thrown
        if (version == null) {
            if (uriInfo.getPathParameters().containsKey("version")) {
                logger.warn("Setting 'version' from UriInfo object");
                this.version = uriInfo.getPathParameters().getFirst("version");
            } else {
                throw new VersionException("Version not valid: '" + version + "'");
            }
        }

         // Check version parameter, must be: v1, v2, ... If 'latest' then is converted to appropriate version.
        if (version.equalsIgnoreCase("latest")) {
            logger.info("Version 'latest' detected, setting 'version' parameter to 'v1'");
            version = "v1";
        }


        MultivaluedMap<String, String> multivaluedMap = uriInfo.getQueryParameters();
        queryOptions.put("metadata", (multivaluedMap.get("metadata") != null) ? multivaluedMap.get("metadata").get(0).equals("true") : true);

        String limitStr = multivaluedMap.getFirst(QueryOptions.LIMIT);
        if (StringUtils.isNotEmpty(limitStr)) {
            limit = Integer.parseInt(limitStr);
        }
        queryOptions.put(QueryOptions.LIMIT, (limit > 0) ? Math.min(limit, MAX_LIMIT) : DEFAULT_LIMIT);

        String skip = multivaluedMap.getFirst(QueryOptions.SKIP);
        if (skip != null) {
            this.skip = Integer.parseInt(skip);
            queryOptions.put(QueryOptions.SKIP, this.skip);
        }

        parseIncludeExclude(multivaluedMap, QueryOptions.EXCLUDE, exclude);
        parseIncludeExclude(multivaluedMap, QueryOptions.INCLUDE, include);

        // Now we add all the others QueryParams in the URL such as limit, of, sid, ...
        // 'sid' query param is excluded from QueryOptions object since is parsed in 'sessionId' attribute
        multivaluedMap.entrySet().stream()
                .filter(entry -> !queryOptions.containsKey(entry.getKey()))
                .filter(entry -> !entry.getKey().equals("sid"))
                .forEach(entry -> {
//                    logger.debug("Adding '{}' to queryOptions object", entry);
                    queryOptions.put(entry.getKey(), entry.getValue().get(0));
                });

//        if (multivaluedMap.get("sid") != null) {
//            queryOptions.put("sessionId", multivaluedMap.get("sid").get(0));
//        }

        try {
            logger.info("URL: {}, queryOptions = {}", uriInfo.getAbsolutePath().toString(), jsonObjectWriter.writeValueAsString(queryOptions));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private void parseIncludeExclude(MultivaluedMap<String, String> multivaluedMap, String key, String value) {
        if(value != null && !value.isEmpty()) {
            queryOptions.put(key, new LinkedList<>(Splitter.on(",").splitToList(value)));
        } else {
            queryOptions.put(key, (multivaluedMap.get(key) != null)
                    ? Splitter.on(",").splitToList(multivaluedMap.get(key).get(0))
                    : null);
        }
    }


    @Deprecated
    @GET
    @Path("/help")
    @ApiOperation(value = "Help", position = 1)
    public Response help() {
        return createOkResponse("No help available");
    }

    protected Response createErrorResponse(Exception e) {
        // First we print the exception in Server logs
        e.printStackTrace();

        // Now we prepare the response to client
        QueryResponse queryResponse = new QueryResponse();
        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
        queryResponse.setApiVersion(version);
        queryResponse.setQueryOptions(queryOptions);
        queryResponse.setError(e.getMessage());

        QueryResult<ObjectMap> result = new QueryResult();
        result.setWarningMsg("Future errors will ONLY be shown in the QueryResponse body");
        result.setErrorMsg("DEPRECATED: " + e.toString());
        queryResponse.setResponse(Arrays.asList(result));

        return Response.fromResponse(createJsonResponse(queryResponse))
                .status(Response.Status.INTERNAL_SERVER_ERROR).build();
//        return createOkResponse(result);
    }

//    protected Response createErrorResponse(String o) {
//        QueryResult<ObjectMap> result = new QueryResult();
//        result.setErrorMsg(o.toString());
//        return createOkResponse(result);
//    }

    protected Response createErrorResponse(String method, String errorMessage) {
        try {
            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(new ObjectMap("error", errorMessage)), MediaType.APPLICATION_JSON_TYPE));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return buildResponse(Response.ok("{\"error\":\"Error parsing json error\"}", MediaType.APPLICATION_JSON_TYPE));
    }

    protected Response createOkResponse(Object obj) {
        QueryResponse queryResponse = new QueryResponse();
        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
        queryResponse.setApiVersion(version);
        queryResponse.setQueryOptions(queryOptions);

        // Guarantee that the QueryResponse object contains a list of results
        List list;
        if (obj instanceof List) {
            list = (List) obj;
        } else {
            list = new ArrayList();
            list.add(obj);
        }
        queryResponse.setResponse(list);

        return createJsonResponse(queryResponse);
    }

    //Response methods
    protected Response createOkResponse(Object o1, MediaType o2) {
        return buildResponse(Response.ok(o1, o2));
    }

    protected Response createOkResponse(Object o1, MediaType o2, String fileName) {
        return buildResponse(Response.ok(o1, o2).header("content-disposition", "attachment; filename =" + fileName));
    }


    protected Response createJsonResponse(QueryResponse queryResponse) {
        try {
            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(queryResponse), MediaType.APPLICATION_JSON_TYPE));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.error("Error parsing queryResponse object");
            return createErrorResponse("", "Error parsing QueryResponse object:\n" + Arrays.toString(e.getStackTrace()));
        }
    }

    protected Response buildResponse(Response.ResponseBuilder responseBuilder) {
        return responseBuilder
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "x-requested-with, content-type")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                .build();
    }

}
