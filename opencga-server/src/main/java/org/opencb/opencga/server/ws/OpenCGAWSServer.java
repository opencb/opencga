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

package org.opencb.opencga.server.ws;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.wordnik.swagger.annotations.ApiParam;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.cellbase.core.CellBaseConfiguration;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.ResponseBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;

@Path("/{version}")
@Produces(MediaType.APPLICATION_JSON)
public class OpenCGAWSServer {

    @DefaultValue("v1")
    @PathParam("version")
    @ApiParam(name = "version", value = "OpenCGA major version", allowableValues = "v1", defaultValue = "v1")
    protected String version;

    @DefaultValue("")
    @QueryParam("exclude")
    @ApiParam(name = "excluded fields", value = "Fields excluded in response. Whole JSON path e.g.: transcripts.id")
    protected String exclude;

    @DefaultValue("")
    @QueryParam("include")
    @ApiParam(name = "included fields", value = "Only fields included in response. Whole JSON path e.g.: transcripts.id")
    protected String include;

    @DefaultValue("")
    @QueryParam("sid")
    @ApiParam(value = "Session Id")
    protected String sessionId;

    protected UriInfo uriInfo;
    protected HttpServletRequest httpServletRequest;
    protected MultivaluedMap<String, String> params;

    protected String sessionIp;

    protected long startTime;
    protected long endTime;

    protected QueryOptions queryOptions;
    protected QueryResponse queryResponse;
    protected Query query;

    protected static ObjectWriter jsonObjectWriter;
    protected static ObjectMapper jsonObjectMapper;

    protected static Logger logger; // = LoggerFactory.getLogger(this.getClass());

//    @DefaultValue("true")
//    @QueryParam("metadata")
//    protected boolean metadata;

//    @DefaultValue("json")
//    @QueryParam("of")
//    protected String outputFormat;

    protected static CatalogManager catalogManager;
    protected static StorageManagerFactory storageManagerFactory;

    protected static CatalogConfiguration catalogConfiguration;

    static {

        Properties properties = new Properties();
        try {
            if(Config.getOpenCGAHome() == null || Config.getOpenCGAHome().isEmpty()) {
                InputStream is = OpenCGAWSServer.class.getClassLoader().getResourceAsStream("application.properties");
                properties.load(is);
                String openCGAHome = properties.getProperty("OPENCGA.INSTALLATION.DIR", Config.getOpenCGAHome());
                System.out.println("OpenCGA home set to: " + openCGAHome);
                Config.setOpenCGAHome(openCGAHome);
            } else {
                System.out.println("OpenCGA home set to: " + Config.getOpenCGAHome());
            }
        } catch (IOException e) {
            System.out.println("Error loading properties:\n" + e.getMessage());
            e.printStackTrace();
        }

        String logFile = null;
        try {
            org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();
            java.nio.file.Path logs = Paths.get(Config.getOpenCGAHome(), "logs");
            //Files.createDirectory(logs);
            PatternLayout layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} [%t] %-5p %c{1}:%L - %m%n");
            logFile = logs.resolve("server.log").toString();
            RollingFileAppender rollingFileAppender = new RollingFileAppender(layout, logFile, true);
            rollingFileAppender.setThreshold(Level.TRACE);
            rollingFileAppender.setMaxFileSize("10MB");
            rollingFileAppender.setMaxBackupIndex(10);
            rootLogger.setLevel(Level.TRACE);
            rootLogger.addAppender(rollingFileAppender);
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger = LoggerFactory.getLogger("org.opencb.opencga.server.ws.OpenCGAWSServer");
        logger.info("========================================================================");
        logger.info("| Starting OpenCGA-server, creating OpenCGAWSServer");
        logger.info("| This log must appear only once.");
        logger.info("|  * OpenCGA home set to: " + Config.getOpenCGAHome());
        logger.info("|  * Server logfile: " + logFile);

//        if (!openCGAHome.isEmpty() && Paths.get(openCGAHome).toFile().exists()) {
//            System.out.println("Using \"openCGAHome\" from the properties file");
//            Config.setOpenCGAHome(openCGAHome);
//        } else {
//            Config.setOpenCGAHome();
//            System.out.println("Using OpenCGA_HOME = " + Config.getOpenCGAHome());
//        }

        try {
            logger.info("|  * Reading StorageConfiguration");
            StorageConfiguration storageConfiguration = StorageConfiguration.load(new FileInputStream(Paths.get(Config.getOpenCGAHome(), "conf", "storage-configuration.yml").toFile()));
            logger.info("|  * Initializing StorageManagerFactory");
            storageManagerFactory = new StorageManagerFactory(storageConfiguration);
        } catch (IOException e) {
            logger.error("Error reading storage-configuration.yml file", e);
        }

        try {
            logger.info("|  * Reading CatalogConfiguration");
            CatalogConfiguration catalogConfiguration = CatalogConfiguration.load(new FileInputStream(Paths.get(Config.getOpenCGAHome(), "conf", "catalog-configuration.yml").toFile()));
            logger.info("|  * Initializing CatalogManager");
            catalogManager = new CatalogManager(catalogConfiguration);
            logger.info("========================================================================");
        } catch (IOException | CatalogException e) {
            logger.error("Error while creating CatalogManager", e);
        }

        jsonObjectMapper = new ObjectMapper();

        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixIn(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);

        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        jsonObjectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        jsonObjectWriter = jsonObjectMapper.writer();
    }

    public OpenCGAWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                           @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
//        this.startTime = System.currentTimeMillis();
        this.version = version;
        this.uriInfo = uriInfo;
        this.httpServletRequest = httpServletRequest;

        this.params = uriInfo.getQueryParameters();
//        this.sessionIp = httpServletRequest.getRemoteAddr();
//        System.out.println("sessionIp = " + sessionIp);
//        logger.debug(uriInfo.getRequestUri().toString());
//        this.queryOptions = null;

        startTime = System.currentTimeMillis();

//        queryResponse = new QueryResponse();
        queryOptions = new QueryOptions();
        query = new Query();

        parseParams();
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
                                           Query query, QueryOptions queryOptions) {
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            String param = entry.getKey();
            int indexOf = param.indexOf('.');
            param = indexOf > 0 ? param.substring(0, indexOf) : param;

            if (getParam.apply(param) != null) {
                query.put(entry.getKey(), entry.getValue());
            } else {
                queryOptions.add(param, entry.getValue());
            }

        }
    }

    public void parseParams() throws VersionException {
        if (version == null) {
            throw new VersionException("Version not valid: '" + version + "'");
        }

        /**
         * Check version parameter, must be: v1, v2, ... If 'latest' then is
         * converted to appropriate version
         */
        if (version.equalsIgnoreCase("latest")) {
            version = "v1";
            logger.info("Version 'latest' detected, setting version parameter to '{}'", version);
        }
        // TODO Valid OpenCGA versions need to be added configuration files
//        if (!cellBaseConfiguration.getVersion().equalsIgnoreCase(this.version)) {
//            logger.error("Version '{}' does not match configuration '{}'", this.version, cellBaseConfiguration.getVersion());
//            throw new VersionException("Version not valid: '" + version + "'");
//        }

        MultivaluedMap<String, String> multivaluedMap = uriInfo.getQueryParameters();
        queryOptions.put("metadata", (multivaluedMap.get("metadata") != null) ? multivaluedMap.get("metadata").get(0).equals("true") : true);

        if(exclude != null && !exclude.isEmpty()) {
            queryOptions.put("exclude", new LinkedList<>(Splitter.on(",").splitToList(exclude)));
        } else {
            queryOptions.put("exclude", (multivaluedMap.get("exclude") != null)
                    ? Splitter.on(",").splitToList(multivaluedMap.get("exclude").get(0))
                    : null);
        }

        if(include != null && !include.isEmpty()) {
            queryOptions.put("include", new LinkedList<>(Splitter.on(",").splitToList(include)));
        } else {
            queryOptions.put("include", (multivaluedMap.get("include") != null)
                    ? Splitter.on(",").splitToList(multivaluedMap.get("include").get(0))
                    : null);
        }

        // Now we add all the others QueryParams in the URL such as limit, of, sid, ...
        // 'sid' query param is excluded from QueryOptions object since is parsed in 'sessionId' attribute
        multivaluedMap.entrySet().stream()
                .filter(entry -> !queryOptions.containsKey(entry.getKey()))
                .filter(entry -> !entry.getKey().equals("sid"))
                .forEach(entry -> {
//                    logger.debug("Adding '{}' to queryOptions object", entry);
                    queryOptions.put(entry.getKey(), entry.getValue().get(0));
                });

        if (multivaluedMap.get("sid") != null) {
            queryOptions.put("sessionId", multivaluedMap.get("sid").get(0));
        }

        try {
            logger.info("URL: {}, queryOptions = {}", uriInfo.getAbsolutePath().toString(), jsonObjectWriter.writeValueAsString(queryOptions));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

//    protected QueryOptions getQueryOptions() {
//        if(queryOptions == null) {
//            this.queryOptions = new QueryOptions();
//            this.queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
//            if(!exclude.isEmpty()) {
//                queryOptions.put("exclude", Arrays.asList(exclude.split(",")));
//            }
//            if(!include.isEmpty()) {
//                queryOptions.put("include", Arrays.asList(include.split(",")));
//            }
//            queryOptions.put("metadata", metadata);
//        }
//        return queryOptions;
//    }

//    protected QueryOptions getAllQueryOptions() {
//        return getAllQueryOptions(null);
//    }

//    protected QueryOptions getAllQueryOptions(Collection<String> acceptedQueryOptions) {
//        return getAllQueryOptions(new HashSet<String>(acceptedQueryOptions));
//    }
//
//    protected QueryOptions getAllQueryOptions(Set<String> acceptedQueryOptions) {
//        QueryOptions queryOptions = this.getQueryOptions();
//        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
//            if (acceptedQueryOptions == null || acceptedQueryOptions.contains(entry.getKey())) {
//                if (!entry.getValue().isEmpty()) {
//                    Iterator<String> iterator = entry.getValue().iterator();
//                    StringBuilder sb = new StringBuilder(iterator.next());
//                    while (iterator.hasNext()) {
//                        sb.append(",").append(iterator.next());
//                    }
//                    queryOptions.add(entry.getKey(), sb.toString());
//                } else {
//                    queryOptions.add(entry.getKey(), null);
//                }
//            }
//        }
//        return queryOptions;
//    }

    @GET
    @Path("/help")
    public Response help() {
        return createOkResponse("No help available");
    }

    protected Response createErrorResponse(Exception e) {
        // First we print the exception in Server logs
        e.printStackTrace();

        // Now we prepare the response to client
        queryResponse = new QueryResponse();
        queryResponse.setTime(new Long(System.currentTimeMillis() - startTime).intValue());
        queryResponse.setApiVersion(version);
        queryResponse.setQueryOptions(queryOptions);
        queryResponse.setError(e.toString());

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
        queryResponse = new QueryResponse();
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

    protected Response buildResponse(ResponseBuilder responseBuilder) {
        return responseBuilder
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "x-requested-with, content-type")
                .build();
    }

    @GET
    @Path("/testIndices")
    public Response testIndices() {
        catalogManager.testIndices();
        return Response.ok("mira el log").build();
    }
}
