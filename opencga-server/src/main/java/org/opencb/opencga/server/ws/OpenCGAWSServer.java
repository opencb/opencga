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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;


import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.storage.core.alignment.json.AlignmentDifferenceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceEntryJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.ResponseBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;

@Path("/{version}")
@Produces("text/plain")
public class OpenCGAWSServer {

    @DefaultValue("")
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
    protected String sessionId;

    protected UriInfo uriInfo;
    protected HttpServletRequest httpServletRequest;
    protected MultivaluedMap<String, String> params;

    protected String sessionIp;

    protected long startTime;
    protected long endTime;

    protected QueryOptions queryOptions;
    protected QueryResponse queryResponse;

    protected static ObjectWriter jsonObjectWriter;
    protected static ObjectMapper jsonObjectMapper;

    protected static Logger logger; // = LoggerFactory.getLogger(this.getClass());


//    @DefaultValue("-1")
//    @QueryParam("limit")
//    @ApiParam(name = "limit", value = "Max number of results to be returned. No limit applied when -1. No limit is set by default.")
//    protected int limit;

//    @DefaultValue("-1")
//    @QueryParam("skip")
//    @ApiParam(name = "skip", value = "Number of results to be skipped. No skip applied when -1. No skip by default.")
//    protected int skip;

//    @DefaultValue("true")
//    @QueryParam("metadata")
//    protected boolean metadata;

//    @DefaultValue("json")
//    @QueryParam("of")
//    protected String outputFormat;

    protected static CatalogManager catalogManager;

    static {
        logger = LoggerFactory.getLogger("org.opencb.opencga.server.ws.OpenCGAWSServer");
//        InputStream is = OpenCGAWSServer.class.getClassLoader().getResourceAsStream("catalog.properties");
//        properties = new Properties();
//        try {
//            properties.load(is);
//            System.out.println("catalog.properties");
//            System.out.println(CatalogManager.CATALOG_DB_HOSTS + " " + properties.getProperty(CatalogManager.CATALOG_DB_HOSTS));
//            System.out.println(CatalogManager.CATALOG_DB_DATABASE + " " + properties.getProperty(CatalogManager.CATALOG_DB_DATABASE));
//            System.out.println(CatalogManager.CATALOG_DB_USER + " " + properties.getProperty(CatalogManager.CATALOG_DB_USER));
//            System.out.println(CatalogManager.CATALOG_DB_PASSWORD + " " + properties.getProperty(CatalogManager.CATALOG_DB_PASSWORD));
//            System.out.println(CatalogManager.CATALOG_MAIN_ROOTDIR + " " + properties.getProperty(CatalogManager.CATALOG_MAIN_ROOTDIR));
//
//        } catch (IOException e) {
//            System.out.println("Error loading properties");
//            System.out.println(e.getMessage());
//            e.printStackTrace();
//        }

        try {
            Properties properties = new Properties();
            InputStream is = OpenCGAWSServer.class.getClassLoader().getResourceAsStream("application.properties");
            properties.load(is);
            String openCGAHome = properties.getProperty("OPENCGA.INSTALLATION.DIR", Config.getOpenCGAHome());
            System.out.println("OpenCGA home set to: " + openCGAHome);
            if(Config.getOpenCGAHome() == null || Config.getOpenCGAHome().isEmpty()) {
                Config.setOpenCGAHome(openCGAHome);
            }
        } catch (IOException e) {
            System.out.println("Error loading properties:\n" + e.getMessage());
            e.printStackTrace();
        }

//        if (!openCGAHome.isEmpty() && Paths.get(openCGAHome).toFile().exists()) {
//            System.out.println("Using \"openCGAHome\" from the properties file");
//            Config.setOpenCGAHome(openCGAHome);
//        } else {
//            Config.setOpenCGAHome();
//            System.out.println("Using OpenCGA_HOME = " + Config.getOpenCGAHome());
//        }

        try {
            catalogManager = new CatalogManager(Config.getCatalogProperties());
        } catch (CatalogIOException | CatalogDBException e) {
            System.out.println("ERROR when creating CatalogManager: " + e.getMessage());
            e.printStackTrace();
        }

        jsonObjectMapper = new ObjectMapper();

//        jsonObjectMapper.addMixIn(VariantSourceEntry.class, VariantSourceEntryJsonMixin.class);
//        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
//        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
//        jsonObjectMapper.addMixIn(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);

        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        jsonObjectWriter = jsonObjectMapper.writer();
    }

    public OpenCGAWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                           @Context HttpServletRequest httpServletRequest) throws IOException {
        this.startTime = System.currentTimeMillis();
        this.version = version;
        this.uriInfo = uriInfo;
        this.httpServletRequest = httpServletRequest;
        this.params = uriInfo.getQueryParameters();
//        logger.debug(uriInfo.getRequestUri().toString());
        this.queryOptions = null;
//        this.sessionIp = httpServletRequest.getRemoteAddr();
//        System.out.println("sessionIp = " + sessionIp);
    }

    protected QueryOptions getQueryOptions() {
        if(queryOptions == null) {
            this.queryOptions = new QueryOptions();
//            this.queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
//            if(!exclude.isEmpty()) {
//                queryOptions.put("exclude", Arrays.asList(exclude.split(",")));
//            }
//            if(!include.isEmpty()) {
//                queryOptions.put("include", Arrays.asList(include.split(",")));
//            }
//            queryOptions.put("metadata", metadata);
        }
        return queryOptions;
    }

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

    protected Response createErrorResponse(Object o) {
        QueryResult<ObjectMap> result = new QueryResult();
        result.setErrorMsg(o.toString());
        return createOkResponse(result);
    }

    protected Response createOkResponse(Object obj) {
        queryResponse = new QueryResponse();
        endTime = System.currentTimeMillis() - startTime;
        queryResponse.setTime(new Long(endTime - startTime).intValue());
        queryResponse.setApiVersion(version);
//        queryResponse.setQueryOptions(getQueryOptions());

        // Guarantee that the QueryResponse object contains a list of results
        List list;
        if (obj instanceof List) {
            list = (List) obj;
        } else {
            list = new ArrayList();
            list.add(obj);
        }
        queryResponse.setResponse(list);

//        switch (outputFormat.toLowerCase()) {
//            case "json":
//            return createJsonResponse(queryResponse);
//            case "xml":
////                return createXmlResponse(queryResponse);
//            default:
//            return buildResponse(Response.ok());
//        }

        return createJsonResponse(queryResponse);

    }

    protected Response createJsonResponse(Object object) {
        try {
            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(object), MediaType.APPLICATION_JSON_TYPE));
        } catch (JsonProcessingException e) {
            System.out.println("object = " + object);
            System.out.println("((QueryResponse)object).getResponse() = " + ((QueryResponse) object).getResponse());

            System.out.println("e = " + e);
            System.out.println("e.getMessage() = " + e.getMessage());
            return createErrorResponse("Error parsing QueryResponse object:\n" + Arrays.toString(e.getStackTrace()));
        }
    }

    //Response methods
    protected Response createOkResponse(Object o1, MediaType o2) {
        return buildResponse(Response.ok(o1, o2));
    }

    protected Response createOkResponse(Object o1, MediaType o2, String fileName) {
        return buildResponse(Response.ok(o1, o2).header("content-disposition", "attachment; filename =" + fileName));
    }

    protected Response buildResponse(ResponseBuilder responseBuilder) {
        return responseBuilder.header("Access-Control-Allow-Origin", "*").header("Access-Control-Allow-Headers", "x-requested-with, content-type").build();
    }
}
