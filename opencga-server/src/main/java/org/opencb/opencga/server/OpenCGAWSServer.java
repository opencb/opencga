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

package org.opencb.opencga.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;


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
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.json.AlignmentDifferenceJsonMixin;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceEntryJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.ResponseBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;

@Path("/")
public class OpenCGAWSServer {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected String version;
    protected UriInfo uriInfo;
    protected String sessionIp;

    // Common input arguments
    protected MultivaluedMap<String, String> params;
    private QueryOptions queryOptions;
    protected QueryResponse queryResponse;

    // Common output members
    protected long startTime;
    protected long endTime;

    protected static ObjectWriter jsonObjectWriter;
    protected static ObjectMapper jsonObjectMapper;

    //Common query params
    @DefaultValue("")
    @QueryParam("sid")
    protected String sessionId;

    @DefaultValue("json")
    @QueryParam("of")
    protected String outputFormat;

    @DefaultValue("")
    @QueryParam("exclude")
    protected String exclude;

    @DefaultValue("")
    @QueryParam("include")
    protected String include;

    @DefaultValue("true")
    @QueryParam("metadata")
    protected Boolean metadata;

    protected static CatalogManager catalogManager;
    protected static StorageManagerFactory storageManagerFactory;

    static {

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

        InputStream is = OpenCGAWSServer.class.getClassLoader().getResourceAsStream("application.properties");
        String openCGAHome = "";
        System.out.println("Default opencga_home " + Config.getOpenCGAHome());
        try {
            Properties properties = new Properties();
            properties.load(is);
            openCGAHome = properties.getProperty("OPENCGA.INSTALLATION.DIR", "");
        } catch (IOException e) {
            System.out.println("Error loading properties");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        if (!openCGAHome.isEmpty() && Paths.get(openCGAHome).toFile().exists()) {
            System.out.println("Using \"openCGAHome\" from the properties file");
            Config.setOpenCGAHome(openCGAHome);
        } else {
            Config.setOpenCGAHome();
            System.out.println("Using OpenCGA_HOME = " + Config.getOpenCGAHome());
        }

        try {
            StorageConfiguration storageConfiguration = StorageConfiguration.findAndLoad();
            storageManagerFactory = new StorageManagerFactory(storageConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            catalogManager = new CatalogManager(Config.getCatalogProperties());
        } catch (CatalogIOException | CatalogDBException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        jsonObjectMapper = new ObjectMapper();

        jsonObjectMapper.addMixIn(VariantSourceEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);

        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        jsonObjectWriter = jsonObjectMapper.writer();

    }

    public OpenCGAWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        this.startTime = System.currentTimeMillis();
        this.version = version;
        this.uriInfo = uriInfo;
        this.params = uriInfo.getQueryParameters();
        logger.debug(uriInfo.getRequestUri().toString());
        this.queryOptions = null;
        this.sessionIp = httpServletRequest.getRemoteAddr();
    }

    protected QueryOptions getQueryOptions() {
        if(queryOptions == null) {
            this.queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            if(!exclude.isEmpty()) {
                queryOptions.put("exclude", Arrays.asList(exclude.split(",")));
            }
            if(!include.isEmpty()) {
                queryOptions.put("include", Arrays.asList(include.split(",")));
            }
            queryOptions.put("metadata", metadata);
        }
        return queryOptions;
    }

    protected QueryOptions getAllQueryOptions() {
        return getAllQueryOptions(null);
    }

    protected QueryOptions getAllQueryOptions(Collection<String> acceptedQueryOptions) {
        return getAllQueryOptions(new HashSet<String>(acceptedQueryOptions));
    }

    protected QueryOptions getAllQueryOptions(Set<String> acceptedQueryOptions) {
        QueryOptions queryOptions = this.getQueryOptions();
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            if (acceptedQueryOptions == null || acceptedQueryOptions.contains(entry.getKey())) {
                if (!entry.getValue().isEmpty()) {
                    Iterator<String> iterator = entry.getValue().iterator();
                    StringBuilder sb = new StringBuilder(iterator.next());
                    while (iterator.hasNext()) {
                        sb.append(",").append(iterator.next());
                    }
                    queryOptions.add(entry.getKey(), sb.toString());
                } else {
                    queryOptions.add(entry.getKey(), null);
                }
            }
        }
        return queryOptions;
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
        queryResponse.setQueryOptions(getQueryOptions());

        // Guarantee that the QueryResponse object contains a list of results
        List list;
        if (obj instanceof List) {
            list = (List) obj;
        } else {
            list = new ArrayList();
            list.add(obj);
        }
        queryResponse.setResponse(list);

        switch (outputFormat.toLowerCase()) {
            case "json":
                return createJsonResponse(queryResponse);
            case "xml":
//                return createXmlResponse(queryResponse);
            default:
                return buildResponse(Response.ok());
        }


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
