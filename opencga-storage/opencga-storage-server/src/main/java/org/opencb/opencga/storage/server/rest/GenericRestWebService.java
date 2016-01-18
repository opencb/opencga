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

package org.opencb.opencga.storage.server.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.*;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.json.AlignmentDifferenceJsonMixin;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.io.json.*;
import org.opencb.opencga.storage.server.common.AuthManager;
import org.opencb.opencga.storage.server.common.DefaultAuthManager;
import org.opencb.opencga.storage.server.common.exceptions.NotAuthorizedHostException;
import org.opencb.opencga.storage.server.common.exceptions.NotAuthorizedUserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by imedina on 23/10/14.
 */
public class GenericRestWebService {

    @DefaultValue("json")
    @QueryParam("of")
    protected String outputFormat;

    @DefaultValue("true")
    @QueryParam("metadata")
    protected Boolean metadata;

    @DefaultValue("v1")
    @QueryParam("version")
    protected String version;

    protected final String sessionIp;
    protected final UriInfo uriInfo;
    private final long startTime;
    protected QueryOptions queryOptions;
    protected MultivaluedMap<String, String> params;
    protected static StorageConfiguration storageConfiguration;

    protected static String defaultStorageEngine;
    protected static StorageManagerFactory storageManagerFactory;

    protected static AuthManager authManager;

    protected static Set<String> authorizedHosts;

    private static Logger privLogger;
    protected Logger logger;
    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;

    static {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.addMixIn(StudyEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);
        jsonObjectMapper.addMixIn(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        jsonObjectWriter = jsonObjectMapper.writer();

        privLogger = LoggerFactory.getLogger("org.opencb.opencga.storage.server.rest.GenericRestWebService");
    }

    public GenericRestWebService(@PathParam("version") String version, @Context UriInfo uriInfo,
                                 @Context HttpServletRequest httpServletRequest, @Context ServletContext context) throws IOException {
        this.startTime = System.currentTimeMillis();
        this.version = version;
        this.uriInfo = uriInfo;
        this.params = uriInfo.getQueryParameters();
        this.queryOptions = new QueryOptions(params, true);
        this.sessionIp = httpServletRequest.getRemoteAddr();

        logger = LoggerFactory.getLogger(this.getClass());

        defaultStorageEngine = storageConfiguration.getDefaultStorageEngineId();

        // Only one StorageManagerFactory is needed, this acts as a simple Singleton pattern which improves the performance significantly
        if (storageManagerFactory == null) {
            privLogger.debug("Creating the StorageManagerFactory object");
            storageManagerFactory = new StorageManagerFactory(storageConfiguration);
        }

        if (authorizedHosts == null) {
            privLogger.debug("Creating the authorizedHost HashSet");
            authorizedHosts = new HashSet<>(storageConfiguration.getServer().getAuthorizedHosts());
        }

        if (authManager == null) {
            try {
                if (StringUtils.isNotEmpty(context.getInitParameter("authManager"))) {
                    privLogger.debug("Loading AuthManager in {} from {}", this.getClass(), context.getInitParameter("authManager"));
                    authManager = (AuthManager) Class.forName(context.getInitParameter("authManager")).newInstance();
                } else {
                    privLogger.debug("Loading DefaultAuthManager in {} from {}", this.getClass(), DefaultAuthManager.class);
                    authManager = new DefaultAuthManager();
                }
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    protected void checkAuthorizedHosts(Query query, String ip) throws NotAuthorizedHostException, NotAuthorizedUserException {
        if (authorizedHosts.contains("0.0.0.0") || authorizedHosts.contains("*") || authorizedHosts.contains(ip)) {
            authManager.checkPermission(query, "");
        } else {
            throw new NotAuthorizedHostException("No queries are allowed from " + ip);
        }
    }

    protected Response createJsonResponse(Object object) {
        try {
            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(object), MediaType.APPLICATION_JSON_TYPE));
        } catch (JsonProcessingException e) {
            return createErrorResponse("Error parsing QueryResponse object:\n" + Arrays.toString(e.getStackTrace()));
        }
    }

    protected Response createErrorResponse(Object o) {
        QueryResult<ObjectMap> result = new QueryResult<>();
        result.setErrorMsg(o.toString());
        System.out.println("ERROR" + o.toString());
        return createOkResponse(result);
    }


    protected Response createOkResponse(Object obj) {
        QueryResponse queryResponse = new QueryResponse();
        long endTime = System.currentTimeMillis() - startTime;
        queryResponse.setTime(new Long(endTime - startTime).intValue());
        queryResponse.setApiVersion(version);
        queryResponse.setQueryOptions(queryOptions);

        // Guarantee that the QueryResponse object contains a coll of results
        List coll;
        if (obj instanceof List) {
            coll = (List) obj;
        } else {
            coll = new ArrayList();
            coll.add(obj);
        }
        queryResponse.setResponse(coll);

        switch (outputFormat.toLowerCase()) {
            case "json":
                return createJsonResponse(queryResponse);
            case "xml":
//                return createXmlResponse(queryResponse);
            default:
                return buildResponse(Response.ok());
        }
    }

    //Response methods
    protected Response createOkResponse(Object o1, MediaType o2) {
        return buildResponse(Response.ok(o1, o2));
    }

    protected Response createOkResponse(Object o1, MediaType o2, String fileName) {
        return buildResponse(Response.ok(o1, o2).header("content-disposition", "attachment; filename =" + fileName));
    }

    protected Response buildResponse(Response.ResponseBuilder responseBuilder) {
        return responseBuilder
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "x-requested-with, content-type")
                .build();
    }

    public static void setStorageConfiguration(StorageConfiguration storageConfiguration) {
        GenericRestWebService.storageConfiguration = storageConfiguration;
    }

}
