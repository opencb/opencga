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

package org.opencb.opencga.storage.server.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.common.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenotypeJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opencb.opencga.core.common.JacksonUtils.getExternalOpencgaObjectMapper;

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
    protected static StorageEngineFactory storageEngineFactory;

    private static Logger privLogger;
    protected Logger logger;
    private static ObjectMapper jsonObjectMapper;
    private static ObjectWriter jsonObjectWriter;

    static {
        jsonObjectMapper = getExternalOpencgaObjectMapper();
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixIn(Genotype.class, GenotypeJsonMixin.class);

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

        defaultStorageEngine = storageConfiguration.getVariant().getDefaultEngine();

        // Only one StorageManagerFactory is needed, this acts as a simple Singleton pattern which improves the performance significantly
        if (storageEngineFactory == null) {
            privLogger.debug("Creating the StorageManagerFactory object");
            // TODO: We will need to pass catalog manager once storage starts doing things over catalog
            storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
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
