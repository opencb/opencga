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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.server.RestServer;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * Created on 03/09/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Path("/admin")
public class AdminRestWebService {

    @DefaultValue("v1")
    @QueryParam("apiVersion")
    protected String apiVersion;

    private static RestServer server;

    public AdminRestWebService(@PathParam("apiVersion") String version, @Context UriInfo uriInfo,
                               @Context HttpServletRequest httpServletRequest, @Context ServletContext context) throws IOException {
//        super(version, uriInfo, httpServletRequest, context);
        System.out.println("Build AdminWSServer");
    }


    @GET
    @Path("/stop")
    @Produces("text/plain")
    public Response stop() {
        try {
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        OpenCGAStorageService.getInstance().stop();
//        try {
//            RestStorageServer.stop();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        return createOkResponse("bye!");
    }

    public static RestServer getServer() {
        return server;
    }

    public static void setServer(RestServer server) {
        AdminRestWebService.server = server;
    }

    protected Response createOkResponse(Object obj) {
        QueryResponse queryResponse = new QueryResponse();
        queryResponse.setTime(new Long(0).intValue());
        queryResponse.setApiVersion(apiVersion);
//        queryResponse.setQueryOptions(queryOptions);

        // Guarantee that the QueryResponse object contains a coll of results
        List coll;
        if (obj instanceof List) {
            coll = (List) obj;
        } else {
            coll = new ArrayList();
            coll.add(obj);
        }
        queryResponse.setResponse(coll);

        try {
            ObjectMapper jsonObjectMapper = getDefaultObjectMapper();
            return buildResponse(Response.ok(jsonObjectMapper.writer().writeValueAsString(queryResponse), MediaType.APPLICATION_JSON_TYPE));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

//        switch (outputFormat.toLowerCase()) {
//            case "json":
//                return createJsonResponse(queryResponse);
//            case "xml":
////                return createXmlResponse(queryResponse);
//            default:
//                return buildResponse(Response.ok());
//        }
        return null;
    }

    protected Response buildResponse(Response.ResponseBuilder responseBuilder) {
        return responseBuilder
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "x-requested-with, content-type")
                .build();
    }
}
