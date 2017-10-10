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
import io.swagger.annotations.ApiParam;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Map;

@Path("/{apiVersion}/test")
@Api(value = "test", hidden = true, position = 12, description = "test web services")
public class TestWSServer extends OpenCGAWSServer {

    public TestWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/echo")
    @ApiOperation(value = "echo json")
    public Response formPost(@ApiParam(value = "json") Map<String, Object> json) {
//        System.out.println("Received message " + message);
        for (String s : json.keySet()) {
            Object o = json.get(s);
            if (o instanceof Map) {
                System.out.println("It is a map");
                for (Object key : ((Map) o).keySet()) {
                    System.out.println(key + " = " + json.get(key));
                }
            }
            System.out.println(s + " = " + json.get(s));
        }
        return buildResponse(Response.ok("Hello, it worked"));
    }


//    @GET
//    @Path("/{param}")
//    @ApiOperation(defaultValue="just to test the sample api")
//    public Response getMessage(@ApiParam(defaultValue="param",required=true)@PathParam("param") String msg ) {
//        String output = "Hello : " + msg;
//        return Response.status(200).entity(output).build();
//    }

    @GET
    @Path("/echo/{message}")
    @Produces("text/plain")
    @ApiOperation(value = "Just to test the api")
    public Response echoGet(@ApiParam(value = "message", required = true) @PathParam("message") String message) {
        return buildResponse(Response.ok(message));
    }
}