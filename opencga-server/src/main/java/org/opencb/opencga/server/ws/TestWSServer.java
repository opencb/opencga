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


import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/{version}/test")
@Api(value = "test", position = 12, description = "test web services")
public class TestWSServer extends OpenCGAWSServer {

    public TestWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                        @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest);
    }

    //
//
//    @POST
//    @Consumes(MediaType.MULTIPART_FORM_DATA)
//    @Path("/echo")
//    @ApiOperation(value = "echo multipart")
//    public Response chunkUpload(@DefaultValue("") @FormDataParam("message") String message) {
//        return createOkResponse(message);
//    }

//    @GET
//    @Path("/{param}")
//    @ApiOperation(value="just to test the sample api")
//    public Response getMsg(@ApiParam(value="param",required=true)@PathParam("param") String msg ) {
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