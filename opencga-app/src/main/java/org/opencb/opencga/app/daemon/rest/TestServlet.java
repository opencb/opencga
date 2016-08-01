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

package org.opencb.opencga.app.daemon.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Created by jacobo on 23/10/14.
 */

@Deprecated
@Path("/test")
public class TestServlet extends DaemonServlet {

    public TestServlet() {
        super();
        System.out.println("Construido TestServlet");
    }


    @GET
    @Path("/echo/{message}")
    @Produces("text/plain")
//    @ApiOperation(defaultValue = "Just to test the api")
    public Response echoGet(/*@ApiParam(defaultValue = "message", required = true)*/ @PathParam("message") String message) {
        System.out.println("Test message: " + message);
        return buildResponse(Response.ok(message));
    }

    @GET
    @Path("/hello")
    @Produces("text/plain")
//    @ApiOperation(defaultValue = "Just to test the api")
    public Response helloWorld() {
        System.out.println("Hello World ");
        return createOkResponse("Hello world");
    }
}
