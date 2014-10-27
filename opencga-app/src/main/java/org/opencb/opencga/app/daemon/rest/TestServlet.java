package org.opencb.opencga.app.daemon.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Created by jacobo on 23/10/14.
 */

@Path("/test")
public class TestServlet extends DaemonServlet {

    public TestServlet() {
        super();
        System.out.println("Construido TestServlet");
    }


    @GET
    @Path("/echo/{message}")
    @Produces("text/plain")
//    @ApiOperation(value = "Just to test the api")
    public Response echoGet(/*@ApiParam(value = "message", required = true)*/ @PathParam("message") String message) {
        System.out.println("Test message: " + message);
        return buildResponse(Response.ok(message));
    }

    @GET
    @Path("/hello")
    @Produces("text/plain")
//    @ApiOperation(value = "Just to test the api")
    public Response helloWorld() {
        System.out.println("Hello World ");
        return createOkResponse("Hello world");
    }
}
