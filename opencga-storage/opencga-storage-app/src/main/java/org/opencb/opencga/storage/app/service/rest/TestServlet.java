package org.opencb.opencga.storage.app.service.rest;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

/**
 * Created by jacobo on 23/10/14.
 */

@Path("/test")
public class TestServlet extends DaemonServlet {

    public TestServlet(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
        System.out.println("Build TestServlet");
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
