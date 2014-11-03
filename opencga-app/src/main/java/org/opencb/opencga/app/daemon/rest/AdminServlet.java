package org.opencb.opencga.app.daemon.rest;

import org.apache.catalina.LifecycleException;
import org.opencb.opencga.app.daemon.OpenCGADaemon;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Created by jacobo on 23/10/14.
 */

@Path("/admin")
public class AdminServlet extends DaemonServlet {

    public AdminServlet() {
        super();
        System.out.println("Construido AdminServlet");
    }

    @GET
    @Path("/stop")
    @Produces("text/plain")
    public Response stop() throws LifecycleException {
        System.out.println("Stop");

        OpenCGADaemon.getDaemon().stop();
        return createOkResponse("Stop");
    }

    @GET
    @Path("/status")
    @Produces("text/plain")
    public Response status() {
        System.out.println("Status");
        return createOkResponse("Status");
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
