package org.opencb.opencga.storage.app.service.rest;

import org.opencb.opencga.storage.app.service.OpenCGAStorageService;

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
 * Created on 03/09/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Path("/admin")
public class AdminWSServer extends StorageWSServer {

    public AdminWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
        System.out.println("Build AdminWSServer");
    }


    @GET
    @Path("/stop")
    @Produces("text/plain")
    public Response stop() {
        OpenCGAStorageService.getInstance().stop();
        return createOkResponse("bye!");
    }

}
