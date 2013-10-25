package org.opencb.opencga.server;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

public class GffWSServer extends GenericWSServer {

    public GffWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(uriInfo, httpServletRequest);
    }


}
