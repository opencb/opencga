package org.opencb.opencga.server.ws.storage;

import org.opencb.opencga.server.ws.GenericWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

/**
 * Created by imedina on 30/03/14.
 */
public class VariantStorageService extends GenericWSServer {


    public VariantStorageService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(uriInfo, httpServletRequest);
    }
}
