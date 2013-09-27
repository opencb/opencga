package org.opencb.opencga.server;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

public class VcfWSServer extends GenericWSServer {

	public VcfWSServer(@Context UriInfo uriInfo,@Context HttpServletRequest httpServletRequest) throws IOException {
		super(uriInfo,httpServletRequest); 
	}

}
