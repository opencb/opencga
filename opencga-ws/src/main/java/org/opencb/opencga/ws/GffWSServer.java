package org.opencb.opencga.ws;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

public class GffWSServer extends GenericWSServer {

	public GffWSServer(@Context UriInfo uriInfo,@Context HttpServletRequest httpServletRequest) throws IOException {
		super(uriInfo,httpServletRequest); 
	}

	
}
