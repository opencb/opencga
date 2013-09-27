package org.opencb.opencga.server;

import org.opencb.opencga.common.networks.Layout;
import org.opencb.opencga.common.networks.Layout.LayoutResp;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Produces("text/plain")
@Path("/utils")
public class UtilsWSServer extends GenericWSServer {
	Layout layout;
	
	public UtilsWSServer(@Context UriInfo uriInfo,
			@Context HttpServletRequest httpServletRequest)  throws IOException {
		super(uriInfo, httpServletRequest);
		layout = new Layout();
	}
	
	
	@POST
	@Path("/network/layout/{algorithm}.{format}")
	public Response layout(@PathParam("algorithm") String layoutAlgorithm, @PathParam("format") String outputFormat, @FormParam("dot") String dotData, @DefaultValue("output") @FormParam("filename") String filename, @DefaultValue("false") @FormParam("base64") String base64, @FormParam("jsonp") String jsonpCallback) {
		LayoutResp resp = layout.layout(layoutAlgorithm, outputFormat, dotData, filename, base64, jsonpCallback);
		return processResp(resp);
	}

	@POST
	@Path("/network/layout/{algorithm}.coords")
	public Response coordinates(@PathParam("algorithm") String layoutAlgorithm, @FormParam("dot") String dotData, @FormParam("jsonp") String jsonpCallback) {
		LayoutResp resp = layout.coordinates(layoutAlgorithm, dotData, jsonpCallback);
		return processResp(resp);
	}
	
	private Response processResp(LayoutResp resp) {
		MediaType type;
		if(resp.getType().equals("json")) {
			type = MediaType.APPLICATION_JSON_TYPE;
			if(resp.getFileName() == null) {
				return createOkResponse((String) resp.getData(), type);
			}
			else {
				return createOkResponse((String) resp.getData(), type, resp.getFileName());
			}
		}
		else if(resp.getType().equals("bytes")) {
			type = MediaType.APPLICATION_OCTET_STREAM_TYPE;
			if(resp.getFileName() == null) {
				return createOkResponse((byte[]) resp.getData(), type);
			}
			else {
				return createOkResponse((byte[]) resp.getData(), type, resp.getFileName());
			}
		}
		else {
			type = MediaType.TEXT_PLAIN_TYPE;
			if(resp.getFileName() == null) {
				return createOkResponse((String) resp.getData(), type);
			}
			else {
				return createOkResponse((String) resp.getData(), type, resp.getFileName());
			}
		}
		
	}
}
