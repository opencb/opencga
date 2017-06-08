package org.opencb.opencga.server.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pfurio on 05/05/17.
 */
@Path("/{version}/meta")
@Produces("application/json")
@Api(value = "Meta", description = "Meta RESTful Web Services API")
public class MetaWSServer extends OpenCGAWSServer {

    public MetaWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders headerParam) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, headerParam);
    }

    @GET
    @Path("/about")
    @ApiOperation(httpMethod = "GET", value = "Returns info about current OpenCGA code.")
    public Response getAbout() {
        Map<String, String> info = new HashMap<>(5);
        info.put("Program", "OpenCGA (OpenCB)");
        info.put("Version", GitRepositoryState.get().getBuildVersion());
        info.put("Git branch", GitRepositoryState.get().getBranch());
        info.put("Git commit", GitRepositoryState.get().getCommitId());
        info.put("Description", "Big Data platform for processing and analysing NGS data");
        QueryResult queryResult = new QueryResult();
        queryResult.setId("about");
        queryResult.setDbTime(0);
        queryResult.setResult(Collections.singletonList(info));

        return createOkResponse(queryResult);
    }

    @GET
    @Path("/ping")
    @ApiOperation(httpMethod = "GET", value = "Ping Opencga webservices.")
    public Response ping() {

        QueryResult queryResult = new QueryResult();
        queryResult.setId("pong");
        queryResult.setDbTime(0);

        return createOkResponse(queryResult);
    }

    @GET
    @Path("/status")
    @ApiOperation(httpMethod = "GET", value = "Database status.")
    public Response status() {

        QueryResult queryResult = new QueryResult();
        queryResult.setId("Status");
        queryResult.setDbTime(0);
        queryResult.setResult(Arrays.asList(catalogManager.getDatabaseStatus()));
        return createOkResponse(queryResult);
    }


}
