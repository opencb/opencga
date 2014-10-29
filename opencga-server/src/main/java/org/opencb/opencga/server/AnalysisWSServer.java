package org.opencb.opencga.server;


import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.CatalogManagerException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/analysis")
@Api(value = "analysis", description = "analysis")
public class AnalysisWSServer extends OpenCGAWSServer {

    public AnalysisWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @Produces("application/json")
    @ApiOperation(value = "Create analysis")

    public Response createStudy(
            @ApiParam(value = "studyId", required = true) @QueryParam("studyId") int studyId,
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
            @ApiParam(value = "alias", required = true) @QueryParam("alias") String alias,
            @ApiParam(value = "creatorId", required = true) @QueryParam("creatorId") String creatorId,
            @ApiParam(value = "description", required = true) @QueryParam("description") String description
    ) {


        QueryResult queryResult;
        try {
            queryResult = catalogManager.createAnalysis(studyId, name, alias, creatorId, description, sessionId);
//            queryResult = catalogManager.createStudy(projectId, name, alias, type, description, sessionId);

            return createOkResponse(queryResult);

        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }


}