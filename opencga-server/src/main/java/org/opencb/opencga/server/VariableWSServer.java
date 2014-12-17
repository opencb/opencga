package org.opencb.opencga.server;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.Variable;
import org.opencb.opencga.catalog.beans.VariableSet;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

/**
 * Created by jacobo on 16/12/14.
 */
@Path("/variables")
@Api(value = "variables", description = "Variable sets")
public class VariableWSServer extends OpenCGAWSServer{

    public VariableWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
        params = uriInfo.getQueryParameters();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create variable set")
    public Response createSet(@ApiParam(value = "studyId", required = true)       @QueryParam("studyId") String studyIdStr,
                              @ApiParam(value = "name", required = true)          @QueryParam("name") String name,
                              @ApiParam(value = "repeatable", required = false)   @QueryParam("repeatable") Boolean repeatable,
                              @ApiParam(value = "description", required = false)  @QueryParam("description") String description,
                              @ApiParam(value = "variables", required = true) List<Variable> variables) {

        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            QueryResult<VariableSet> queryResult = catalogManager.createVariableSet(studyId,
                    name, repeatable, description, null, variables, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }


    @GET
    @Path("/{variableSetId}/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get VariableSet info")
    public Response variableSetInfo(
            @ApiParam(value = "variableSetId", required = true) @PathParam("variableSetId") int variableSetId
    ) {
        try {
            QueryResult<VariableSet> queryResult = catalogManager.getVariableSet(variableSetId, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}
