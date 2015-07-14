package org.opencb.opencga.server.ws;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

/**
 * Created by jacobo on 22/06/15.
 */

@Path("/{version}/individuals")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Individuals", position = 6, description = "Methods for working with 'individuals' endpoint")
public class IndividualWSServer extends OpenCGAWSServer {


    public IndividualWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                          @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create sample", position = 1)
    public Response createIndividual(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                 @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                                 @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") int fatherId,
                                 @ApiParam(value = "motherId", required = false) @QueryParam("motherId") int motherId,
                                 @ApiParam(value = "gender", required = false) @QueryParam("gender") @DefaultValue("UNKNOWN") Individual.Gender gender) {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            QueryResult<Individual> queryResult = catalogManager.createIndividual(studyId, name, family, fatherId, motherId, gender, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/info")
    @ApiOperation(value = "Get individual information", position = 1)
    public Response infoIndividual(@ApiParam(value = "individualId", required = true) @PathParam("individualId") int individualId) {
        try {
            QueryResult<Individual> queryResult = catalogManager.getIndividual(individualId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search for individuals", position = 1)
    public Response searchIndividuals(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                      @ApiParam(value = "id", required = false) @QueryParam("id") String id,
                                      @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                                      @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") int fatherId,
                                      @ApiParam(value = "motherId", required = false) @QueryParam("motherId") int motherId,
                                      @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                                      @ApiParam(value = "gender", required = false) @QueryParam("gender") String gender,
                                      @ApiParam(value = "race", required = false) @QueryParam("race") String race
                                      ) {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            QueryResult<Individual> queryResult = catalogManager.getAllIndividuals(studyId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/update")
    @ApiOperation(value = "Update individual information", position = 1)
    public Response updateIndividual(@ApiParam(value = "individualId", required = true) @PathParam("individualId") int individualId,
                                     @ApiParam(value = "id", required = false) @QueryParam("id") String id,
                                     @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                                     @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") int fatherId,
                                     @ApiParam(value = "motherId", required = false) @QueryParam("motherId") int motherId,
                                     @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                                     @ApiParam(value = "gender", required = false) @QueryParam("gender") String gender,
                                     @ApiParam(value = "race", required = false) @QueryParam("race") String race
                                      ) {
        try {
            QueryResult<Individual> queryResult = catalogManager.modifyIndividual(individualId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/delete")
    @ApiOperation(value = "Delete individual information", position = 1)
    public Response deleteIndividual(@ApiParam(value = "individualId", required = true) @PathParam("individualId") int individualId) {
        try {
            QueryResult<Individual> queryResult = catalogManager.deleteIndividual(individualId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
