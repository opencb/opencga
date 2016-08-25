package org.opencb.opencga.server.ws;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
    @ApiOperation(value = "Get individual information", position = 2)
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
    @ApiOperation(value = "Search for individuals", position = 3)
    public Response searchIndividuals(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                      @ApiParam(value = "id", required = false) @QueryParam("id") String id,
                                      @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                                      @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") String fatherId,
                                      @ApiParam(value = "motherId", required = false) @QueryParam("motherId") String motherId,
                                      @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                                      @ApiParam(value = "gender", required = false) @QueryParam("gender") String gender,
                                      @ApiParam(value = "race", required = false) @QueryParam("race") String race,
                                      @ApiParam(value = "species", required = false) @QueryParam("species") String species,
                                      @ApiParam(value = "population", required = false) @QueryParam("population") String population,
                                      @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") int variableSetId,
                                      @ApiParam(value = "annotationSetId", required = false) @QueryParam("annotationSetId") String annotationSetId,
                                      @ApiParam(value = "annotation", required = false) @QueryParam("annotation") String annotation) {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            QueryResult<Individual> queryResult = catalogManager.getAllIndividuals(studyId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individualId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Annotate an individual", position = 4)
    public Response annotateSamplePOST(@ApiParam(value = "individualId", required = true) @PathParam("individualId") int individualId,
                                       @ApiParam(value = "Annotation set name. Must be unique for the individual", required = true) @QueryParam("annotateSetName") String annotateSetName,
                                       @ApiParam(value = "VariableSetId", required = false) @QueryParam("variableSetId") int variableSetId,
                                       @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
                                       @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete,
                                       Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (update && delete) {
                return createErrorResponse("Annotate individual", "Unable to update and delete annotations at the same time");
            } else if (update) {
                queryResult = catalogManager.updateIndividualAnnotation(individualId, annotateSetName, annotations, sessionId);
            } else if (delete) {
                queryResult = catalogManager.deleteIndividualAnnotation(individualId, annotateSetName, sessionId);
            } else {
                queryResult = catalogManager.annotateIndividual(individualId, annotateSetName, variableSetId,
                        annotations, Collections.emptyMap(), sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Annotate an individual", position = 5)
    public Response annotateSampleGET(@ApiParam(value = "individualId", required = true) @PathParam("individualId") int individualId,
                                      @ApiParam(value = "Annotation set name. Must be unique", required = true) @QueryParam("annotateSetName") String annotateSetName,
                                      @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") int variableSetId,
                                      @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update") @DefaultValue("false") boolean update,
                                      @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete") @DefaultValue("false") boolean delete) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (update && delete) {
                return createErrorResponse("Annotate individual", "Unable to update and delete annotations at the same time");
            } else if (delete) {
                queryResult = catalogManager.deleteIndividualAnnotation(individualId, annotateSetName, sessionId);
            } else {
                if (update) {
                    for (AnnotationSet annotationSet : catalogManager.getIndividual(individualId, null, sessionId).first().getAnnotationSets()) {
                        if (annotationSet.getId().equals(annotateSetName)) {
                            variableSetId = annotationSet.getVariableSetId();
                        }
                    }
                }
                QueryResult<VariableSet> variableSetResult = catalogManager.getVariableSet(variableSetId, null, sessionId);
                if(variableSetResult.getResult().isEmpty()) {
                    return createErrorResponse("sample annotate", "VariableSet not find.");
                }
                Map<String, Object> annotations = variableSetResult.getResult().get(0).getVariables().stream()
                        .filter(variable -> params.containsKey(variable.getId()))
                        .collect(Collectors.toMap(Variable::getId, variable -> params.getFirst(variable.getId())));

                if (update) {
                    queryResult = catalogManager.updateIndividualAnnotation(individualId, annotateSetName, annotations, sessionId);
                } else {
                    queryResult = catalogManager.annotateIndividual(individualId, annotateSetName, variableSetId,
                            annotations, Collections.emptyMap(), sessionId);
                }
            }

            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/update")
    @ApiOperation(value = "Update individual information", position = 6)
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

    public static class UpdateIndividual {
        public String name;
        public int fatherId;
        public int motherId;
        public String family;
        public Individual.Gender gender;

        public String race;
        public Individual.Species species;
        public Individual.Population population;
    }

    @POST
    @Path("/{individualId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some individual attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "individualId", required = true) @PathParam("individualId") int individualId,
                                 @ApiParam(value = "params", required = true) UpdateIndividual updateParams) {
        try {
            QueryResult<Individual> queryResult = catalogManager.modifyIndividual(individualId, new QueryOptions(jsonObjectMapper.writeValueAsString(updateParams)), sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/delete")
    @ApiOperation(value = "Delete individual information", position = 7)
    public Response deleteIndividual(@ApiParam(value = "individualId", required = true) @PathParam("individualId") int individualId) {
        try {
            QueryResult<Individual> queryResult = catalogManager.deleteIndividual(individualId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
