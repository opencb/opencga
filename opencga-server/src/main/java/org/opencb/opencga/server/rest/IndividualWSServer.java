/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.server.rest;

import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.api.IIndividualManager;
import org.opencb.opencga.catalog.managers.api.IStudyManager;
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.OntologyTerm;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.*;

/**
 * Created by jacobo on 22/06/15.
 */

@Path("/{version}/individuals")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Individuals", position = 6, description = "Methods for working with 'individuals' endpoint")
public class IndividualWSServer extends OpenCGAWSServer {

    private IIndividualManager individualManager;

    public IndividualWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
        individualManager = catalogManager.getIndividualManager();
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create individual [DEPRECATED]", position = 1, response = Individual.class, hidden = true,
            notes = "DEPRECATED: the usage of this web service is discouraged, please use the POST version instead. Be aware that this is web "
                    + "service is not tested and this can be deprecated in a future version.")
    public Response createIndividual(@ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId")
                                                 String studyIdStr,
                                     @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                            @QueryParam("study") String studyStr,
                                     @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                     @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                                     @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") long fatherId,
                                     @ApiParam(value = "motherId", required = false) @QueryParam("motherId") long motherId,
                                     @ApiParam(value = "sex", required = false) @QueryParam("sex") @DefaultValue("UNKNOWN")
                                                 Individual.Sex sex,
                                     @ApiParam(value = "Date of birth. Format: yyyyMMdd", required = false) @QueryParam("dateOfBirth")
                                                 String dateOfBirth,
                                     @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
                                     @ApiParam(value = "Population name", required = false) @QueryParam("population.name")
                                                 String populationName,
                                     @ApiParam(value = "Subpopulation name", required = false) @QueryParam("population.subpopulation")
                                                 String populationSubpopulation,
                                     @ApiParam(value = "Population description", required = false) @QueryParam("population.description")
                                                 String populationDescription,
                                     @ApiParam(value = "Karyotypic sex", required = false) @QueryParam("karyotypicSex")
                                                 Individual.KaryotypicSex karyotypicSex,
                                     @ApiParam(value = "Life status", required = false) @QueryParam("lifeStatus")
                                                 Individual.LifeStatus lifeStatus,
                                     @ApiParam(value = "Affectation status", required = false) @QueryParam("affectationStatus")
                                                 Individual.AffectationStatus affectationStatus){
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            QueryResult<Individual> queryResult = individualManager.create(studyId, name, family, fatherId, motherId, sex, ethnicity,
                    populationName, populationSubpopulation, populationDescription, dateOfBirth, karyotypicSex, lifeStatus, affectationStatus,
                    queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create individual", position = 1, response = Individual.class)
    public Response createIndividualPOST(
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing individual information", required = true) IndividualPOST params){
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            return createOkResponse(
                    individualManager.create(studyStr, params.toIndividual(studyStr, catalogManager.getStudyManager(), sessionId),
                            queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individuals}/info")
    @ApiOperation(value = "Get individual information", position = 2, response = Individual.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response infoIndividual(@ApiParam(value = "Comma separated list of individual names or ids", required = true)
                                       @PathParam("individuals") String individualStr,
                                   @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                        @QueryParam("study") String studyStr) {
        try {
            List<QueryResult<Individual>> queryResults = new LinkedList<>();
            AbstractManager.MyResourceIds resourceId = individualManager.getIds(individualStr, studyStr, sessionId);

            for (Long individualId : resourceId.getResourceIds()) {
                queryResults.add(catalogManager.getIndividual(individualId, queryOptions, sessionId));
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search for individuals", position = 3, response = Individual[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response searchIndividuals(
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                      + "alias") @QueryParam("study") String studyStr,
              @ApiParam(value = "DEPRECATED: id", hidden = true) @QueryParam("id") String id,
              @ApiParam(value = "name", required = false) @QueryParam("name") String name,
              @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") String fatherId,
              @ApiParam(value = "motherId", required = false) @QueryParam("motherId") String motherId,
              @ApiParam(value = "family", required = false) @QueryParam("family") String family,
              @ApiParam(value = "sex", required = false) @QueryParam("sex") String sex,
              @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
              @ApiParam(value = "Population name", required = false) @QueryParam("population.name")
                          String populationName,
              @ApiParam(value = "Subpopulation name", required = false) @QueryParam("population.subpopulation")
                          String populationSubpopulation,
              @ApiParam(value = "Population description", required = false) @QueryParam("population.description")
                          String populationDescription,
              @ApiParam(value = "Ontology terms", required = false) @QueryParam("ontologies") String ontologies,
              @ApiParam(value = "Karyotypic sex", required = false) @QueryParam("karyotypicSex")
                          Individual.KaryotypicSex karyotypicSex,
              @ApiParam(value = "Life status", required = false) @QueryParam("lifeStatus")
                          Individual.LifeStatus lifeStatus,
              @ApiParam(value = "Affectation status", required = false) @QueryParam("affectationStatus")
                          Individual.AffectationStatus affectationStatus,
              @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
              @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId")
                          String variableSetId,
              @ApiParam(value = "Variable set id or name") @QueryParam("variableSet") String variableSet,
              @ApiParam(value = "annotationsetName", required = false) @QueryParam("annotationsetName")
                          String annotationsetName,
              @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)", required = false) @QueryParam("annotation") String annotation,
              @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            QueryResult<Individual> queryResult = individualManager.search(studyStr, query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @POST
    @Path("/{individual}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Annotate an individual [DEPRECATED]", position = 4, hidden = true)
    public Response annotateSamplePOST(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
                                       @ApiParam(value = "Annotation set name. Must be unique for the individual", required = true)
                                       @QueryParam("annotateSetName") String annotateSetName,
                                       @ApiParam(value = "VariableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                                       @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update")
                                           @DefaultValue("false") boolean update,
                                       @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete")
                                           @DefaultValue("false") boolean delete,
                                       Map<String, Object> annotations) {
        return createErrorResponse(new CatalogException("Webservice no longer supported. Please, use "
                + "/{individual}/annotationsets/..."));
//        try {
//            long individualId = catalogManager.getIndividualId(individualStr, sessionId);
//            QueryResult<AnnotationSet> queryResult;
//            if (update && delete) {
//                return createErrorResponse("Annotate individual", "Unable to update and delete annotations at the same"
//                        + " time");
//            } else if (update) {
//                queryResult = catalogManager.updateIndividualAnnotation(individualId, annotateSetName, annotations, sessionId);
//            } else if (delete) {
//                queryResult = catalogManager.deleteIndividualAnnotation(individualId, annotateSetName, sessionId);
//            } else {
//                queryResult = catalogManager.annotateIndividual(individualId, annotateSetName, variableSetId,
//                        annotations, Collections.emptyMap(), sessionId);
//            }
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
    }

    @GET
    @Path("/{individual}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets", position = 11)
    public Response searchAnnotationSetGET(
            @ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)", required = false) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }
            if (asMap) {
                return createOkResponse(individualManager.searchAnnotationSetAsMap(individualStr, studyStr, variableSet, annotation,
                        sessionId));
            } else {
                return createOkResponse(individualManager.searchAnnotationSet(individualStr, studyStr, variableSet, annotation,
                        sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individual}/annotationsets/info")
    @ApiOperation(value = "Return all the annotation sets of the individual", position = 12)
    public Response infoAnnotationSetGET(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
                                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                                 + "alias") @QueryParam("study") String studyStr,
                                         @ApiParam(value = "[PENDING] Indicates whether to show the annotations as key-value",
                                                 required = false, defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(individualManager.getAllAnnotationSetsAsMap(individualStr, studyStr, sessionId));
            } else {
                return createOkResponse(individualManager.getAllAnnotationSets(individualStr, studyStr, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individual}/annotationsets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the individual", position = 13)
    public Response annotateSamplePOST(
            @ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value="JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "individual", required = true) CohortWSServer.AnnotationsetParameters params) {
        try {
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }
            QueryResult<AnnotationSet> queryResult = individualManager.createAnnotationSet(individualStr, studyStr, variableSet,
                    params.name, params.annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individual}/annotationsets/{annotationsetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set", position = 14)
    public Response deleteAnnotationGET(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
                                        @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                                + "alias") @QueryParam("study") String studyStr,
                                        @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName")
                                                    String annotationsetName,
                                        @ApiParam(value = "[NOT IMPLEMENTED] Comma separated list of annotation names to be deleted",
                                                required = false) @QueryParam("annotations") String annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (annotations != null) {
                queryResult = individualManager.deleteAnnotations(individualStr, studyStr, annotationsetName, annotations, sessionId);
            } else {
                queryResult = individualManager.deleteAnnotationSet(individualStr, studyStr, annotationsetName, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individual}/annotationsets/{annotationsetName}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the annotations", position = 15)
    public Response updateAnnotationGET(
            @ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName") String annotationsetName,
            @ApiParam(value="JSON containing key:value annotations to update", required = true) Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = individualManager.updateAnnotationSet(individualStr, studyStr, annotationsetName,
                    annotations, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individual}/annotationsets/{annotationsetName}/info")
    @ApiOperation(value = "Return the annotation set", position = 16)
    public Response infoAnnotationGET(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
                                      @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                              + "alias") @QueryParam("study") String studyStr,
                                      @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName")
                                                  String annotationsetName,
                                      @ApiParam(value = "Indicates whether to show the annotations as key-value", required = false,
                                              defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(individualManager.getAnnotationSetAsMap(individualStr, studyStr, annotationsetName, sessionId));
            } else {
                return createOkResponse(individualManager.getAnnotationSet(individualStr, studyStr, annotationsetName, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{individual}/update")
    @ApiOperation(value = "Update individual information [DEPRECATED]", position = 6, response = Individual.class, hidden = true,
            notes = "DEPRECATED: the usage of this web service is discouraged, please use the POST version instead. Be aware that this is web service "
            + "is not tested and this can be deprecated in a future version.")
    public Response updateIndividual(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
                                     @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                            @QueryParam("study") String studyStr,
                                     @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                                     @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") Long fatherId,
                                     @ApiParam(value = "motherId", required = false) @QueryParam("motherId") Long motherId,
                                     @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                                     @ApiParam(value = "sex", required = false) @QueryParam("sex") Individual.Sex sex,
                                     @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
                                     @ApiParam(value = "Date of birth. Format: yyyyMMdd", required = false) @QueryParam("dateOfBirth")
                                                 String dateOfBirth,
                                     @ApiParam(value = "Population name", required = false) @QueryParam("population.name")
                                                 String populationName,
                                     @ApiParam(value = "Subpopulation name", required = false) @QueryParam("population.subpopulation")
                                                 String populationSubpopulation,
                                     @ApiParam(value = "Population description", required = false) @QueryParam("population.description")
                                                 String populationDescription,
                                     @ApiParam(value = "Karyotypic sex", required = false) @QueryParam("karyotypicSex")
                                                 Individual.KaryotypicSex karyotypicSex,
                                     @ApiParam(value = "Life status", required = false) @QueryParam("lifeStatus")
                                                 Individual.LifeStatus lifeStatus,
                                     @ApiParam(value = "Affectation status", required = false) @QueryParam("affectationStatus")
                                                 Individual.AffectationStatus affectationStatus) {
        try {
            AbstractManager.MyResourceId resource = individualManager.getId(individualStr, studyStr, sessionId);
            ObjectMap params = new ObjectMap();
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.NAME.key(), name);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.FATHER_ID.key(), fatherId);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.MOTHER_ID.key(), motherId);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.FAMILY.key(), family);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.SEX.key(), sex);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.ETHNICITY.key(), ethnicity);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(), populationName);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.POPULATION_DESCRIPTION.key(), populationDescription);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.POPULATION_SUBPOPULATION.key(), populationSubpopulation);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(), karyotypicSex);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(), lifeStatus);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(), affectationStatus);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.DATE_OF_BIRTH.key(), dateOfBirth);
            QueryResult<Individual> queryResult = individualManager.update(resource.getResourceId(), params, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individual}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some individual attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                        @QueryParam("study") String studyStr,
                                 @ApiParam(value = "params", required = true) IndividualPOST updateParams) {
        try {
            AbstractManager.MyResourceId resource = individualManager.getId(individualStr, studyStr, sessionId);
            QueryResult<Individual> queryResult = catalogManager.modifyIndividual(resource.getResourceId(),
                    new QueryOptions(jsonObjectMapper.writeValueAsString(updateParams)), sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individuals}/delete")
    @ApiOperation(value = "Delete individual information [NOT TESTED]", position = 7)
    public Response deleteIndividual(@ApiParam(value = "Comma separated list of individual IDs or names", required = true)
                                         @PathParam ("individuals") String individualIds,
                                     @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                            @QueryParam("study") String studyStr) {
        try {
            List<QueryResult<Individual>> queryResult = individualManager.delete(individualIds, studyStr, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group individuals by several fields", position = 10)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                                @QueryParam("fields") String fields,
                            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                            @ApiParam(value = "name", required = false) @QueryParam("name") String names,
                            @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") String fatherId,
                            @ApiParam(value = "motherId", required = false) @QueryParam("motherId") String motherId,
                            @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                            @ApiParam(value = "sex", required = false) @QueryParam("sex") Individual.Sex sex,
                            @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
                            @ApiParam(value = "Population name", required = false) @QueryParam("population.name") String populationName,
                            @ApiParam(value = "Subpopulation name", required = false) @QueryParam("population.subpopulation")
                                        String populationSubpopulation,
                            @ApiParam(value = "Population description", required = false) @QueryParam("population.description")
                                        String populationDescription,
                            @ApiParam(value = "Karyotypic sex", required = false) @QueryParam("karyotypicSex")
                                        Individual.KaryotypicSex karyotypicSex,
                            @ApiParam(value = "Life status", required = false) @QueryParam("lifeStatus") Individual.LifeStatus lifeStatus,
                            @ApiParam(value = "Affectation status", required = false) @QueryParam("affectationStatus")
                                        Individual.AffectationStatus affectationStatus,
                            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId") String variableSetId,
                            @ApiParam(value = "Variable set id or name", required = false) @QueryParam("variableSet") String variableSet,
                            @ApiParam(value = "annotationsetName", required = false) @QueryParam("annotationsetName")
                                        String annotationsetName,
                            @ApiParam(value = "annotation", required = false) @QueryParam("annotation") String annotation) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            QueryResult result = individualManager.groupBy(studyStr, query, queryOptions, fields, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individuals}/acl")
    @ApiOperation(value = "Return the acl of the individual. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individuals")
                                        String individualIdsStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member) {
        try {
            if (StringUtils.isEmpty(member)) {
                return createOkResponse(catalogManager.getAllIndividualAcls(individualIdsStr, studyStr, sessionId));
            } else {
                return createOkResponse(catalogManager.getIndividualAcl(individualIdsStr, studyStr, member, sessionId));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individuals}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", hidden = true, position = 19)
    public Response createAcl(@ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individuals")
                                          String individualIdsStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list",
                                       required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                                       required = true) @DefaultValue("") @QueryParam("members") String members) {
        try {
            Individual.IndividualAclParams aclParams = getAclParams(permissions, null, null);
            aclParams.setAction(AclParams.Action.SET);
            return createOkResponse(individualManager.updateAcl(individualIdsStr, studyStr, members, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class CreateAclCommands extends StudyWSServer.CreateAclCommands {
        public boolean propagate;
    }

    // Temporal method used by deprecated methods. This will be removed at some point.
    @Override
    protected Individual.IndividualAclParams getAclParams(
            @ApiParam(value = "Comma separated list of permissions to add", required = false) @QueryParam("add") String addPermissions,
            @ApiParam(value = "Comma separated list of permissions to remove", required = false) @QueryParam("remove") String removePermissions,
            @ApiParam(value = "Comma separated list of permissions to set", required = false) @QueryParam("set") String setPermissions)
            throws CatalogException {
        int count = 0;
        count += StringUtils.isNotEmpty(setPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(addPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(removePermissions) ? 1 : 0;
        if (count > 1) {
            throw new CatalogException("Only one of add, remove or set parameters are allowed.");
        } else if (count == 0) {
            throw new CatalogException("One of add, remove or set parameters is expected.");
        }

        String permissions = null;
        AclParams.Action action = null;
        if (StringUtils.isNotEmpty(addPermissions)) {
            permissions = addPermissions;
            action = AclParams.Action.ADD;
        }
        if (StringUtils.isNotEmpty(setPermissions)) {
            permissions = setPermissions;
            action = AclParams.Action.SET;
        }
        if (StringUtils.isNotEmpty(removePermissions)) {
            permissions = removePermissions;
            action = AclParams.Action.REMOVE;
        }
        return new Individual.IndividualAclParams(permissions, action, null, false);
    }

    @POST
    @Path("/{individuals}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members [DEPRECATED]", position = 19, hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. From now one this will be internally managed by the "
                    + "/acl/{members}/update entrypoint.")
    public Response createAcl(
            @ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individuals") String individualIdsStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
            @ApiParam(value="JSON containing the parameters defined in GET. Mandatory keys: 'members'", required = true)
                    CreateAclCommands params) {
        try {
            Individual.IndividualAclParams aclParams = getAclParams(params.permissions, null, null);
            aclParams.setAction(AclParams.Action.SET);
            return createOkResponse(individualManager.updateAcl(individualIdsStr, studyStr, params.members, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individual}/acl/{memberId}/info")
    @ApiOperation(value = "Return the set of permissions granted for the member [DEPRECATED]", position = 20, hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. From now one this will be internally managed by the "
                    + "/acl entrypoint.")
    public Response getAcl(@ApiParam(value = "individualId", required = true) @PathParam("individual") String individualIdStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                           @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.getIndividualAcl(individualIdStr, studyStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individual}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", hidden = true, position = 21)
    public Response updateAcl(@ApiParam(value = "individualId", required = true) @PathParam("individual") String individualIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false)
                                  @QueryParam("add") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false)
                                  @QueryParam("remove") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false)
                                  @QueryParam("set") String setPermissions) {
        try {
            Individual.IndividualAclParams aclParams = getAclParams(addPermissions, removePermissions, setPermissions);
            return createOkResponse(individualManager.updateAcl(individualIdStr, studyStr, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class MemberAclUpdate extends StudyWSServer.MemberAclUpdateOld {
        public boolean propagate;
    }

    @POST
    @Path("/{individual}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member [WARNING]", position = 21,
            notes = "WARNING: The usage of this webservice is discouraged. A different entrypoint /acl/{members}/update has been added "
                    + "to also support changing permissions using queries.")
    public Response updateAcl(
            @ApiParam(value = "individualId", required = true) @PathParam("individual") String individualIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value="JSON containing one of the keys 'add', 'set' or 'remove'", required = true)
                    MemberAclUpdate params) {
        try {
            Individual.IndividualAclParams aclParams = getAclParams(params.add, params.remove, params.set);
            return createOkResponse(individualManager.updateAcl(individualIdStr, studyStr, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class IndividualAcl extends AclParams {
        public String individual;
        public String sample;

        public boolean propagate;
    }

    @POST
    @Path("/acl/{memberIds}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("memberIds") String memberId,
            @ApiParam(value="JSON containing the parameters to add ACLs", required = true) IndividualAcl params) {
        try {
            Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(params.getPermissions(), params.getAction(),
                    params.sample, params.propagate);
            return createOkResponse(individualManager.updateAcl(params.individual, studyStr, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individuals}/acl/{memberId}/delete")
    @ApiOperation(value = "Remove all the permissions granted for the member [DEPRECATED]", position = 22, hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. A RESET action has been added to the /acl/{members}/update "
                    + "entrypoint.")
    public Response deleteAcl(@ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individuals")
                                          String individualIdsStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(null, AclParams.Action.RESET, null, false);
            return createOkResponse(individualManager.updateAcl(individualIdsStr, studyStr, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    // Data models

    protected static class IndividualPOST {
        public String name;
        @Deprecated
        public String family;
        @Deprecated
        public long fatherId;
        @Deprecated
        public long motherId;
        public Individual.Sex sex;
        public String ethnicity;
        public Boolean parentalConsanguinity;
        public Individual.Population population;
        public String dateOfBirth;
        public Individual.KaryotypicSex karyotypicSex;
        public Individual.LifeStatus lifeStatus;
        public Individual.AffectationStatus affectationStatus;
        public List<CommonModels.AnnotationSetParams> annotationSets;
        public List<OntologyTerm> ontologyTerms;
        public Map<String, Object> attributes;


        public Individual toIndividual(String studyStr, IStudyManager studyManager, String sessionId) throws CatalogException {
            List<AnnotationSet> annotationSetList = new ArrayList<>();
            if (annotationSets != null) {
                for (CommonModels.AnnotationSetParams annotationSet : annotationSets) {
                    if (annotationSet != null) {
                        annotationSetList.add(annotationSet.toAnnotationSet(studyStr, studyManager, sessionId));
                    }
                }
            }

            return new Individual(-1, name, fatherId, motherId, family, sex, karyotypicSex, ethnicity, population, lifeStatus,
                    affectationStatus, dateOfBirth, parentalConsanguinity != null ? parentalConsanguinity : false, 1, annotationSetList,
                    ontologyTerms);
        }
    }

}
