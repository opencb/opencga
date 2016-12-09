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
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.catalog.models.VariableSet;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    @ApiOperation(value = "Create individual", position = 1, response = Individual.class)
    public Response createIndividual(@ApiParam(value = "(DEPRECATED) Use study instead", required = true) @QueryParam("studyId")
                                                 String studyIdStr,
                                     @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                            @QueryParam("study") String studyStr,
                                     @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                     @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                                     @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") long fatherId,
                                     @ApiParam(value = "motherId", required = false) @QueryParam("motherId") long motherId,
                                     @ApiParam(value = "sex", required = false) @QueryParam("sex") @DefaultValue("UNKNOWN")
                                                 Individual.Sex sex,
                                     @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
                                     @ApiParam(value = "Species taxonomy code", required = false) @QueryParam("species.taxonomyCode")
                                                 String speciesTaxonomyCode,
                                     @ApiParam(value = "Species scientific name", required = false) @QueryParam("species.scientificName")
                                                 String speciesScientificName,
                                     @ApiParam(value = "Species common name", required = false) @QueryParam("species.commonName")
                                                 String speciesCommonName,
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
            QueryResult<Individual> queryResult = individualManager.create(studyId, name, family, fatherId, motherId,
                    sex, ethnicity, speciesCommonName, speciesScientificName, speciesTaxonomyCode, populationName, populationSubpopulation,
                    populationDescription, karyotypicSex, lifeStatus, affectationStatus, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualIds}/info")
    @ApiOperation(value = "Get individual information", position = 2, response = Individual.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response infoIndividual(@ApiParam(value = "Comma separated list of individual names or ids", required = true)
                                       @PathParam("individualIds") String individualStr,
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
    public Response searchIndividuals(@ApiParam(value = "(DEPRECATED) Use study instead", required = true) @QueryParam("studyId")
                                                  String studyIdStr,
                                      @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                              + "alias") @QueryParam("study") String studyStr,
                                      @ApiParam(value = "id", required = false) @QueryParam("id") String id,
                                      @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                                      @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") String fatherId,
                                      @ApiParam(value = "motherId", required = false) @QueryParam("motherId") String motherId,
                                      @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                                      @ApiParam(value = "sex", required = false) @QueryParam("sex") String sex,
                                      @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
                                      @ApiParam(value = "Species taxonomy code", required = false) @QueryParam("species.taxonomyCode")
                                                  String speciesTaxonomyCode,
                                      @ApiParam(value = "Species scientific name", required = false) @QueryParam("species.scientificName")
                                                  String speciesScientificName,
                                      @ApiParam(value = "Species common name", required = false) @QueryParam("species.commonName")
                                                  String speciesCommonName,
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
                                      @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                                      @ApiParam(value = "annotationSetName", required = false) @QueryParam("annotationSetName")
                                                  String annotationSetName,
                                      @ApiParam(value = "annotation", required = false) @QueryParam("annotation") String annotation) {
        try {
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
    @Path("/{individualId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Annotate an individual [DEPRECATED]", position = 4)
    public Response annotateSamplePOST(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualStr,
                                       @ApiParam(value = "Annotation set name. Must be unique for the individual", required = true)
                                       @QueryParam("annotateSetName") String annotateSetName,
                                       @ApiParam(value = "VariableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                                       @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update")
                                           @DefaultValue("false") boolean update,
                                       @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete")
                                           @DefaultValue("false") boolean delete,
                                       Map<String, Object> annotations) {
        try {
            long individualId = catalogManager.getIndividualId(individualStr, sessionId);
            QueryResult<AnnotationSet> queryResult;
            if (update && delete) {
                return createErrorResponse("Annotate individual", "Unable to update and delete annotations at the same"
                        + " time");
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

    @Deprecated
    @GET
    @Path("/{individualId}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Annotate an individual [DEPRECATED]", position = 5)
    public Response annotateSampleGET(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualStr,
                                      @ApiParam(value = "Annotation set name. Must be unique", required = true)
                                      @QueryParam("annotateSetName") String annotateSetName,
                                      @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                                      @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update")
                                          @DefaultValue("false") boolean update,
                                      @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete")
                                          @DefaultValue("false") boolean delete) {
        try {
            long individualId = catalogManager.getIndividualId(individualStr, sessionId);
            QueryResult<AnnotationSet> queryResult;
            if (update && delete) {
                return createErrorResponse("Annotate individual", "Unable to update and delete annotations at the same"
                        + " time");
            } else if (delete) {
                queryResult = catalogManager.deleteIndividualAnnotation(individualId, annotateSetName, sessionId);
            } else {
                if (update) {
                    for (AnnotationSet annotationSet : catalogManager.getIndividual(individualId, null, sessionId).first()
                            .getAnnotationSets()) {
                        if (annotationSet.getName().equals(annotateSetName)) {
                            variableSetId = annotationSet.getVariableSetId();
                        }
                    }
                }
                QueryResult<VariableSet> variableSetResult = catalogManager.getVariableSet(variableSetId, null, sessionId);
                if(variableSetResult.getResult().isEmpty()) {
                    return createErrorResponse("sample annotate", "VariableSet not find.");
                }
                Map<String, Object> annotations = variableSetResult.getResult().get(0).getVariables().stream()
                        .filter(variable -> params.containsKey(variable.getName()))
                        .collect(Collectors.toMap(Variable::getName, variable -> params.getFirst(variable.getName())));

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
    @Path("/{individualId}/annotationSets/search")
    @ApiOperation(value = "Search annotation sets [NOT TESTED]", position = 11)
    public Response searchAnnotationSetGET(@ApiParam(value = "individualId", required = true) @PathParam("individualId")
                                                       String individualStr,
                                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                                   + "alias") @QueryParam("study") String studyStr,
                                           @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId")
                                                       long variableSetId,
                                           @ApiParam(value = "annotation", required = false) @QueryParam("annotation") String annotation,
                                           @ApiParam(value = "Indicates whether to show the annotations as key-value", required = false,
                                                   defaultValue = "true") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(individualManager.searchAnnotationSetAsMap(individualStr, studyStr, variableSetId, annotation,
                        sessionId));
            } else {
                return createOkResponse(individualManager.searchAnnotationSet(individualStr, studyStr, variableSetId, annotation,
                        sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/annotationSets/info")
    @ApiOperation(value = "Return all the annotation sets of the individual [NOT TESTED]", position = 12)
    public Response infoAnnotationSetGET(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualStr,
                                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                                 + "alias") @QueryParam("study") String studyStr,
                                         @ApiParam(value = "[PENDING] Indicates whether to show the annotations as key-value",
                                                 required = false, defaultValue = "true") @QueryParam("asMap") boolean asMap) {
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
    @Path("/{individualId}/annotationSets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the individual [NOT TESTED]", position = 13)
    public Response annotateSamplePOST(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualStr,
                                       @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                               + "alias") @QueryParam("study") String studyStr,
                                       @ApiParam(value = "VariableSetId of the new annotation", required = true)
                                           @QueryParam("variableSetId") long variableSetId,
                                       @ApiParam(value = "Annotation set name. Must be unique for the individual", required = true)
                                           @QueryParam("annotateSetName") String annotateSetName,
                                       Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = individualManager.createAnnotationSet(individualStr, studyStr, variableSetId,
                    annotateSetName, annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/annotationSets/{annotationSetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set [NOT TESTED]", position = 14)
    public Response deleteAnnotationGET(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualStr,
                                        @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                                + "alias") @QueryParam("study") String studyStr,
                                        @ApiParam(value = "annotationSetName", required = true) @PathParam("annotationSetName")
                                                    String annotationSetName,
                                        @ApiParam(value = "[NOT IMPLEMENTED] Comma separated list of annotation names to be deleted",
                                                required = false) @QueryParam("annotations") String annotations) {
        try {
            QueryResult<AnnotationSet> queryResult;
            if (annotations != null) {
                queryResult = individualManager.deleteAnnotations(individualStr, studyStr, annotationSetName, annotations, sessionId);
            } else {
                queryResult = individualManager.deleteAnnotationSet(individualStr, studyStr, annotationSetName, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individualId}/annotationSets/{annotationSetName}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the annotations [NOT TESTED]", position = 15)
    public Response updateAnnotationGET(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualStr,
                                        @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or"
                                                + " alias") @QueryParam("study") String studyStr,
                                        @ApiParam(value = "annotationSetName", required = true) @PathParam("annotationSetName")
                                                    String annotationSetName,
//                                      @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") long variableSetId,
//                                        @ApiParam(value = "reset", required = false) @QueryParam("reset") String reset,
                                        Map<String, Object> annotations) {
        try {
            QueryResult<AnnotationSet> queryResult = individualManager.updateAnnotationSet(individualStr, studyStr, annotationSetName,
                    annotations, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/annotationSets/{annotationSetName}/info")
    @ApiOperation(value = "Return the annotation set [NOT TESTED]", position = 16)
    public Response infoAnnotationGET(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualStr,
                                      @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                              + "alias") @QueryParam("study") String studyStr,
                                      @ApiParam(value = "annotationSetName", required = true) @PathParam("annotationSetName")
                                                  String annotationSetName,
                                      @ApiParam(value = "Indicates whether to show the annotations as key-value", required = false,
                                              defaultValue = "true") @QueryParam("asMap") boolean asMap) {
        try {
            if (asMap) {
                return createOkResponse(individualManager.getAnnotationSetAsMap(individualStr, studyStr, annotationSetName, sessionId));
            } else {
                return createOkResponse(individualManager.getAnnotationSet(individualStr, studyStr, annotationSetName, sessionId));
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{individualId}/update")
    @ApiOperation(value = "Update individual information", position = 6, response = Individual.class)
    public Response updateIndividual(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualStr,
                                     @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                            @QueryParam("study") String studyStr,
                                     @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                                     @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") Long fatherId,
                                     @ApiParam(value = "motherId", required = false) @QueryParam("motherId") Long motherId,
                                     @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                                     @ApiParam(value = "sex", required = false) @QueryParam("sex") Individual.Sex sex,
                                     @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
                                     @ApiParam(value = "Species taxonomy code", required = false) @QueryParam("species.taxonomyCode")
                                                 String speciesTaxonomyCode,
                                     @ApiParam(value = "Species scientific name", required = false) @QueryParam("species.scientificName")
                                                 String speciesScientificName,
                                     @ApiParam(value = "Species common name", required = false) @QueryParam("species.commonName")
                                                 String speciesCommonName,
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
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.SPECIES_COMMON_NAME.key(), speciesCommonName);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.SPECIES_SCIENTIFIC_NAME.key(), speciesScientificName);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.SPECIES_TAXONOMY_CODE.key(), speciesTaxonomyCode);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.POPULATION_NAME.key(), populationName);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.POPULATION_DESCRIPTION.key(), populationDescription);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.POPULATION_SUBPOPULATION.key(), populationSubpopulation);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.KARYOTYPIC_SEX.key(), karyotypicSex);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.LIFE_STATUS.key(), lifeStatus);
            params.putIfNotNull(IndividualDBAdaptor.QueryParams.AFFECTATION_STATUS.key(), affectationStatus);
            QueryResult<Individual> queryResult = individualManager.update(resource.getResourceId(), params, queryOptions, sessionId);
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
        public Individual.Sex sex;

        public String ethnicity;
        public Individual.Species species;
        public Individual.Population population;
    }

    @POST
    @Path("/{individualId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some individual attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                        @QueryParam("study") String studyStr,
                                 @ApiParam(value = "params", required = true) UpdateIndividual updateParams) {
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
    @Path("/{individualIds}/delete")
    @ApiOperation(value = "Delete individual information", position = 7)
    public Response deleteIndividual(@ApiParam(value = "Comma separated list of individual ids", required = true)
                                         @PathParam ("individualIds") String individualIds,
                                     @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                            @QueryParam("study") String studyStr) {
        try {
            List<QueryResult<Individual>> queryResult = individualManager.delete(individualIds, studyStr, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
//
//    @GET
//    @Path("/{individualIds}/share")
//    @ApiOperation(value = "Share individuals with other members", position = 8)
//    public Response share(@PathParam(value = "individualIds") String individualIds,
//                          @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
//                          @ApiParam(value = "Comma separated list of individual permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
//                          @ApiParam(value = "Boolean indicating whether to allow the change of of permissions in case any member already had any", required = true) @DefaultValue("false") @QueryParam("override") boolean override) {
//        try {
//            return createOkResponse(catalogManager.shareIndividual(individualIds, members, Arrays.asList(permissions.split(",")), override, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @GET
//    @Path("/{individualIds}/unshare")
//    @ApiOperation(value = "Remove the permissions for the list of members", position = 9)
//    public Response unshare(@PathParam(value = "individualIds") String individualIds,
//                            @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
//                            @ApiParam(value = "Comma separated list of individual permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions) {
//        try {
//            return createOkResponse(catalogManager.unshareIndividual(individualIds, members, permissions, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group individuals by several fields", position = 10)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                                @QueryParam("fields") String fields,
                            @ApiParam(value = "(DEPRECATED) Use study instead", required = true) @QueryParam("studyId") String studyIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                            //@ApiParam(value = "id", required = false) @QueryParam("id") String ids,
                            @ApiParam(value = "name", required = false) @QueryParam("name") String names,
                            @ApiParam(value = "fatherId", required = false) @QueryParam("fatherId") String fatherId,
                            @ApiParam(value = "motherId", required = false) @QueryParam("motherId") String motherId,
                            @ApiParam(value = "family", required = false) @QueryParam("family") String family,
                            @ApiParam(value = "sex", required = false) @QueryParam("sex") Individual.Sex sex,
                            @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
                            @ApiParam(value = "Species taxonomy code", required = false) @QueryParam("species.taxonomyCode")
                                        String speciesTaxonomyCode,
                            @ApiParam(value = "Species scientific name", required = false) @QueryParam("species.scientificName")
                                        String speciesScientificName,
                            @ApiParam(value = "Species common name", required = false) @QueryParam("species.commonName")
                                        String speciesCommonName,
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
                            @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                            @ApiParam(value = "annotationSetName", required = false) @QueryParam("annotationSetName")
                                        String annotationSetName,
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
    @Path("/{individualIds}/acl")
    @ApiOperation(value = "Return the acl of the individual", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individualIds")
                                        String individualIdsStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr) {
        try {
            return createOkResponse(catalogManager.getAllIndividualAcls(individualIdsStr, studyStr, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{individualIds}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", position = 19)
    public Response createAcl(@ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individualIds")
                                          String individualIdsStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list",
                                       required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                                       required = true) @DefaultValue("") @QueryParam("members") String members) {
        try {
            return createOkResponse(catalogManager.createIndividualAcls(individualIdsStr, studyStr, members, permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualId}/acl/{memberId}/info")
    @ApiOperation(value = "Return the set of permissions granted for the member", position = 20)
    public Response getAcl(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualIdStr,
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
    @Path("/{individualId}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(@ApiParam(value = "individualId", required = true) @PathParam("individualId") String individualIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false)
                                  @QueryParam("addPermissions") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false)
                                  @QueryParam("removePermissions") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false)
                                  @QueryParam("setPermissions") String setPermissions) {
        try {
            return createOkResponse(catalogManager.updateIndividualAcl(individualIdStr, studyStr, memberId, addPermissions,
                    removePermissions, setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individualIds}/acl/{memberId}/delete")
    @ApiOperation(value = "Remove all the permissions granted for the member", position = 22)
    public Response deleteAcl(@ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individualIds")
                                          String individualIdsStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                    @QueryParam("study") String studyStr,
                              @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.removeIndividualAcl(individualIdsStr, studyStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
