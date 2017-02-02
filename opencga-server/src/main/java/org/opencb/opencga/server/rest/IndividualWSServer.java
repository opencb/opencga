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
    @ApiOperation(value = "Create individual [WARNING]", position = 1, response = Individual.class,
            notes = "WARNING: the usage of this web service is discouraged, please use the POST version instead. Be aware that this is web "
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

    private static class IndividualParameters {
        public String name;
        public String family;
        public long fatherId;
        public long motherId;
        public Individual.Sex sex;
        public String ethnicity;
        public Individual.Species species;
        public Individual.Population population;
        public Individual.KaryotypicSex karyotypicSex;
        public Individual.LifeStatus lifeStatus;
        public Individual.AffectationStatus affectationStatus;
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create individual", position = 1, response = Individual.class)
    public Response createIndividualPOST(
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing individual information", required = true) IndividualParameters params){
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            long studyId = catalogManager.getStudyId(studyStr, sessionId);

            String scientificName = "";
            String commonName = "";
            String taxonomyCode = "";
            if (params.species != null) {
                commonName = params.species.getCommonName();
                scientificName = params.species.getScientificName();
                taxonomyCode = params.species.getTaxonomyCode();
            }

            String populationName = "";
            String subpopulationName = "";
            String description = "";
            if (params.population != null) {
                populationName = params.population.getName();
                subpopulationName = params.population.getSubpopulation();
                description = params.population.getDescription();
            }

            QueryResult<Individual> queryResult = individualManager.create(studyId, params.name, params.family, params.fatherId,
                    params.motherId, params.sex, params.ethnicity, commonName, scientificName, taxonomyCode, populationName,
                    subpopulationName, description, params.karyotypicSex, params.lifeStatus, params.affectationStatus, queryOptions,
                    sessionId);
            return createOkResponse(queryResult);
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
    public Response searchIndividuals(@ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
                                      @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                              + "alias") @QueryParam("study") String studyStr,
                                      @ApiParam(value = "DEPRECATED: id", hidden = true) @QueryParam("id") String id,
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
                                      @ApiParam(value = "annotationsetName", required = false) @QueryParam("annotationsetName")
                                                  String annotationsetName,
                                      @ApiParam(value = "annotation", required = false) @QueryParam("annotation") String annotation,
                                      @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
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

    @Deprecated
    @GET
    @Path("/{individual}/annotate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Annotate an individual [DEPRECATED]", position = 5, hidden = true)
    public Response annotateSampleGET(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual")
                                                  String individualStr,
                                      @ApiParam(value = "Annotation set name. Must be unique", required = true)
                                      @QueryParam("annotateSetName") String annotateSetName,
                                      @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId") long variableSetId,
                                      @ApiParam(value = "Update an already existing AnnotationSet") @ QueryParam("update")
                                          @DefaultValue("false") boolean update,
                                      @ApiParam(value = "Delete an AnnotationSet") @ QueryParam("delete")
                                          @DefaultValue("false") boolean delete) {
        return createErrorResponse(new CatalogException("Webservice no longer supported. Please, use "
                + "/{individual}/annotationsets/..."));
//        try {
//            long individualId = catalogManager.getIndividualId(individualStr, sessionId);
//            QueryResult<AnnotationSet> queryResult;
//            if (update && delete) {
//                return createErrorResponse("Annotate individual", "Unable to update and delete annotations at the same"
//                        + " time");
//            } else if (delete) {
//                queryResult = catalogManager.deleteIndividualAnnotation(individualId, annotateSetName, sessionId);
//            } else {
//                if (update) {
//                    for (AnnotationSet annotationset : catalogManager.getIndividual(individualId, null, sessionId).first()
//                            .getAnnotationSets()) {
//                        if (annotationset.getName().equals(annotateSetName)) {
//                            variableSetId = annotationset.getVariableSetId();
//                        }
//                    }
//                }
//                QueryResult<VariableSet> variableSetResult = catalogManager.getVariableSet(variableSetId, null, sessionId);
//                if(variableSetResult.getResult().isEmpty()) {
//                    return createErrorResponse("sample annotate", "VariableSet not find.");
//                }
//                Map<String, Object> annotations = variableSetResult.getResult().get(0).getVariables().stream()
//                        .filter(variable -> params.containsKey(variable.getName()))
//                        .collect(Collectors.toMap(Variable::getName, variable -> params.getFirst(variable.getName())));
//
//                if (update) {
//                    queryResult = catalogManager.updateIndividualAnnotation(individualId, annotateSetName, annotations, sessionId);
//                } else {
//                    queryResult = catalogManager.annotateIndividual(individualId, annotateSetName, variableSetId,
//                            annotations, Collections.emptyMap(), sessionId);
//                }
//            }
//
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
    }

    @GET
    @Path("/{individual}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets [NOT TESTED]", position = 11)
    public Response searchAnnotationSetGET(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual")
                                                       String individualStr,
                                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                                   + "alias") @QueryParam("study") String studyStr,
                                           @ApiParam(value = "variableSetId", required = false) @QueryParam("variableSetId")
                                                       long variableSetId,
                                           @ApiParam(value = "annotation", required = false) @QueryParam("annotation") String annotation,
                                           @ApiParam(value = "Indicates whether to show the annotations as key-value", required = false,
                                                   defaultValue = "false") @QueryParam("asMap") boolean asMap) {
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
    @Path("/{individual}/annotationsets/info")
    @ApiOperation(value = "Return all the annotation sets of the individual [NOT TESTED]", position = 12)
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
    @ApiOperation(value = "Create an annotation set for the individual [NOT TESTED]", position = 13)
    public Response annotateSamplePOST(
            @ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "VariableSetId of the new annotation", required = true) @QueryParam("variableSetId") long variableSetId,
            @ApiParam(value="JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "individual", required = true) CohortWSServer.AnnotationsetParameters params) {
        try {
            QueryResult<AnnotationSet> queryResult = individualManager.createAnnotationSet(individualStr, studyStr, variableSetId,
                    params.name, params.annotations, Collections.emptyMap(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individual}/annotationsets/{annotationsetName}/delete")
    @ApiOperation(value = "Delete the annotation set or the annotations within the annotation set [NOT TESTED]", position = 14)
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
    @ApiOperation(value = "Update the annotations [NOT TESTED]", position = 15)
    public Response updateAnnotationGET(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
                                        @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or"
                                                + " alias") @QueryParam("study") String studyStr,
                                        @ApiParam(value = "annotationsetName", required = true) @PathParam("annotationsetName")
                                                    String annotationsetName,
//                                      @ApiParam(value = "variableSetId", required = true) @QueryParam("variableSetId") long variableSetId,
//                                        @ApiParam(value = "reset", required = false) @QueryParam("reset") String reset,
                                        Map<String, Object> annotations) {
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
    @ApiOperation(value = "Return the annotation set [NOT TESTED]", position = 16)
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
    @ApiOperation(value = "Update individual information [WARNING]", position = 6, response = Individual.class,
    notes = "WARNING: the usage of this web service is discouraged, please use the POST version instead. Be aware that this is web service "
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
    @Path("/{individual}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some individual attributes using POST method", position = 6)
    public Response updateByPost(@ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
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
    @Path("/{individuals}/delete")
    @ApiOperation(value = "Delete individual information", position = 7)
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
    @ApiOperation(value = "Return the acl of the individual", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individuals")
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
            return createOkResponse(catalogManager.createIndividualAcls(individualIdsStr, studyStr, members, permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individuals}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of members", position = 19)
    public Response createAcl(
            @ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individuals") String individualIdsStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value="JSON containing the parameters defined in GET. Mandatory keys: 'members'", required = true)
                    StudyWSServer.CreateAclCommands params) {
        try {
            return createOkResponse(catalogManager.createIndividualAcls(individualIdsStr, studyStr, params.members, params.permissions,
                    sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individual}/acl/{memberId}/info")
    @ApiOperation(value = "Return the set of permissions granted for the member", position = 20)
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
            return createOkResponse(catalogManager.updateIndividualAcl(individualIdStr, studyStr, memberId, addPermissions,
                    removePermissions, setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individual}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "individualId", required = true) @PathParam("individual") String individualIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value="JSON containing one of the keys 'add', 'set' or 'remove'", required = true)
                    StudyWSServer.MemberAclUpdate params) {
        try {
            return createOkResponse(catalogManager.updateIndividualAcl(individualIdStr, studyStr, memberId, params.add,
                    params.remove, params.set, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individuals}/acl/{memberId}/delete")
    @ApiOperation(value = "Remove all the permissions granted for the member", position = 22)
    public Response deleteAcl(@ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individuals")
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
