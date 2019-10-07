/*
 * Copyright 2015-2017 OpenCB
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
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.pedigree.Multiples;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.IndividualManager;
import org.opencb.opencga.catalog.models.update.IndividualUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Location;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.server.WebServiceException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jacobo on 22/06/15.
 */

@Path("/{apiVersion}/individuals")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Individuals", position = 6, description = "Methods for working with 'individuals' endpoint")
public class IndividualWSServer extends OpenCGAWSServer {

    private IndividualManager individualManager;

    public IndividualWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        individualManager = catalogManager.getIndividualManager();
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create individual", position = 1, response = Individual.class)
    public Response createIndividualPOST(
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of sample ids to be associated to the created individual") @QueryParam("samples")
                    String samples,
            @ApiParam(value = "JSON containing individual information", required = true) IndividualCreatePOST params) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            return createOkResponse(individualManager.create(studyStr, params.toIndividual(), getIdList(samples), queryOptions, sessionId));
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
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response infoIndividual(
            @ApiParam(value = "Comma separated list of individual names or ids up to a maximum of 100", required = true)
                @PathParam("individuals") String individualStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
            @ApiParam(value = "Individual version") @QueryParam("version") Integer version,
            @ApiParam(value = "Fetch all individual versions", defaultValue = "false")
                @QueryParam(Constants.ALL_VERSIONS) boolean allVersions,
            @ApiParam(value = "Boolean to retrieve deleted cohorts", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                    + "exception whenever one of the entries looked for cannot be shown for whichever reason",
                    defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            query.remove("study");
            query.remove("individuals");

            List<String> individualList = getIdList(individualStr);
            List<DataResult<Individual>> individualQueryResult = individualManager.get(studyStr, individualList, query, queryOptions,
                    silent, sessionId);
            return createOkResponse(individualQueryResult);
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
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response searchIndividuals(
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                    + "alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "DEPRECATED: id", hidden = true) @QueryParam("id") String id,
            @ApiParam(value = "name", required = false) @QueryParam("name") String name,
            @ApiParam(value = "father", required = false) @QueryParam("father") String father,
            @ApiParam(value = "mother", required = false) @QueryParam("mother") String mother,
            @ApiParam(value = "Comma separated list of sample ids or names") @QueryParam("samples") String samples,
            @ApiParam(value = "sex", required = false) @QueryParam("sex") String sex,
            @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
            @ApiParam(value = "Comma separated list of disorder ids or names") @QueryParam("disorders") String disorders,
            @ApiParam(value = "Population name", required = false) @QueryParam("population.name") String populationName,
            @ApiParam(value = "Subpopulation name", required = false) @QueryParam("population.subpopulation") String populationSubpopulation,
            @ApiParam(value = "Population description", required = false) @QueryParam("population.description") String populationDescription,
            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Karyotypic sex", required = false) @QueryParam("karyotypicSex") String karyotypicSex,
            @ApiParam(value = "Life status", required = false) @QueryParam("lifeStatus") String lifeStatus,
            @ApiParam(value = "Affectation status", required = false) @QueryParam("affectationStatus") String affectationStatus,
            @ApiParam(value = "Boolean to retrieve deleted cohorts", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
            @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
            @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}")
            @QueryParam("annotationsetName") String annotationsetName,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}")
            @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)", required = false) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
            @ApiParam(value = "Release value (Current release from the moment the individuals were first created)")
            @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of individuals in the specified release)") @QueryParam("snapshot")
                    int snapshot) {
        try {
            query.remove("study");

            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            List<String> annotationList = new ArrayList<>();
            if (StringUtils.isNotEmpty(annotation)) {
                annotationList.add(annotation);
            }
            if (StringUtils.isNotEmpty(variableSet)) {
                annotationList.add(Constants.VARIABLE_SET + "=" + variableSet);
            }
            if (StringUtils.isNotEmpty(annotationsetName)) {
                annotationList.add(Constants.ANNOTATION_SET_NAME + "=" + annotationsetName);
            }
            if (!annotationList.isEmpty()) {
                query.put(Constants.ANNOTATION, StringUtils.join(annotationList, ";"));
            }

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            DataResult<Individual> queryResult;
            if (count) {
                queryResult = individualManager.count(studyStr, query, sessionId);
            } else {
                queryResult = individualManager.search(studyStr, query, queryOptions, sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individual}/annotationsets/search")
    @ApiOperation(value = "Search annotation sets [DEPRECATED]", hidden = true, position = 11, notes = "Use /individuals/search instead")
    public Response searchAnnotationSetGET(
            @ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Variable set id") @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(,key2=value)", required = false) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap) {
        try {
            Individual individual = individualManager.get(studyStr, individualStr, IndividualManager.INCLUDE_INDIVIDUAL_IDS, sessionId).first();
            Query query = new Query(IndividualDBAdaptor.QueryParams.UID.key(), individual.getUid());

            if (StringUtils.isEmpty(annotation)) {
                if (StringUtils.isNotEmpty(variableSet)) {
                    annotation = Constants.VARIABLE_SET + "=" + variableSet;
                }
            } else {
                if (StringUtils.isNotEmpty(variableSet)) {
                    String[] annotationsSplitted = StringUtils.split(annotation, ",");
                    List<String> annotationList = new ArrayList<>(annotationsSplitted.length);
                    for (String auxAnnotation : annotationsSplitted) {
                        String[] split = StringUtils.split(auxAnnotation, ":");
                        if (split.length == 1) {
                            annotationList.add(variableSet + ":" + auxAnnotation);
                        } else {
                            annotationList.add(auxAnnotation);
                        }
                    }
                    annotation = StringUtils.join(annotationList, ";");
                }
            }
            query.putIfNotEmpty(Constants.ANNOTATION, annotation);

            DataResult<Individual> search = individualManager.search(studyStr, query,
                    new QueryOptions(Constants.FLATTENED_ANNOTATIONS, asMap), sessionId);
            if (search.getNumResults() == 1) {
                return createOkResponse(new DataResult<>(search.getTime(), search.getEvents(), search.first().getAnnotationSets().size(),
                        search.first().getAnnotationSets(), search.first().getAnnotationSets().size()));
            } else {
                return createOkResponse(search);
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individuals}/annotationsets")
    @ApiOperation(value = "Return all the annotation sets of the individual [DEPRECATED]", position = 12, hidden = true,
            notes = "Use /individuals/search instead")
    public Response getAnnotationSet(
            @ApiParam(value = "Comma separated list of individual IDs or names up to a maximum of 100", required = true) @PathParam("individuals") String individualsStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Indicates whether to show the annotations as key-value", defaultValue = "false") @QueryParam("asMap") boolean asMap,
            @ApiParam(value = "Annotation set name. If provided, only chosen annotation set will be shown") @QueryParam("name") String annotationsetName,
            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                    + "exception whenever one of the entries looked for cannot be shown for whichever reason", defaultValue = "false")
            @QueryParam("silent") boolean silent) throws WebServiceException {
        try {
            List<DataResult<Individual>> queryResults = individualManager.get(studyStr, getIdList(individualsStr), null, sessionId);

            Query query = new Query(IndividualDBAdaptor.QueryParams.UID.key(),
                    queryResults.stream().map(DataResult::first).map(Individual::getUid).collect(Collectors.toList()));
            QueryOptions queryOptions = new QueryOptions(Constants.FLATTENED_ANNOTATIONS, asMap);

            if (StringUtils.isNotEmpty(annotationsetName)) {
                query.append(Constants.ANNOTATION, Constants.ANNOTATION_SET_NAME + "=" + annotationsetName);
                queryOptions.put(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationsetName);
            }

            DataResult<Individual> search = individualManager.search(studyStr, query, queryOptions, sessionId);
            if (search.getNumResults() == 1) {
                return createOkResponse(new DataResult<>(search.getTime(), search.getEvents(), search.first().getAnnotationSets().size(),
                        search.first().getAnnotationSets(), search.first().getAnnotationSets().size()));
            } else {
                return createOkResponse(search);
            }
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individual}/annotationsets/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create an annotation set for the individual [DEPRECATED]", position = 13, hidden = true,
            notes = "Use /{individual}/update instead")
    public Response annotateSamplePOST(
            @ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Variable set id or name", hidden = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Variable set id or name", required = true) @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "JSON containing the annotation set name and the array of annotations. The name should be unique for the "
                    + "individual", required = true) CohortWSServer.AnnotationsetParameters params) {
        try {
            if (StringUtils.isNotEmpty(variableSetId)) {
                variableSet = variableSetId;
            }
            String annotationSetId = StringUtils.isEmpty(params.id) ? params.name : params.id;

            individualManager.update(studyStr, individualStr, new IndividualUpdateParams()
                            .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, variableSet, params.annotations))),
                    QueryOptions.empty(), sessionId);
            DataResult<Individual> sampleQueryResult = individualManager.get(studyStr, individualStr,
                    new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + "." + annotationSetId), sessionId);
            List<AnnotationSet> annotationSets = sampleQueryResult.first().getAnnotationSets();
            DataResult<AnnotationSet> queryResult = new DataResult<>(sampleQueryResult.getTime(), Collections.emptyList(),
                    annotationSets.size(), annotationSets, annotationSets.size());
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individual}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some individual attributes", position = 6,
            notes = "The entire individual is returned after the modification. Using include/exclude query parameters is encouraged to "
                    + "avoid slowdowns when sending unnecessary information where possible")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query")
    })
    public Response updateByPost(
            @ApiParam(value = "Individual ID or name", required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Action to be performed if the array of samples is being updated.", defaultValue = "ADD")
            @QueryParam("samplesAction") ParamUtils.UpdateAction samplesAction,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", defaultValue = "ADD")
            @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(value = "Create a new version of individual", defaultValue = "false")
            @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(value = "Update all the sample references from the individual to point to their latest versions",
                    defaultValue = "false") @QueryParam("updateSampleVersion") boolean refresh,
            @ApiParam(value = "params") IndividualUpdateParams updateParams) {
        try {
            queryOptions.put(Constants.REFRESH, refresh);
            queryOptions.remove("updateSampleVersion");

            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.UpdateAction.ADD;
            }
            if (samplesAction == null) {
                samplesAction = ParamUtils.UpdateAction.ADD;
            }

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), samplesAction.name());
            actionMap.put(IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            DataResult<Individual> queryResult = catalogManager.getIndividualManager().update(studyStr, individualStr, updateParams,
                    queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individual}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet")
    public Response updateAnnotations(
            @ApiParam(value = "Individual id", required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = "Study [[user@]project:]study.") @QueryParam("study") String studyStr,
            @ApiParam(value = "AnnotationSet id to be updated.") @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = "Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing "
                    + "annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some "
                    + "annotations; RESET to set some annotations to the default value configured in the corresponding variables of the "
                    + "VariableSet if any.", defaultValue = "ADD") @QueryParam("action") ParamUtils.CompleteUpdateAction action,
            @ApiParam(value = "Create a new version of individual", defaultValue = "false") @QueryParam(Constants.INCREMENT_VERSION)
                    boolean incVersion,
            @ApiParam(value = "Update all the sample references from the individual to point to their latest versions",
                    defaultValue = "false") @QueryParam("updateSampleVersion") boolean refresh,
            @ApiParam(value = "Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key "
                    + "'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json "
                    + "with only the key 'reset' containing the comma separated variables that will be set to the default value"
                    + " when the action is RESET") Map<String, Object> updateParams) {
        try {
            if (action == null) {
                action = ParamUtils.CompleteUpdateAction.ADD;
            }
            queryOptions.put(Constants.REFRESH, refresh);

            return createOkResponse(catalogManager.getIndividualManager().updateAnnotations(studyStr, individualStr, annotationSetId,
                    updateParams, action, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/delete")
    @ApiOperation(value = "Delete existing individuals")
    @ApiImplicitParams({
            @ApiImplicitParam(name = Constants.FORCE, value = "Force the deletion of individuals that already belong to families",
                    dataType = "boolean", defaultValue = "false", paramType = "query")
    })
    public Response delete(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "id") @QueryParam("id") String id,
            @ApiParam(value = "name") @QueryParam("name") String name,
            @ApiParam(value = "father") @QueryParam("father") String father,
            @ApiParam(value = "mother") @QueryParam("mother") String mother,
            @ApiParam(value = "sex") @QueryParam("sex") String sex,
            @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
            @ApiParam(value = "Population name", required = false) @QueryParam("population.name")
                    String populationName,
            @ApiParam(value = "Subpopulation name", required = false) @QueryParam("population.subpopulation")
                    String populationSubpopulation,
            @ApiParam(value = "Population description", required = false) @QueryParam("population.description")
                    String populationDescription,
            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Karyotypic sex", required = false) @QueryParam("karyotypicSex")
                    IndividualProperty.KaryotypicSex karyotypicSex,
            @ApiParam(value = "Life status", required = false) @QueryParam("lifeStatus")
                    IndividualProperty.LifeStatus lifeStatus,
            @ApiParam(value = "Affectation status", required = false) @QueryParam("affectationStatus")
                    IndividualProperty.AffectationStatus affectationStatus,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)", required = false) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Release value (Current release from the moment the individuals were first created)")
            @QueryParam("release") String release) {
        try {
            query.remove("study");
            return createOkResponse(individualManager.delete(studyStr, query, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group individuals by several fields", position = 10, hidden = true,
            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(
            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
            @QueryParam("fields") String fields,
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "name", required = false) @QueryParam("name") String names,
            @ApiParam(value = "Comma separated list of sample ids or names") @QueryParam("samples") String samples,
            @ApiParam(value = "sex", required = false) @QueryParam("sex") IndividualProperty.Sex sex,
            @ApiParam(value = "ethnicity", required = false) @QueryParam("ethnicity") String ethnicity,
            @ApiParam(value = "Population name", required = false) @QueryParam("population.name") String populationName,
            @ApiParam(value = "Subpopulation name", required = false) @QueryParam("population.subpopulation")
                    String populationSubpopulation,
            @ApiParam(value = "Population description", required = false) @QueryParam("population.description")
                    String populationDescription,
            @ApiParam(value = "Karyotypic sex", required = false) @QueryParam("karyotypicSex")
                    IndividualProperty.KaryotypicSex karyotypicSex,
            @ApiParam(value = "Life status", required = false) @QueryParam("lifeStatus") IndividualProperty.LifeStatus lifeStatus,
            @ApiParam(value = "Affectation status", required = false) @QueryParam("affectationStatus")
                    IndividualProperty.AffectationStatus affectationStatus,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}")
            @QueryParam("annotationsetName") String annotationsetName,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}")
            @QueryParam("variableSet") String variableSet,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Release value (Current release from the moment the families were first created)")
            @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of families in the specified release)") @QueryParam("snapshot")
                    int snapshot) {
        try {
            query.remove("study");
            query.remove("fields");

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            DataResult result = individualManager.groupBy(studyStr, query, fields, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individuals}/acl")
    @ApiOperation(value = "Return the acl of the individual. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(@ApiParam(value = "Comma separated list of individual ids up to a maximum of 100", required = true) @PathParam("individuals")
                                    String individualIdsStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                                    + "exception whenever one of the entries looked for cannot be shown for whichever reason",
                                    defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(individualIdsStr);
            return createOkResponse(individualManager.getAcls(studyStr, idList, member, silent, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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

    public static class MemberAclUpdate extends StudyWSServer.MemberAclUpdateOld {
        public boolean propagate;
    }

    @POST
    @Path("/{individual}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member [DEPRECATED]", position = 21, hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. A different entrypoint /acl/{members}/update has been added "
                    + "to also support changing permissions using queries.")
    public Response updateAcl(
            @ApiParam(value = "individualId", required = true) @PathParam("individual") String individualIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Member id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value = "JSON containing one of the keys 'add', 'set' or 'remove'", required = true)
                    MemberAclUpdate params) {
        try {
            Individual.IndividualAclParams aclParams = getAclParams(params.add, params.remove, params.set);
            List<String> idList = StringUtils.isEmpty(individualIdStr) ? Collections.emptyList() : getIdList(individualIdStr);
            return createOkResponse(individualManager.updateAcl(studyStr, idList, memberId, aclParams, sessionId));
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
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = "JSON containing the parameters to update the permissions. If propagate flag is set to true, it will "
                    + "propagate the permissions defined to the samples that are associated to the matching individuals",
                    required = true) IndividualAcl params) {
        try {
            ObjectUtils.defaultIfNull(params, new IndividualAcl());

            Individual.IndividualAclParams aclParams = new Individual.IndividualAclParams(params.getPermissions(), params.getAction(),
                    params.sample, params.propagate);
            List<String> idList = StringUtils.isEmpty(params.individual) ? Collections.emptyList() : getIdList(params.individual, false);
            return createOkResponse(individualManager.updateAcl(studyStr, idList, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Fetch catalog individual stats", position = 15, hidden = true, response = QueryResponse.class)
    public Response getStats(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Has father") @QueryParam("hasFather") Boolean hasFather,
            @ApiParam(value = "Has mother") @QueryParam("hasMother") Boolean hasMother,
            @ApiParam(value = "Number of multiples") @QueryParam("numMultiples") String numMultiples,
            @ApiParam(value = "Multiples type") @QueryParam("multiplesType") String multiplesType,
            @ApiParam(value = "Sex") @QueryParam("sex") String sex,
            @ApiParam(value = "Karyotypic sex") @QueryParam("karyotypicSex") String karyotypicSex,
            @ApiParam(value = "Ethnicity") @QueryParam("ethnicity") String ethnicity,
            @ApiParam(value = "Population") @QueryParam("population") String population,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Life status") @QueryParam("lifeStatus") String lifeStatus,
            @ApiParam(value = "Affectation status") @QueryParam("affectationStatus") String affectationStatus,
            @ApiParam(value = "Phenotypes") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Number of samples") @QueryParam("numSamples") String numSamples,
            @ApiParam(value = "Parental consanguinity") @QueryParam("parentalConsanguinity") Boolean parentalConsanguinity,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = "Version") @QueryParam("version") String version,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove("study");
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            FacetQueryResult queryResult = catalogManager.getIndividualManager().facet(studyStr, query, queryOptions, defaultStats, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog individual stats", position = 15, response = QueryResponse.class)
    public Response getAggregationStats(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Has father") @QueryParam("hasFather") Boolean hasFather,
            @ApiParam(value = "Has mother") @QueryParam("hasMother") Boolean hasMother,
            @ApiParam(value = "Number of multiples") @QueryParam("numMultiples") String numMultiples,
            @ApiParam(value = "Multiples type") @QueryParam("multiplesType") String multiplesType,
            @ApiParam(value = "Sex") @QueryParam("sex") String sex,
            @ApiParam(value = "Karyotypic sex") @QueryParam("karyotypicSex") String karyotypicSex,
            @ApiParam(value = "Ethnicity") @QueryParam("ethnicity") String ethnicity,
            @ApiParam(value = "Population") @QueryParam("population") String population,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Life status") @QueryParam("lifeStatus") String lifeStatus,
            @ApiParam(value = "Affectation status") @QueryParam("affectationStatus") String affectationStatus,
            @ApiParam(value = "Phenotypes") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Number of samples") @QueryParam("numSamples") String numSamples,
            @ApiParam(value = "Parental consanguinity") @QueryParam("parentalConsanguinity") Boolean parentalConsanguinity,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = "Version") @QueryParam("version") String version,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove("study");
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            FacetQueryResult queryResult = catalogManager.getIndividualManager().facet(studyStr, query, queryOptions, defaultStats, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    // Data models
    protected static class IndividualCreatePOST {
        public String id;
        public String name;

        public String father;
        public String mother;
        public Multiples multiples;
        public Location location;
        public List<SampleWSServer.CreateSamplePOST> samples;
        public IndividualProperty.Sex sex;
        public String ethnicity;
        public Boolean parentalConsanguinity;
        public Individual.Population population;
        public String dateOfBirth;
        public IndividualProperty.KaryotypicSex karyotypicSex;
        public IndividualProperty.LifeStatus lifeStatus;
        public IndividualProperty.AffectationStatus affectationStatus;
        public List<AnnotationSet> annotationSets;
        public List<Phenotype> phenotypes;
        public List<Disorder> disorders;
        public Map<String, Object> attributes;

        public Individual toIndividual() {

            List<Sample> sampleList = null;
            if (samples != null) {
                sampleList = new ArrayList<>(samples.size());
                for (SampleWSServer.CreateSamplePOST sample : samples) {
                    sampleList.add(sample.toSample());
                }
            }

            String individualId = StringUtils.isEmpty(id) ? name : id;
            String individualName = StringUtils.isEmpty(name) ? individualId : name;
            return new Individual(individualId, individualName, new Individual().setId(father), new Individual().setId(mother), multiples,
                    location, sex, karyotypicSex, ethnicity, population, lifeStatus, affectationStatus, dateOfBirth,
                    sampleList, parentalConsanguinity != null ? parentalConsanguinity : false, 1, annotationSets, phenotypes, disorders)
                    .setAttributes(attributes);
        }
    }

}
