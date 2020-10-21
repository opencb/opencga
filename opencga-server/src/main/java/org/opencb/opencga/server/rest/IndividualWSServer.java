/*
 * Copyright 2015-2020 OpenCB
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
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.individual.IndividualTsvAnnotationLoader;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.managers.IndividualManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.common.TsvAnnotationParams;
import org.opencb.opencga.core.models.individual.*;
import org.opencb.opencga.core.models.job.Job;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by jacobo on 22/06/15.
 */

@Path("/{apiVersion}/individuals")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Individuals", description = "Methods for working with 'individuals' endpoint")
public class IndividualWSServer extends OpenCGAWSServer {

    private IndividualManager individualManager;

    public IndividualWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        individualManager = catalogManager.getIndividualManager();
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create individual", response = Individual.class)
    public Response createIndividualPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
                    String studyStr,
            @ApiParam(value = "Comma separated list of sample ids to be associated to the created individual") @QueryParam("samples")
                    String samples,
            @ApiParam(value = "JSON containing individual information", required = true) IndividualCreateParams params) {
        return run(() -> individualManager.create(studyStr, params.toIndividual(), getIdListOrEmpty(samples), queryOptions, token));
    }

    @GET
    @Path("/{individuals}/info")
    @ApiOperation(value = "Get individual information", response = Individual.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response infoIndividual(
            @ApiParam(value = ParamConstants.INDIVIDUALS_DESCRIPTION, required = true) @PathParam("individuals") String individualStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Individual version") @QueryParam("version") Integer version,
            @ApiParam(value = "Boolean to retrieve deleted individuals", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("individuals");

            List<String> individualList = getIdList(individualStr);
            DataResult<Individual> individualQueryResult = individualManager.get(studyStr, individualList, query, queryOptions, true, token);
            return createOkResponse(individualQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individual}/relatives")
    @ApiOperation(value = "Get individual relatives", response = Individual.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response relatives(
            @ApiParam(value = ParamConstants.INDIVIDUAL_DESCRIPTION, required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Pedigree degree", defaultValue = "2") @QueryParam("degree") Integer degree) {
        try {
            int degreeCopy = degree != null ? degree : 2;
            return createOkResponse(individualManager.relatives(studyStr, individualStr, degreeCopy, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search for individuals", response = Individual.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response searchIndividuals(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                    + "alias") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "DEPRECATED: id", hidden = true) @QueryParam("id") String id,
            @ApiParam(value = "name", required = false) @QueryParam("name") String name,
            @ApiParam(value = "father", required = false) @QueryParam("father") String father,
            @ApiParam(value = "mother", required = false) @QueryParam("mother") String mother,
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION) @QueryParam("samples") String samples,
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
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.STATUS_DESCRIPTION) @QueryParam(ParamConstants.STATUS_PARAM) String status,
            @ApiParam(value = "Boolean to retrieve deleted individuals", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION)
            @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION)
            @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}")
            @QueryParam("annotationsetName") String annotationsetName,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}")
            @QueryParam("variableSet") String variableSet,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION, required = false) @QueryParam("annotation") String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = "Release value (Current release from the moment the individuals were first created)")
            @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of individuals in the specified release)") @QueryParam("snapshot")
                    int snapshot) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);

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

            return createOkResponse(individualManager.search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "Individual distinct method")
    public Response distinct(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                    + "alias") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "DEPRECATED: id", hidden = true) @QueryParam("id") String id,
            @ApiParam(value = "name", required = false) @QueryParam("name") String name,
            @ApiParam(value = "father", required = false) @QueryParam("father") String father,
            @ApiParam(value = "mother", required = false) @QueryParam("mother") String mother,
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION) @QueryParam("samples") String samples,
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
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.STATUS_DESCRIPTION) @QueryParam(ParamConstants.STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION)
            @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION)
            @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: annotationSet[=|==|!|!=]{annotationSetName}")
            @QueryParam("annotationsetName") String annotationsetName,
            @ApiParam(value = "DEPRECATED: Use annotation queryParam this way: variableSet[=|==|!|!=]{variableSetId}")
            @QueryParam("variableSet") String variableSet,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION, required = false) @QueryParam("annotation") String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = "Release value (Current release from the moment the individuals were first created)")
            @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of individuals in the specified release)") @QueryParam("snapshot") int snapshot,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return createOkResponse(individualManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some individual attributes", hidden = true, response = Individual.class)
    public Response updateByQuery(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
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
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION, required = false) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Release value (Current release from the moment the individuals were first created)") @QueryParam("release") String release,
            @ApiParam(value = ParamConstants.SAMPLES_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
                @QueryParam(ParamConstants.SAMPLES_ACTION_PARAM) ParamUtils.UpdateAction samplesAction,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
                @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(value = "Create a new version of individual", defaultValue = "false")
                @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(value = "Update all the sample references from the individual to point to their latest versions",
                    defaultValue = "false") @QueryParam("updateSampleVersion") boolean refresh,
            @ApiParam(value = ParamConstants.BODY_PARAM) IndividualUpdateParams updateParams) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
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

            DataResult<Individual> queryResult = catalogManager.getIndividualManager().update(studyStr, query, updateParams, true,
                    queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individuals}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some individual attributes", response = Individual.class)
    public Response updateByPost(
            @ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individuals") String individualStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.SAMPLES_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
                @QueryParam(ParamConstants.SAMPLES_ACTION_PARAM) ParamUtils.UpdateAction samplesAction,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
                @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(value = "Create a new version of individual", defaultValue = "false")
                @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(value = "Update all the sample references from the individual to point to their latest versions",
                    defaultValue = "false") @QueryParam("updateSampleVersion") boolean refresh,
            @ApiParam(value = ParamConstants.BODY_PARAM) IndividualUpdateParams updateParams) {
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

            List<String> individualIds = getIdList(individualStr);

            DataResult<Individual> queryResult = catalogManager.getIndividualManager().update(studyStr, individualIds, updateParams, true,
                    queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/annotationSets/load")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Load annotation sets from a TSV file", response = Job.class)
    public Response loadTsvAnnotations(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.VARIABLE_SET_DESCRIPTION, required = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Path where the TSV file is located in OpenCGA or where it should be located.", required = true)
            @QueryParam("path") String path,
            @ApiParam(value = "Flag indicating whether to create parent directories if they don't exist (only when TSV file was not previously associated).")
                @DefaultValue("false") @QueryParam("parents") boolean parents,
            @ApiParam(value = "Annotation set id. If not provided, variableSetId will be used.") @QueryParam("annotationSetId") String annotationSetId,
            @ApiParam(value = ParamConstants.TSV_ANNOTATION_DESCRIPTION) TsvAnnotationParams params) {
        try {
            ObjectMap additionalParams = new ObjectMap()
                    .append("parents", parents)
                    .append("annotationSetId", annotationSetId);

            return createOkResponse(catalogManager.getIndividualManager().loadTsvAnnotations(studyStr, variableSetId, path, params,
                    additionalParams, IndividualTsvAnnotationLoader.ID, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{individual}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet", response = Individual.class)
    public Response updateAnnotations(
            @ApiParam(value = ParamConstants.INDIVIDUAL_DESCRIPTION, required = true) @PathParam("individual") String individualStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_ID) @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE,RESET,REPLACE", defaultValue = "ADD")
                @QueryParam("action") ParamUtils.CompleteUpdateAction action,
            @ApiParam(value = "Create a new version of individual", defaultValue = "false") @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(value = "Update all the sample references from the individual to point to their latest versions", defaultValue = "false")
                @QueryParam("updateSampleVersion") boolean refresh,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_PARAMS_DESCRIPTION) Map<String, Object> updateParams) {
        try {
            if (action == null) {
                action = ParamUtils.CompleteUpdateAction.ADD;
            }
            queryOptions.put(Constants.REFRESH, refresh);

            return createOkResponse(catalogManager.getIndividualManager().updateAnnotations(studyStr, individualStr, annotationSetId,
                    updateParams, action, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{individuals}/delete")
    @ApiOperation(value = "Delete existing individuals", response = Individual.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = Constants.FORCE, value = "Force the deletion of individuals that already belong to families",
                    dataType = "boolean", defaultValue = "false", paramType = "query")
    })
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of individual ids") @PathParam("individuals") String individuals) {
        try {
            return createOkResponse(individualManager.delete(studyStr, getIdList(individuals), queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{individuals}/acl")
    @ApiOperation(value = "Return the acl of the individual. If member is provided, it will only return the acl for the member.", response = Map.class)
    public Response getAcls(@ApiParam(value = ParamConstants.INDIVIDUALS_DESCRIPTION, required = true) @PathParam("individuals") String individualIdsStr,
                            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION,
                                    defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(individualIdsStr);
            return createOkResponse(individualManager.getAcls(studyStr, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = Map.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
                    String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true) @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "Propagate individual permissions to related samples", defaultValue = "false") @QueryParam("propagate") boolean propagate,
            @ApiParam(value = "JSON containing the parameters to update the permissions. If propagate flag is set to true, it will "
                    + "propagate the permissions defined to the samples that are associated to the matching individuals",
                    required = true) IndividualAclUpdateParams params) {
        try {
            ObjectUtils.defaultIfNull(params, new IndividualAclUpdateParams());

            IndividualAclParams aclParams = new IndividualAclParams(params.getSample(), params.getPermissions());
            List<String> idList = StringUtils.isEmpty(params.getIndividual()) ? Collections.emptyList() : getIdList(params.getIndividual(), false);
            return createOkResponse(individualManager.updateAcl(studyStr, idList, memberId, aclParams, action, propagate, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog individual stats", response = FacetField.class)
    public Response getAggregationStats(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Has father") @QueryParam("hasFather") Boolean hasFather,
            @ApiParam(value = "Has mother") @QueryParam("hasMother") Boolean hasMother,
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
            @ApiParam(value = "Phenotypes") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Number of samples") @QueryParam("numSamples") String numSamples,
            @ApiParam(value = "Parental consanguinity") @QueryParam("parentalConsanguinity") Boolean parentalConsanguinity,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = "Version") @QueryParam("version") String version,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            DataResult<FacetField> queryResult = catalogManager.getIndividualManager().facet(studyStr, query, queryOptions, defaultStats,
                    token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
