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

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.common.TsvAnnotationParams;
import org.opencb.opencga.core.models.individual.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response createIndividualPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
            String studyStr,
            @ApiParam(value = "Comma separated list of sample ids to be associated to the created individual") @QueryParam("samples")
            String samples,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) Boolean includeResult,
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
            @ApiParam(value = ParamConstants.INDIVIDUAL_VERSION_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_VERSION_PARAM) String version,
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
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INDIVIDUALS_ID_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.INDIVIDUAL_UUID_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.INDIVIDUAL_NAME_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.INDIVIDUAL_FATHER_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_FATHER_PARAM) String father,
            @ApiParam(value = ParamConstants.INDIVIDUAL_MOTHER_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_MOTHER_PARAM) String mother,
            @ApiParam(value = ParamConstants.INDIVIDUAL_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_SAMPLES_PARAM) String samples,
            @ApiParam(value = ParamConstants.INDIVIDUAL_FAMILY_IDS_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_FAMILY_IDS_PARAM) String familyIds,
            @ApiParam(value = ParamConstants.INDIVIDUAL_SEX_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_SEX_PARAM) String sex,
            @ApiParam(value = ParamConstants.INDIVIDUAL_DATE_OF_BIRTH_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_DATE_OF_BIRTH_PARAM) String dateOfBirth,
            @ApiParam(value = ParamConstants.INDIVIDUAL_ETHNICITY_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_ETHNICITY_PARAM) String ethnicity,
            @ApiParam(value = ParamConstants.INDIVIDUAL_DISORDERS_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_DISORDERS_PARAM) String disorders,
            @ApiParam(value = ParamConstants.INDIVIDUAL_PHENOTYPES_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_PHENOTYPES_PARAM) String phenotypes,
            @ApiParam(value = ParamConstants.INDIVIDUAL_POPULATION_NAME_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_POPULATION_NAME_PARAM) String populationName,
            @ApiParam(value = ParamConstants.INDIVIDUAL_POPULATION_SUBPOPULATION_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_POPULATION_SUBPOPULATION_PARAM) String populationSubpopulation,
            @ApiParam(value = ParamConstants.INDIVIDUAL_KARYOTYPIC_SEX_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_KARYOTYPIC_SEX_PARAM) String karyotypicSex,
            @ApiParam(value = ParamConstants.INDIVIDUAL_LIFE_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_LIFE_STATUS_PARAM) String lifeStatus,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.STATUS_DESCRIPTION) @QueryParam(ParamConstants.STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.INDIVIDUAL_DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INDIVIDUAL_DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.INDIVIDUAL_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.INDIVIDUAL_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam(Constants.ANNOTATION) String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.INDIVIDUAL_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.INDIVIDUAL_SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_SNAPSHOT_PARAM) int snapshot) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(individualManager.search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "Individual distinct method", response = Object.class)
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INDIVIDUALS_ID_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.INDIVIDUAL_UUID_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.INDIVIDUAL_NAME_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.INDIVIDUAL_FAMILY_IDS_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_FAMILY_IDS_PARAM) String familyIds,
            @ApiParam(value = ParamConstants.INDIVIDUAL_FATHER_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_FATHER_PARAM) String father,
            @ApiParam(value = ParamConstants.INDIVIDUAL_MOTHER_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_MOTHER_PARAM) String mother,
            @ApiParam(value = ParamConstants.INDIVIDUAL_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_SAMPLES_PARAM) String samples,
            @ApiParam(value = ParamConstants.INDIVIDUAL_SEX_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_SEX_PARAM) String sex,
            @ApiParam(value = ParamConstants.INDIVIDUAL_ETHNICITY_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_ETHNICITY_PARAM) String ethnicity,
            @ApiParam(value = ParamConstants.INDIVIDUAL_DATE_OF_BIRTH_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_DATE_OF_BIRTH_PARAM) String dateOfBirth,
            @ApiParam(value = ParamConstants.INDIVIDUAL_DISORDERS_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_DISORDERS_PARAM) String disorders,
            @ApiParam(value = ParamConstants.INDIVIDUAL_PHENOTYPES_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_PHENOTYPES_PARAM) String phenotypes,
            @ApiParam(value = ParamConstants.INDIVIDUAL_POPULATION_NAME_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_POPULATION_NAME_PARAM) String populationName,
            @ApiParam(value = ParamConstants.INDIVIDUAL_POPULATION_SUBPOPULATION_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_POPULATION_SUBPOPULATION_PARAM) String populationSubpopulation,
            @ApiParam(value = ParamConstants.INDIVIDUAL_KARYOTYPIC_SEX_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_KARYOTYPIC_SEX_PARAM) String karyotypicSex,
            @ApiParam(value = ParamConstants.INDIVIDUAL_LIFE_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_LIFE_STATUS_PARAM) String lifeStatus,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.STATUS_DESCRIPTION) @QueryParam(ParamConstants.STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.INDIVIDUAL_DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INDIVIDUAL_DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.INDIVIDUAL_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.INDIVIDUAL_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam(Constants.ANNOTATION) String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.INDIVIDUAL_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.INDIVIDUAL_SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.INDIVIDUAL_SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return createOkResponse(individualManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @POST
//    @Path("/update")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Update some individual attributes", hidden = true, response = Individual.class)
//    public Response updateByQuery(
//            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = "id") @QueryParam("id") String id,
//            @ApiParam(value = "name") @QueryParam("name") String name,
//            @ApiParam(value = "father") @QueryParam("father") String father,
//            @ApiParam(value = "mother") @QueryParam("mother") String mother,
//            @ApiParam(value = "sex") @QueryParam("sex") String sex,
//            @ApiParam(value = "ethnicity") @QueryParam("ethnicity") String ethnicity,
//            @ApiParam(value = "Population name") @QueryParam("population.name")
//                    String populationName,
//            @ApiParam(value = "Subpopulation name") @QueryParam("population.subpopulation")
//                    String populationSubpopulation,
//            @ApiParam(value = "Population description") @QueryParam("population.description")
//                    String populationDescription,
//            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
//            @ApiParam(value = "Karyotypic sex") @QueryParam("karyotypicSex")
//                    IndividualProperty.KaryotypicSex karyotypicSex,
//            @ApiParam(value = "Life status") @QueryParam("lifeStatus")
//                    IndividualProperty.LifeStatus lifeStatus,
//            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
//            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
//            @ApiParam(value = "Release value (Current release from the moment the individuals were first created)") @QueryParam("release") String release,
//            @ApiParam(value = ParamConstants.SAMPLES_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
//                @QueryParam(ParamConstants.SAMPLES_ACTION_PARAM) ParamUtils.BasicUpdateAction samplesAction,
//            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
//                @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
//            @ApiParam(value = "Create a new version of individual", defaultValue = "false")
//                @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
//            @ApiParam(value = ParamConstants.BODY_PARAM) IndividualUpdateParams updateParams) {
//        try {
//            query.remove(ParamConstants.STUDY_PARAM);
//
//            if (annotationSetsAction == null) {
//                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
//            }
//            if (samplesAction == null) {
//                samplesAction = ParamUtils.BasicUpdateAction.ADD;
//            }
//
//            Map<String, Object> actionMap = new HashMap<>();
//            actionMap.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), samplesAction.name());
//            actionMap.put(IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
//            queryOptions.put(Constants.ACTIONS, actionMap);
//
//            DataResult<Individual> queryResult = catalogManager.getIndividualManager().update(studyStr, query, updateParams, true,
//                    queryOptions, token);
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @POST
    @Path("/{individuals}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some individual attributes", response = Individual.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updateByPost(
            @ApiParam(value = "Comma separated list of individual ids", required = true) @PathParam("individuals") String individualStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.SAMPLES_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam(ParamConstants.SAMPLES_ACTION_PARAM) ParamUtils.BasicUpdateAction samplesAction,
            @ApiParam(value = ParamConstants.INDIVIDUAL_PHENOTYPES_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam(ParamConstants.INDIVIDUAL_PHENOTYPES_ACTION_PARAM) ParamUtils.BasicUpdateAction phenotypesAction,
            @ApiParam(value = ParamConstants.INDIVIDUAL_DISORDERS_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam(ParamConstants.INDIVIDUAL_DISORDERS_ACTION_PARAM) ParamUtils.BasicUpdateAction disordersAction,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) Boolean includeResult,
            @ApiParam(value = ParamConstants.BODY_PARAM) IndividualUpdateParams updateParams) {
        try {
            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
            }
            if (samplesAction == null) {
                samplesAction = ParamUtils.BasicUpdateAction.ADD;
            }
            if (phenotypesAction == null) {
                phenotypesAction = ParamUtils.BasicUpdateAction.ADD;
            }
            if (disordersAction == null) {
                disordersAction = ParamUtils.BasicUpdateAction.ADD;
            }

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), samplesAction.name());
            actionMap.put(IndividualDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            actionMap.put(IndividualDBAdaptor.QueryParams.PHENOTYPES.key(), phenotypesAction);
            actionMap.put(IndividualDBAdaptor.QueryParams.DISORDERS.key(), disordersAction);
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
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_PARAMS_DESCRIPTION) Map<String, Object> updateParams) {
        try {
            if (action == null) {
                action = ParamUtils.CompleteUpdateAction.ADD;
            }

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
    @ApiOperation(value = "Return the acl of the individual. If member is provided, it will only return the acl for the member.", response = AclEntryList.class)
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
    @ApiOperation(value = "Update the set of permissions granted for the member", response = IndividualAclEntryList.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
            String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
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
