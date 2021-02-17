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
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.sample.SampleTsvAnnotationLoader;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.catalog.utils.CatalogSampleAnnotationsLoader;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.common.TsvAnnotationParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.sample.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 15/12/14.
 */
@Path("/{apiVersion}/samples")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Samples", description = "Methods for working with 'samples' endpoint")
public class SampleWSServer extends OpenCGAWSServer {

    private SampleManager sampleManager;

    public SampleWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        sampleManager = catalogManager.getSampleManager();
    }

    @GET
    @Path("/{samples}/info")
    @ApiOperation(value = "Get sample information", response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_PARAM, value = ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_DESCRIPTION,
                    defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response infoSample(
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION, required = true) @PathParam("samples") String samplesStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.SAMPLE_VERSION_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_VERSION_PARAM) String version,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("samples");

            List<String> sampleList = getIdList(samplesStr);
            DataResult<Sample> sampleQueryResult = sampleManager.get(studyStr, sampleList, query, queryOptions, true, token);
            return createOkResponse(sampleQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create sample", response = Sample.class,
            notes = "WARNING: The Individual object in the body is deprecated and will be completely removed in a future release. From"
                    + " that moment on it will not be possible to create an individual when creating a new sample. To do that you must "
                    + "use the individual/create web service, this web service allows now to create a new individual with its samples. "
                    + "This web service now allows to create a new sample and associate it to an existing individual.")
    public Response createSamplePOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "DEPRECATED: It should be passed in the body.") @QueryParam("individual") String individual,
            @ApiParam(value = "JSON containing sample information", required = true) SampleCreateParams params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new SampleCreateParams());

            Sample sample = params.toSample();
            if (StringUtils.isNotEmpty(individual) && StringUtils.isNotEmpty(sample.getIndividualId())) {
                throw new CatalogParameterException("Found both individual and individualId as a query parameter and in the body. Please, "
                        + "only pass individualId in the body");
            }
            if (StringUtils.isNotEmpty(individual)) {
                sample.setIndividualId(individual);
            }

            return createOkResponse(sampleManager.create(studyStr, sample, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/load")
    @ApiOperation(value = "Load samples from a ped file [EXPERIMENTAL]", response = Sample.class)
    public Response loadSamples(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "file", required = true) @QueryParam("file") String fileStr,
            @ApiParam(value = "variableSet", required = false) @QueryParam("variableSet") String variableSet) {
        try {
            File pedigreeFile = catalogManager.getFileManager().get(studyStr, fileStr, null, token).first();
            CatalogSampleAnnotationsLoader loader = new CatalogSampleAnnotationsLoader(catalogManager);
            DataResult<Sample> sampleQueryResult = loader.loadSampleAnnotations(pedigreeFile, variableSet, token);
            return createOkResponse(sampleQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Sample search method", response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_PARAM, value = ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_DESCRIPTION,
                    defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.SAMPLE_SOMATIC_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_SOMATIC_PARAM) Boolean somatic,
            @ApiParam(value = ParamConstants.SAMPLE_INDIVIDUAL_ID_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_INDIVIDUAL_ID_PARAM) String individual,
            @ApiParam(value = ParamConstants.SAMPLE_FILE_IDS_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_FILE_IDS_PARAM) String fileIds,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.STATUS_DESCRIPTION) @QueryParam(ParamConstants.STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.PHENOTYPES_DESCRIPTION) @QueryParam(ParamConstants.PHENOTYPES_PARAM) String phenotypes,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam(Constants.ANNOTATION) String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.ATTRIBUTES_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.ATTRIBUTES_PARAM) String attributes,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted,

            @ApiParam(value = "Sample variant stats id. If filtering by other stats fields and not provided, it will be automatically set to ALL")
                @QueryParam(SampleDBAdaptor.STATS_ID) String statsId,
            @ApiParam(value = "Sample variant stats variantCount.") @QueryParam(SampleDBAdaptor.STATS_VARIANT_COUNT) String variantCount,
            @ApiParam(value = "Sample variant stats chromosomeCount.") @QueryParam("stats.chromosomeCount") String chromosomeCount,
            @ApiParam(value = "Sample variant stats typeCount.") @QueryParam("stats.typeCount") String typeCount,
            @ApiParam(value = "Sample variant stats genotypeCount.") @QueryParam("stats.genotypeCount") String genotypeCount,
            @ApiParam(value = "Sample variant stats tiTvRatio.") @QueryParam("stats.tiTvRatio") String tiTvRatio,
            @ApiParam(value = "Sample variant stats qualityAvg.") @QueryParam("stats.qualityAvg") String qualityAvg,
            @ApiParam(value = "Sample variant stats qualityStdDev.") @QueryParam("stats.qualityStdDev") String qualityStdDev,
            @ApiParam(value = "Sample variant stats heterozygosityRate.") @QueryParam("stats.heterozygosityRate") String heterozygosityRate,
            @ApiParam(value = "Sample variant stats depthCount.") @QueryParam("stats.depthCount") String depthCount,
            @ApiParam(value = "Sample variant stats biotypeCount.") @QueryParam("stats.biotypeCount") String biotypeCount,
            @ApiParam(value = "Sample variant stats clinicalSignificanceCount.") @QueryParam("stats.clinicalSignificanceCount") String clinicalSignificanceCount,
            @ApiParam(value = "Sample variant stats consequenceTypeCount.") @QueryParam("stats.consequenceTypeCount") String consequenceTypeCount) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(sampleManager.search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "Sample distinct method")
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.SAMPLE_SOMATIC_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_SOMATIC_PARAM) Boolean somatic,
            @ApiParam(value = ParamConstants.SAMPLE_INDIVIDUAL_ID_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_INDIVIDUAL_ID_PARAM) String individual,
            @ApiParam(value = ParamConstants.SAMPLE_FILE_IDS_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_FILE_IDS_PARAM) String fileIds,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.STATUS_DESCRIPTION) @QueryParam(ParamConstants.STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.PHENOTYPES_DESCRIPTION) @QueryParam(ParamConstants.PHENOTYPES_PARAM) String phenotypes,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam(Constants.ANNOTATION) String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.ATTRIBUTES_DESCRIPTION) @DefaultValue("") @QueryParam(ParamConstants.ATTRIBUTES_PARAM) String attributes,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam(ParamConstants.RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return createOkResponse(sampleManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some sample attributes", hidden = true, response = Sample.class)
    public Response updateByPost(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.SAMPLE_ID_DESCRIPTION) @QueryParam("id") String id,
            @ApiParam(value = ParamConstants.SAMPLE_NAME_DESCRIPTION) @QueryParam("name") String name,
            @ApiParam(value = "Sample source") @QueryParam("source") String source,
            @ApiParam(value = "Sample type") @QueryParam("type") String type,
            @ApiParam(value = "Somatic") @QueryParam("somatic") Boolean somatic,
            @ApiParam(value = ParamConstants.INDIVIDUAL_DESCRIPTION) @QueryParam("individual") String individual,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Comma separated list of phenotype ids or names") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @QueryParam("attributes") String attributes,
            @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)") @QueryParam("nattributes") String nattributes,
            @ApiParam(value = "Release value (Current release from the moment the samples were first created)")
            @QueryParam("release") String release,

            @ApiParam(value = "Create a new version of sample", defaultValue = "false")
            @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
            @ApiParam(value = "body") SampleUpdateParams parameters) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
            }
            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

            return createOkResponse(sampleManager.update(studyStr, query, parameters, true, options, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{samples}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some sample attributes", response = Sample.class)
    public Response updateByPost(
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION, required = true) @PathParam("samples") String sampleStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Create a new version of sample", defaultValue = "false")
                @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
                @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
            @ApiParam(value = "body") SampleUpdateParams parameters) {
        try {
            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
            }
            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

            return createOkResponse(sampleManager.update(studyStr, getIdList(sampleStr), parameters, true, options, token));
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
            @DefaultValue("false")  @QueryParam("parents") boolean parents,
            @ApiParam(value = "Annotation set id. If not provided, variableSetId will be used.") @QueryParam("annotationSetId") String annotationSetId,
            @ApiParam(value = ParamConstants.TSV_ANNOTATION_DESCRIPTION) TsvAnnotationParams params) {
        try {
            ObjectMap additionalParams = new ObjectMap()
                    .append("parents", parents)
                    .append("annotationSetId", annotationSetId);

            return createOkResponse(catalogManager.getSampleManager().loadTsvAnnotations(studyStr, variableSetId, path, params,
                    additionalParams, SampleTsvAnnotationLoader.ID, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{sample}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet", response = Sample.class)
    public Response updateAnnotations(
            @ApiParam(value = ParamConstants.SAMPLE_ID_DESCRIPTION, required = true) @PathParam("sample") String sampleStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_ID) @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE,RESET,REPLACE", defaultValue = "ADD")
                @QueryParam("action") ParamUtils.CompleteUpdateAction action,
            @ApiParam(value = "Create a new version of sample", defaultValue = "false") @QueryParam(Constants.INCREMENT_VERSION)
                    boolean incVersion,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_PARAMS_DESCRIPTION) Map<String, Object> updateParams) {
        try {
            if (action == null) {
                action = ParamUtils.CompleteUpdateAction.ADD;
            }
            return createOkResponse(catalogManager.getSampleManager().updateAnnotations(studyStr, sampleStr, annotationSetId,
                    updateParams, action, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{samples}/delete")
    @ApiOperation(value = "Delete samples", response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = Constants.FORCE, value = ParamConstants.SAMPLE_FORCE_DELETE_DESCRIPTION, dataType = "boolean",
                    defaultValue = "false", paramType = "query"),
            @ApiImplicitParam(name = Constants.EMPTY_FILES_ACTION, value = ParamConstants.SAMPLE_EMPTY_FILES_ACTION_DESCRIPTION,
                    dataType = "string", defaultValue = "NONE", paramType = "query"),
            @ApiImplicitParam(name = Constants.DELETE_EMPTY_COHORTS, value = ParamConstants.SAMPLE_DELETE_EMPTY_COHORTS_DESCRIPTION,
                    dataType = "boolean", defaultValue = "false", paramType = "query")
    })
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION) @PathParam("samples") String samples) {
        try {
            queryOptions.put(Constants.EMPTY_FILES_ACTION, query.getString(Constants.EMPTY_FILES_ACTION, "NONE"));
            queryOptions.put(Constants.DELETE_EMPTY_COHORTS, query.getBoolean(Constants.DELETE_EMPTY_COHORTS, false));

            return createOkResponse(sampleManager.delete(studyStr, getIdList(samples), queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{samples}/acl")
    @ApiOperation(value = "Returns the acl of the samples. If member is provided, it will only return the acl for the member.", response = Map.class)
    public Response getAcls(@ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION, required = true) @PathParam("samples") String sampleIdsStr,
                            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(sampleIdsStr);
            return createOkResponse(sampleManager.getAcls(studyStr, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = Map.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "JSON containing the parameters to update the permissions. If propagate flag is set to true, it will "
                    + "propagate the permissions defined to the individuals that are associated to the matching samples", required = true)
                    SampleAclUpdateParams params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new SampleAclUpdateParams());
            SampleAclParams sampleAclParams = new SampleAclParams(
                    params.getIndividual(), params.getFile(), params.getCohort(), params.getPermissions());
            List<String> idList = StringUtils.isEmpty(params.getSample()) ? Collections.emptyList() : getIdList(params.getSample(), false);
            return createOkResponse(sampleManager.updateAcl(studyStr, idList, memberId, sampleAclParams, action, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog sample stats", response = FacetField.class)
    public Response getAggregationStats(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Source") @QueryParam("source") String source,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Type") @QueryParam("type") String type,
            @ApiParam(value = "Phenotypes") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = "Version") @QueryParam("version") String version,
            @ApiParam(value = "Somatic") @QueryParam("somatic") Boolean somatic,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            DataResult<FacetField> queryResult = catalogManager.getSampleManager().facet(studyStr, query, queryOptions, defaultStats,
                    token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
