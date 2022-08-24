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
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.panel.PanelImportTask;
import org.opencb.opencga.catalog.managers.PanelManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.panel.*;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;

@Path("/{apiVersion}/panels")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Disease Panels", description = "Methods for working with 'panels' endpoint")
public class PanelWSServer extends OpenCGAWSServer {

    private PanelManager panelManager;

    public PanelWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        panelManager = catalogManager.getPanelManager();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a panel", response = Panel.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response createPanel(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) Boolean includeResult,
            @ApiParam(name = "body", value = "Panel parameters") PanelCreateParams params) {
        try {
            return createOkResponse(panelManager.create(studyStr, params.toPanel(), queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/import")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Import panels", response = Job.class)
    public Response importPanel(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobId,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(name = "body", value = "Panel parameters") PanelImportParams params) {
        return submitJob(PanelImportTask.ID, studyStr, params, jobId, jobDescription, dependsOn, jobTags);
    }

//    @POST
//    @Path("/update")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Update panel attributes", hidden = true, response = Panel.class)
//    public Response updateByQuery(
//            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = "Panel id") @QueryParam("id") String id,
//            @ApiParam(value = "Panel name") @QueryParam("name") String name,
//            @ApiParam(value = "Panel phenotypes") @QueryParam("phenotypes") String phenotypes,
//            @ApiParam(value = "Panel variants") @QueryParam("variants") String variants,
//            @ApiParam(value = "Panel genes") @QueryParam("genes") String genes,
//            @ApiParam(value = "Panel regions") @QueryParam("regions") String regions,
//            @ApiParam(value = "Panel categories") @QueryParam("categories") String categories,
//            @ApiParam(value = "Panel tags") @QueryParam("tags") String tags,
//            @ApiParam(value = "Panel description") @QueryParam("description") String description,
//            @ApiParam(value = "Panel author") @QueryParam("author") String author,
//            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
//            @ApiParam(value = "Release") @QueryParam("release") String release,
//
//            @ApiParam(value = "Create a new version of panel", defaultValue = "false")
//                @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
//            @ApiParam(name = "body", value = "Panel parameters") PanelUpdateParams panelParams) {
//        try {
//            query.remove(ParamConstants.STUDY_PARAM);
//            return createOkResponse(panelManager.update(studyStr, query, panelParams, true, queryOptions, token));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @POST
    @Path("/{panels}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update panel attributes", response = Panel.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updatePanel(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of panel ids") @PathParam("panels") String panels,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) Boolean includeResult,
            @ApiParam(name = "body", value = "Panel parameters") PanelUpdateParams panelParams) {
        try {
            return createOkResponse(panelManager.update(studyStr, getIdList(panels), panelParams, true, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{panels}/info")
    @ApiOperation(value = "Panel info", response = Panel.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query")
    })
    public Response info(
            @ApiParam(value = ParamConstants.PANELS_DESCRIPTION) @PathParam(value = "panels") String panelStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.PANEL_VERSION_DESCRIPTION) @QueryParam(ParamConstants.PANEL_VERSION_PARAM) String version,
            @ApiParam(value = "Boolean to retrieve deleted panels", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);

            List<String> idList = getIdList(panelStr);
            DataResult<Panel> panelQueryResult = panelManager.get(studyStr, idList, query, queryOptions, true, token);
            return createOkResponse(panelQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/search")
    @ApiOperation(value = "Panel search", response = Panel.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.PANEL_ID_DESCRIPTION) @QueryParam(ParamConstants.PANEL_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.PANEL_UUID_DESCRIPTION) @QueryParam(ParamConstants.PANEL_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.PANEL_NAME_DESCRIPTION) @QueryParam(ParamConstants.PANEL_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.PANEL_DISORDERS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_DISORDERS_PARAM) String disorders,
            @ApiParam(value = ParamConstants.PANEL_VARIANTS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_VARIANTS_PARAM) String variants,
            @ApiParam(value = ParamConstants.PANEL_GENES_DESCRIPTION) @QueryParam(ParamConstants.PANEL_GENES_PARAM) String genes,
            @ApiParam(value = ParamConstants.PANEL_SOURCE_DESCRIPTION) @QueryParam(ParamConstants.PANEL_SOURCE_PARAM) String source,
            @ApiParam(value = ParamConstants.PANEL_REGIONS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_REGIONS_PARAM) String regions,
            @ApiParam(value = ParamConstants.PANEL_CATEGORIES_DESCRIPTION) @QueryParam(ParamConstants.PANEL_CATEGORIES_PARAM) String categories,
            @ApiParam(value = ParamConstants.PANEL_TAGS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.PANEL_DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.PANEL_DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.PANEL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.PANEL_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.PANEL_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.PANEL_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.PANEL_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.PANEL_ACL_DESCRIPTION) @QueryParam(ParamConstants.PANEL_ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.PANEL_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.PANEL_RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.PANEL_SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.PANEL_SNAPSHOT_PARAM) int snapshot) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(panelManager.search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "Panel distinct method", response = Object.class)
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.PANEL_ID_DESCRIPTION) @QueryParam(ParamConstants.PANEL_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.PANEL_UUID_DESCRIPTION) @QueryParam(ParamConstants.PANEL_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.PANEL_NAME_DESCRIPTION) @QueryParam(ParamConstants.PANEL_NAME_PARAM) String name,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.PANEL_DISORDERS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_DISORDERS_PARAM) String disorders,
            @ApiParam(value = ParamConstants.PANEL_VARIANTS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_VARIANTS_PARAM) String variants,
            @ApiParam(value = ParamConstants.PANEL_GENES_DESCRIPTION) @QueryParam(ParamConstants.PANEL_GENES_PARAM) String genes,
            @ApiParam(value = ParamConstants.PANEL_SOURCE_DESCRIPTION) @QueryParam(ParamConstants.PANEL_SOURCE_PARAM) String source,
            @ApiParam(value = ParamConstants.PANEL_REGIONS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_REGIONS_PARAM) String regions,
            @ApiParam(value = ParamConstants.PANEL_CATEGORIES_DESCRIPTION) @QueryParam(ParamConstants.PANEL_CATEGORIES_PARAM) String categories,
            @ApiParam(value = ParamConstants.PANEL_TAGS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_TAGS_PARAM) String tags,
            @ApiParam(value = ParamConstants.PANEL_DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.PANEL_DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.PANEL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.PANEL_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.PANEL_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.PANEL_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.PANEL_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.PANEL_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.PANEL_ACL_DESCRIPTION) @QueryParam(ParamConstants.PANEL_ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.PANEL_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.PANEL_RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.PANEL_SNAPSHOT_DESCRIPTION) @QueryParam(ParamConstants.PANEL_SNAPSHOT_PARAM) int snapshot,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return createOkResponse(panelManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{panels}/delete")
    @ApiOperation(value = "Delete existing panels", response = Panel.class)
    public Response deleteList(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of panel ids") @PathParam("panels") String panels) {
        try {
            return createOkResponse(panelManager.delete(studyStr, getIdList(panels), queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{panels}/acl")
    @ApiOperation(value = "Returns the acl of the panels. If member is provided, it will only return the acl for the member.", response = AclEntryList.class)
    public Response getAcls(
            @ApiParam(value = ParamConstants.PANELS_DESCRIPTION, required = true) @PathParam("panels")
                    String sampleIdsStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "User or group id") @QueryParam("member") String member,
            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false")
            @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(sampleIdsStr);
            return createOkResponse(panelManager.getAcls(studyStr, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = AclEntryList.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "JSON containing the parameters to update the permissions.", required = true) PanelAclUpdateParams params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new PanelAclUpdateParams());
            AclParams panelAclParams = new AclParams(params.getPermissions());
            List<String> idList = getIdList(params.getPanel(), false);
            return createOkResponse(panelManager.updateAcl(studyStr, idList, memberId, panelAclParams, action, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
