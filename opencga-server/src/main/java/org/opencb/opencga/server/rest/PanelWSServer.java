package org.opencb.opencga.server.rest;

import io.swagger.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.managers.PanelManager;
import org.opencb.opencga.core.models.panel.PanelAclUpdateParams;
import org.opencb.opencga.core.models.panel.PanelCreateParams;
import org.opencb.opencga.core.models.panel.PanelUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.AclParams;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;

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
    public Response createPanel(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of installation panel ids to be imported. To import them all at once, write the "
                    + "special word 'ALL_GLOBAL_PANELS'")
            @QueryParam("import") String panelIds,
            @ApiParam(name = "params", value = "Panel parameters") PanelCreateParams params) {
        try {
            if (StringUtils.isNotEmpty(panelIds)) {
                if ("ALL_GLOBAL_PANELS".equals(panelIds.toUpperCase())) {
                    return createOkResponse(panelManager.importAllGlobalPanels(studyStr, queryOptions, token));
                } else {
                    return createOkResponse(panelManager.importGlobalPanels(studyStr, getIdList(panelIds), queryOptions, token));
                }
            } else {
                return createOkResponse(panelManager.create(studyStr, params.toPanel(), queryOptions, token));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update panel attributes", hidden = true, response = Panel.class)
    public Response updateByQuery(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Panel id") @QueryParam("id") String id,
            @ApiParam(value = "Panel name") @QueryParam("name") String name,
            @ApiParam(value = "Panel phenotypes") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Panel variants") @QueryParam("variants") String variants,
            @ApiParam(value = "Panel genes") @QueryParam("genes") String genes,
            @ApiParam(value = "Panel regions") @QueryParam("regions") String regions,
            @ApiParam(value = "Panel categories") @QueryParam("categories") String categories,
            @ApiParam(value = "Panel tags") @QueryParam("tags") String tags,
            @ApiParam(value = "Panel description") @QueryParam("description") String description,
            @ApiParam(value = "Panel author") @QueryParam("author") String author,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Release") @QueryParam("release") String release,

            @ApiParam(value = "Create a new version of panel", defaultValue = "false")
                @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(name = "params", value = "Panel parameters") PanelUpdateParams panelParams) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(panelManager.update(studyStr, query, panelParams, true, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{panels}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update panel attributes", response = Panel.class)
    public Response updatePanel(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of panel ids") @PathParam("panels") String panels,
            @ApiParam(value = "Create a new version of panel", defaultValue = "false")
            @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(name = "params", value = "Panel parameters") PanelUpdateParams panelParams) {
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
            @ApiParam(value = "Panel  version") @QueryParam("version") Integer version,
            @ApiParam(value = "Boolean to retrieve deleted panels", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = "Boolean indicating which panels are queried (installation or study specific panels)",
                    defaultValue = "false") @QueryParam("global") boolean global) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("global");

            if (global) {
                studyStr = PanelManager.INSTALLATION_PANELS;
            }

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
            @ApiParam(value = "Panel name") @QueryParam("name") String name,
            @ApiParam(value = "Panel phenotypes") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Panel variants") @QueryParam("variants") String variants,
            @ApiParam(value = "Panel genes") @QueryParam("genes") String genes,
            @ApiParam(value = "Panel regions") @QueryParam("regions") String regions,
            @ApiParam(value = "Panel categories") @QueryParam("categories") String categories,
            @ApiParam(value = "Panel tags") @QueryParam("tags") String tags,
            @ApiParam(value = "Panel description") @QueryParam("description") String description,
            @ApiParam(value = "Panel author") @QueryParam("author") String author,
            @ApiParam(value = "Boolean to retrieve deleted panels", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION)
            @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION)
            @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = "Boolean indicating which panels are queried (installation or study specific panels)",
                    defaultValue = "false") @QueryParam("global") boolean global,
            @ApiParam(value = "Release value (Current release from the moment the samples were first created)")
            @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of samples in the specified release)") @QueryParam("snapshot")
                    int snapshot) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("global");

            if (global) {
                studyStr = PanelManager.INSTALLATION_PANELS;
            }

            return createOkResponse(panelManager.search(studyStr, query, queryOptions, token));
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
    @ApiOperation(value = "Returns the acl of the panels. If member is provided, it will only return the acl for the member.", response = Map.class)
    public Response getAcls(
            @ApiParam(value = ParamConstants.PANELS_DESCRIPTION, required = true) @PathParam("panels")
                    String sampleIdsStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "User or group id") @QueryParam("member") String member,
            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false")
            @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(sampleIdsStr);
            return createOkResponse(panelManager.getAcls(studyStr, idList, member,silent, token));
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
            @ApiParam(value = "JSON containing the parameters to update the permissions.", required = true) PanelAclUpdateParams params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new PanelAclUpdateParams());
            AclParams panelAclParams = new AclParams(params.getPermissions(), params.getAction());
            List<String> idList = getIdList(params.getPanel(), false);
            return createOkResponse(panelManager.updateAcl(studyStr, idList, memberId, panelAclParams, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
