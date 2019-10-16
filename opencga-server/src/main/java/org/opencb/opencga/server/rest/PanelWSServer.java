package org.opencb.opencga.server.rest;

import io.swagger.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.managers.PanelManager;
import org.opencb.opencga.catalog.models.update.PanelUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.Status;
import org.opencb.opencga.core.models.acls.AclParams;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opencb.biodata.models.clinical.interpretation.DiseasePanel.*;

@Path("/{apiVersion}/panels")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Panels", position = 4, description = "Methods for working with 'panels' endpoint")
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
    @ApiOperation(value = "Create a panel")
    public Response createPanel(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
            @ApiParam(value = "Comma separated list of installation panel ids to be imported. To import them all at once, write the "
                    + "special word 'ALL_GLOBAL_PANELS'")
            @QueryParam("import") String panelIds,
            @ApiParam(name = "params", value = "Panel parameters") PanelPOST params) {
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
    @ApiOperation(value = "Update panel attributes")
    public Response updateByQuery(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
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
            query.remove("study");
            return createOkResponse(panelManager.update(studyStr, query, panelParams, true, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{panels}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update panel attributes")
    public Response updatePanel(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
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
    @ApiOperation(value = "Panel info")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query")
    })
    public Response info(
            @ApiParam(value = "Comma separated list of panel ids up to a maximum of 100") @PathParam(value = "panels") String panelStr,
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
            @ApiParam(value = "Panel  version") @QueryParam("version") Integer version,
            @ApiParam(value = "Boolean to retrieve deleted panels", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = "Boolean indicating which panels are queried (installation or study specific panels)",
                    defaultValue = "false") @QueryParam("global") boolean global) {
        try {
            query.remove("study");
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
    @ApiOperation(value = "Panel search")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
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
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
            @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
            @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "Boolean indicating which panels are queried (installation or study specific panels)",
                    defaultValue = "false") @QueryParam("global") boolean global,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
            @ApiParam(value = "Release value (Current release from the moment the samples were first created)")
            @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of samples in the specified release)") @QueryParam("snapshot")
                    int snapshot) {
        try {
            query.remove("study");
            query.remove("global");

            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (global) {
                studyStr = PanelManager.INSTALLATION_PANELS;
            }

            DataResult<Panel> queryResult;
            if (count) {
                queryResult = panelManager.count(studyStr, query, token);
            } else {
                queryResult = panelManager.search(studyStr, query, queryOptions, token);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{panels}/delete")
    @ApiOperation(value = "Delete existing panels")
    public Response deleteList(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
            @ApiParam(value = "Comma separated list of panel ids") @PathParam("panels") String panels) {
        try {
            return createOkResponse(panelManager.delete(studyStr, getIdList(panels), queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/delete")
    @ApiOperation(value = "Delete existing panels")
    public Response delete(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
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
            @ApiParam(value = "Release") @QueryParam("release") String release) {
        try {
            query.remove("study");
            return createOkResponse(panelManager.delete(studyStr, query, queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group panels by several fields", position = 10, hidden = true,
            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(
            @ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @QueryParam("fields") String fields,
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
            @ApiParam(value = "Panel name") @QueryParam("name") String name,
            @ApiParam(value = "Panel phenotypes") @QueryParam("phenotypes") String phenotypes,
            @ApiParam(value = "Panel categories") @QueryParam("categories") String categories,
            @ApiParam(value = "Panel tags") @QueryParam("tags") String tags,
            @ApiParam(value = "Panel variants") @QueryParam("variants") String variants,
            @ApiParam(value = "Panel genes") @QueryParam("genes") String genes,
            @ApiParam(value = "Panel regions") @QueryParam("regions") String regions,
            @ApiParam(value = "Panel description") @QueryParam("description") String description,
            @ApiParam(value = "Panel author") @QueryParam("author") String author,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Release value (Current release from the moment the families were first created)") @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of families in the specified release)") @QueryParam("snapshot") int snapshot) {
        try {
            query.remove("study");
            query.remove("fields");

            DataResult result = panelManager.groupBy(studyStr, query, fields, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{panels}/acl")
    @ApiOperation(value = "Returns the acl of the panels. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(
            @ApiParam(value = "Comma separated list of panel ids up to a maximum of 100", required = true) @PathParam("panels")
                    String sampleIdsStr,
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
            @ApiParam(value = "User or group id") @QueryParam("member") String member,
            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                    + "exception whenever one of the entries looked for cannot be shown for whichever reason", defaultValue = "false")
            @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(sampleIdsStr);
            return createOkResponse(panelManager.getAcls(studyStr, idList, member,silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class PanelAcl extends AclParams {
        public String panel;
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = "JSON containing the parameters to update the permissions.", required = true) PanelAcl params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new PanelAcl());
            AclParams panelAclParams = new AclParams(params.getPermissions(), params.getAction());
            List<String> idList = getIdList(params.panel, false);
            return createOkResponse(panelManager.updateAcl(studyStr, idList, memberId, panelAclParams, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class PanelPOST {
        public String id;
        public String name;
        public String description;
        @Deprecated
        public String author;
        public SourcePanel source;

        public List<PanelCategory> categories;
        public List<String> tags;
        public List<Phenotype> phenotypes;
        public List<VariantPanel> variants;
        public List<GenePanel> genes;
        public List<RegionPanel> regions;
        public List<STR> strs;

        public Map<String, Integer> stats;

        public Map<String, Object> attributes;

        public Panel toPanel() {
            return new Panel(id, name, categories, phenotypes, tags, variants, genes, regions, strs, stats, 1, 1, author,
                    source, new Status(), description, attributes);
        }
    }


}
