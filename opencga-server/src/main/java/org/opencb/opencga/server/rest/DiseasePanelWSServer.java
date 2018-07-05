package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.DiseasePanelManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.DiseasePanel;
import org.opencb.opencga.core.models.OntologyTerm;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.server.rest.json.mixin.PanelMixin;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Path("/{apiVersion}/panels")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Panel", position = 4, description = "Methods for working with 'panels' endpoint")
public class DiseasePanelWSServer extends OpenCGAWSServer {

    private DiseasePanelManager panelManager;

    public DiseasePanelWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        panelManager = catalogManager.getPanelManager();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a panel", response = DiseasePanel[].class)
    public Response createPanel(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
            @ApiParam(value = "Predefined panel id") @QueryParam("panelId") String panelId,
            @ApiParam(name = "params", value = "Panel parameters") PanelPOST params) {
        try {
            // TODO: Check panelId (from installation panel)
            return createOkResponse(panelManager.create(studyStr, params.toPanel(), queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{panel}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update a panel", response = DiseasePanel[].class)
    public Response updatePanel(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
            @ApiParam(value = "Panel id") @PathParam("panel") String panelId,
            @ApiParam(value = "Create a new version of panel", defaultValue = "false")
                @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(name = "params", value = "Panel parameters") PanelPOST panelParams) {
        try {
            return createOkResponse(panelManager.update(studyStr, panelId, panelParams.toObjectMap(), queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{panels}/info")
    @ApiOperation(value = "Panel info", response = DiseasePanel[].class)
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
            @ApiParam(value = "Fetch all panel versions", defaultValue = "false") @QueryParam(Constants.ALL_VERSIONS)
                    boolean allVersions,
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false")
                @QueryParam("silent") boolean silent) {
        try {
            query.remove("study");

            List<String> idList = getIdList(panelStr);
            List<QueryResult<DiseasePanel>> panelQueryResult = panelManager.get(studyStr, idList, query, queryOptions, silent, sessionId);
            return createOkResponse(panelQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/search")
    @ApiOperation(value = "Panel search", response = DiseasePanel[].class)
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
            @ApiParam(value = "Panel description") @QueryParam("description") String description,
            @ApiParam(value = "Panel author") @QueryParam("author") String author,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @QueryParam("attributes") String attributes,
            @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)") @QueryParam("nattributes") String nattributes,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
            @ApiParam(value = "Release value (Current release from the moment the samples were first created)")
                @QueryParam("release") String release,
            @ApiParam(value = "Snapshot value (Latest version of samples in the specified release)") @QueryParam("snapshot")
                    int snapshot) {
        try {
            query.remove("study");
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            QueryResult<DiseasePanel> queryResult;
            if (count) {
                queryResult = panelManager.count(studyStr, query, sessionId);
            } else {
                queryResult = panelManager.search(studyStr, query, queryOptions, sessionId);
            }
            return createOkResponse(queryResult);
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
            @ApiParam(value = "Panel description") @QueryParam("description") String description,
            @ApiParam(value = "Panel author") @QueryParam("author") String author,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Release") @QueryParam("release") String release) {
        try {
            query.remove("study");
            return createOkResponse(panelManager.delete(studyStr, query, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group panels by several fields", position = 10,
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

            QueryResult result = panelManager.groupBy(studyStr, query, fields, queryOptions, sessionId);
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
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false")
                @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(sampleIdsStr);
            return createOkResponse(panelManager.getAcls(studyStr, idList, member,silent, sessionId));
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
            List<String> idList = getIdList(params.panel);
            return createOkResponse(panelManager.updateAcl(studyStr, idList, memberId, panelAclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class PanelPOST {
        public String id;
        public String name;
        public String description;
        public String author;
        public DiseasePanel.SourcePanel source;

        public List<OntologyTerm> phenotypes;
        public List<String> variants;
        public List<DiseasePanel.GenePanel> genes;
        public List<DiseasePanel.RegionPanel> regions;
        public Map<String, Object> attributes;

        DiseasePanel toPanel() {
            return new DiseasePanel(id, name, 1, 1, author, source, description, phenotypes, variants, genes, regions, attributes);
        }

        ObjectMap toObjectMap() throws JsonProcessingException {
            ObjectMapper mapper = new ObjectMapper();
            mapper.addMixIn(DiseasePanel.class, PanelMixin.class);
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

            DiseasePanel panel = new DiseasePanel()
                    .setId(id)
                    .setName(name)
                    .setAuthor(author)
                    .setSource(source)
                    .setDescription(description)
                    .setPhenotypes(phenotypes)
                    .setVariants(variants)
                    .setGenes(genes)
                    .setRegions(regions)
                    .setAttributes(attributes);

            return new ObjectMap(mapper.writeValueAsString(panel));
        }
    }


}
