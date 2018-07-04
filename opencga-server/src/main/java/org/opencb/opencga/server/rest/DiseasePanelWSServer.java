package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.DiseasePanelManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.*;
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
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
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
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
            @ApiParam(value = "Panel id") @PathParam("panel") String panelId,
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
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
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
    @Path("/{panels}/search")
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
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
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
