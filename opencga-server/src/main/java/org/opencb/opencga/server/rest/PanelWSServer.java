package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.managers.PanelManager;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.server.rest.json.mixin.PanelMixin;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;

@Path("/{apiVersion}/panels")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Panel", position = 4, description = "Methods for working with 'panels' endpoint")
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
    @ApiOperation(value = "Create a panel", response = Panel[].class)
    public Response createPanel(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
            @ApiParam(value = "Predefined panel id") @QueryParam("panelId") String panelId,
            @ApiParam(name = "params", value = "Panel parameters") Panel params) {
        try {
            // TODO: Check if the user has passed the panelId

            return createOkResponse(panelManager.create(studyStr, params, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{panel}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update a panel", response = Panel[].class)
    public Response updatePanel(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
            @ApiParam(value = "Panel id") @PathParam("panel") String panelId,
            @ApiParam(name = "params", value = "Panel parameters") Panel panelParams) {
        try {
            // TODO: Check if the user has passed the panelId

            ObjectMapper mapper = new ObjectMapper();
            mapper.addMixIn(Panel.class, PanelMixin.class);
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            ObjectMap params = new ObjectMap(mapper.writeValueAsString(panelParams));

            return createOkResponse(panelManager.update(studyStr, panelId, params, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{panels}/info")
    @ApiOperation(value = "Panel info", response = Panel[].class)
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
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false")
                @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(panelStr);
            List<QueryResult<Panel>> panelQueryResult = panelManager.get(studyStr, idList, query, queryOptions, silent, sessionId);
            return createOkResponse(panelQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{panels}/search")
    @ApiOperation(value = "Panel search", response = Panel[].class)
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
            @ApiParam(value = "Panel version") @QueryParam("version") int version,
            @ApiParam(value = "Panel author") @QueryParam("author") String author) {
        try {
            QueryResult<Panel> panelQueryResult = panelManager.get(studyStr, query, queryOptions, sessionId);
            return createOkResponse(panelQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
