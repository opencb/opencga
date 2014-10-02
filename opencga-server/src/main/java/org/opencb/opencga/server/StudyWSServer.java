package org.opencb.opencga.server;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.Project;
import org.opencb.opencga.catalog.core.beans.Study;
import org.opencb.opencga.catalog.core.db.CatalogManagerException;
import org.opencb.opencga.catalog.core.io.CatalogIOManagerException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/studies")
@Api(value = "studies", description = "studies")
public class StudyWSServer extends OpenCGAWSServer {

    public StudyWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @Produces("text/plain")
    @ApiOperation(value = "Create study")

    public Response createStudy(
            @ApiParam(value = "projectId", required = true) @QueryParam("projectId") int projectId,
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
            @ApiParam(value = "alias", required = true) @QueryParam("alias") String alias,
            @ApiParam(value = "type", required = true) @QueryParam("type") String type,
            @ApiParam(value = "description", required = true) @QueryParam("description") String description
    ) {


        QueryResult queryResult;
        try {

            queryResult = catalogManager.createStudy(projectId, name, alias, type, description, sessionId);

            return createOkResponse(queryResult);

        } catch (CatalogManagerException | CatalogIOManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }

    @GET
    @Path("/{studyId}/info")
    @Produces("text/plain")
    @ApiOperation(value = "Study information")

    public Response info(
            @ApiParam(value = "studyId", required = true) @PathParam("studyId") int studyId
    ) {
        QueryResult queryResult;
        try {
            queryResult = catalogManager.getStudy(studyId, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{projectId}/all-studies")
    @Produces("text/plain")
    @ApiOperation(value = "Study information")

    public Response getAllStudies(
            @ApiParam(value = "projectId", required = true) @PathParam("projectId") int projectId
    ) {
        QueryResult queryResult;
        try {
            queryResult = catalogManager.getAllStudies(projectId, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogManagerException | JsonProcessingException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{studyId}/modify")
    @Produces("text/plain")
    @ApiOperation(value = "Study modify")
    public Response modifyUser(
            @ApiParam(value = "studyId", required = true) @PathParam("studyId") int studyId,
            @ApiParam(value = "name", required = false) @QueryParam("name") String name,
            @ApiParam(value = "type", required = false) @QueryParam("type") String type,
            @ApiParam(value = "description", required = false) @QueryParam("description") String description,
            @ApiParam(value = "status", required = false) @QueryParam("status") String status)
//            @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes,
//            @ApiParam(value = "stats", required = false) @QueryParam("stats") String stats)
            throws IOException {
        try {
            ObjectMap objectMap = new ObjectMap();
            objectMap.put("name", name);
            objectMap.put("type", type);
            objectMap.put("description", description);
            objectMap.put("status", status);
//            objectMap.put("attributes", attributes);
//            objectMap.put("stats", stats);

            QueryResult result = catalogManager.modifyStudy(studyId, objectMap, sessionId);
            return createOkResponse(result);
        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}