package org.opencb.opencga.server;


import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;

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
    @Produces("application/json")
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

            queryResult = catalogManager.createStudy(projectId, name, alias, Study.StudyType.valueOf(type), description, sessionId);

            return createOkResponse(queryResult);

        } catch (CatalogDBException | CatalogIOManagerException | IOException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }

    @GET
    @Path("/{studyId}/info")
    @Produces("application/json")
    @ApiOperation(value = "Study information")

    public Response info(
            @ApiParam(value = "studyId", required = true) @PathParam("studyId") int studyId
    ) {
        QueryResult queryResult;
        try {
            queryResult = catalogManager.getStudy(studyId, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogDBException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{studyId}/files")
    @Produces("application/json")
    @ApiOperation(value = "Study information")
    public Response getAllFiles(@ApiParam(value = "studyId", required = true) @PathParam("studyId") int studyId) {
        QueryResult queryResult;
        try {
            queryResult = catalogManager.getAllFiles(studyId, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogDBException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

//    @GET
//    @Path("/{studyId}/analysis")
//    @Produces("application/json")
//    @ApiOperation(value = "Study information")
//    public Response getAllAnalysis(@ApiParam(value = "studyId", required = true) @PathParam("studyId") int studyId) {
//        QueryResult queryResult;
//        try {
//            queryResult = catalogManager.getAllAnalysis(studyId, sessionId);
//            return createOkResponse(queryResult);
//        } catch (CatalogManagerException e) {
//            e.printStackTrace();
//            return createErrorResponse(e.getMessage());
//        }
//    }

    @GET
    @Path("/{studyId}/modify")
    @Produces("application/json")
    @ApiOperation(value = "Study modify")
    public Response modifyStudy(
            @ApiParam(value = "studyId", required = true) @PathParam("studyId") int studyId,
            @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
            @ApiParam(value = "type", required = false) @DefaultValue("") @QueryParam("type") String type,
            @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description") String description,
            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status)
//            @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes,
//            @ApiParam(value = "stats", required = false) @QueryParam("stats") String stats)
            throws IOException {
        try {
            ObjectMap objectMap = new ObjectMap();
            if(!name.isEmpty()) {
                objectMap.put("name", name);
            }
            if(!type.isEmpty()) {
                objectMap.put("type", type);
            }
            if(!description.isEmpty()) {
                objectMap.put("description", description);
            }
            if(!status.isEmpty()) {
                objectMap.put("status", status);
            }
//            objectMap.put("attributes", attributes);
//            objectMap.put("stats", stats);
            System.out.println(objectMap.toJson());
            QueryResult result = catalogManager.modifyStudy(studyId, objectMap, sessionId);
            return createOkResponse(result);
        } catch (CatalogDBException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}