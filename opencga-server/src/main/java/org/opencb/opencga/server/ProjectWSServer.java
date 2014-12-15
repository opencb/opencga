package org.opencb.opencga.server;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

@Path("/projects")
@Api(value = "projects", description = "projects", position = 2)
public class ProjectWSServer extends OpenCGAWSServer {

    public ProjectWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @Produces("application/json")
    @ApiOperation(value = "Create project")
    public Response createProject(
            @ApiParam(value = "userId", required = true) @QueryParam("userId") String userId,
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
            @ApiParam(value = "alias", required = true) @QueryParam("alias") String alias,
            @ApiParam(value = "description", required = true) @QueryParam("description") String description,
            @ApiParam(value = "organization", required = true) @QueryParam("organization") String organization) {


        QueryResult queryResult;
        try {

            queryResult = catalogManager.createProject(userId, name, alias, description, organization, sessionId);

            return createOkResponse(queryResult);

        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }

    @GET
    @Path("/{projectId}/info")
    @Produces("application/json")
    @ApiOperation(value = "Project information")
    public Response info(
            @ApiParam(value = "projectId", required = true) @PathParam("projectId") int projectId
    ) {
        QueryResult queryResult;
        try {
            queryResult = catalogManager.getProject(projectId, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{projectId}/studies")
    @Produces("application/json")
    @ApiOperation(value = "Study information")

    public Response getAllStudies(
            @ApiParam(value = "projectId", required = true) @PathParam("projectId") int projectId
    ) {
        QueryResult queryResult;
        try {
            queryResult = catalogManager.getAllStudies(projectId, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{projectId}/modify")
    @Produces("application/json")
    @ApiOperation(value = "Project modify")
    public Response modifyUser(
            @ApiParam(value = "projectId", required = true) @PathParam("projectId") int projectId,
            @ApiParam(value = "name", required = false) @QueryParam("name") String name,
            @ApiParam(value = "description", required = false) @QueryParam("description") String description,
            @ApiParam(value = "organization", required = false) @QueryParam("organization") String organization,
            @ApiParam(value = "status", required = false) @QueryParam("status") String status,
            @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes)
            throws IOException {
        try {
            ObjectMap objectMap = new ObjectMap();
            objectMap.put("name", name);
            objectMap.put("description", description);
            objectMap.put("organization", organization);
            objectMap.put("status", status);
            objectMap.put("attributes", attributes);

            QueryResult result = catalogManager.modifyProject(projectId, objectMap, sessionId);
            return createOkResponse(result);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}