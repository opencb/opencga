package org.opencb.opencga.server;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.beans.Project;
import org.opencb.opencga.catalog.core.beans.User;
import org.opencb.opencga.catalog.core.db.CatalogManagerException;
import org.opencb.opencga.catalog.core.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.TimeUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.Date;

@Path("/projects")
@Api(value = "projects", description = "projects")
public class ProjectWSServer extends OpenCGAWSServer {

    public ProjectWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @Produces("text/plain")
    @ApiOperation(value = "Just to create the api")


    //createProject(String userId, Project project, String sessionId)
    public Response createProject(
            @ApiParam(value = "userId", required = true) @QueryParam("userId") String userId,
            @ApiParam(value = "sessionId", required = true) @QueryParam("sessionId") String sessionId,
            @ApiParam(value = "name", required = true) @QueryParam("name") String name,
            @ApiParam(value = "alias", required = true) @QueryParam("alias") String alias,
            @ApiParam(value = "description", required = true) @QueryParam("description") String description,
            @ApiParam(value = "organization", required = true) @QueryParam("organization") String organization) {

        Date dt = new Date();

        String creationDate = TimeUtils.getTime();
        String status = "initial";
        String lastActivity = creationDate;
        long diskUsage = Long.MIN_VALUE;
//        System.out.println("Project project = new Project("+name+","+alias+","+creationDate+","+description+"," +status+","+lastActivity+"," +diskUsage+","+organization+")");
//        System.out.println("catalogManager.createProject(" + userId + ",project, " + sessionId + ")");
//        Project project = new Project(name, alias, creationDate, description, status, lastActivity, diskUsage,  organization);
        QueryResult queryResult;
        try {

            Project p = new Project("Project about some genomes", "1000G", "Today", "Cool", "", "", 1000, "");
            queryResult = catalogManager.createProject(userId, p, sessionId);

          //  queryResult = catalogManager.createProject(userId, project, sessionId);
            return createOkResponse(queryResult);

        } catch (CatalogManagerException | CatalogIOManagerException | JsonProcessingException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }

    @GET
    @Path("/{userId}/login")
    @Produces("text/plain")
    @ApiOperation(value = "User login")

    public Response login(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "password", required = false) @QueryParam("password") String password){
        QueryResult queryResult;
        try {
            if (userId.toLowerCase().equals("anonymous")) {
                queryResult = catalogManager.loginAsAnonymous(sessionIp);
            }
            else{
                queryResult = catalogManager.login(userId, password, sessionIp);
            }
            return createOkResponse(queryResult);
        } catch (CatalogManagerException | IOException | CatalogIOManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{userId}/logout")
    @Produces("text/plain")
    @ApiOperation(value = "User login")
    public Response logout(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "sessionId", required = true) @QueryParam("sessionId") String sessionId) throws IOException {
        try {
            QueryResult result;
            if (userId.toLowerCase().equals("anonymous")) {
                result = catalogManager.logoutAnonymous(sessionId);
            } else {
                result = catalogManager.logout(userId, sessionId);
            }
            return createOkResponse(result);
        } catch (CatalogManagerException | IOException | CatalogIOManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }
    @GET
    @Path("/{userId}/info")
    @Produces("text/plain")
    @ApiOperation(value = "User info")
    public Response getInfo(
            @ApiParam(value = "userId", required = true) @PathParam("userId") String userId,
            @ApiParam(value = "sessionId", required = true) @QueryParam("sessionId") String sessionId,
            @ApiParam(value = "lastActivity", required = false) @QueryParam("lastActivity") String lastActivity) throws IOException {
        try {
            QueryResult result = catalogManager.getUserInfo(userId, lastActivity, sessionId);
            return createOkResponse(result);
        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}