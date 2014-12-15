package org.opencb.opencga.server;


import com.wordnik.swagger.annotations.*;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Path("/studies")
@Api(value = "studies", description = "studies", position = 3)
public class StudyWSServer extends OpenCGAWSServer {

    public StudyWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create study with POST method", response = QueryResult.class, position = 1, notes = "Work in progress.<br>" +
            "Only few parameters accepted.<br>" +
            "<b>{ alias, name, type, description, files:[ { format, bioformat, path, description, type, jobId, attributes } ] }</b>")
    public Response createStudyPOST(
            @ApiParam(value = "projectId", required = true) @QueryParam("projectId") int projectId,
            @ApiParam(value="studies", required = true) List<Study> studies
    ) {
        List<Study> catalogStudies = new LinkedList<>();
        for (Study study : studies) {
            System.out.println("study = " + study);
            try {
                QueryResult<Study> queryResult = catalogManager.createStudy(projectId, study.getName(),
                        study.getAlias(), study.getType(), study.getDescription(), sessionId);
                Study studyAdded = queryResult.getResult().get(0);
                catalogStudies.add(studyAdded);
                System.out.println(study.getFiles());
                System.out.println(study.getFiles().size());
                System.out.println(study.getFiles().get(0));
                for (File file : study.getFiles()) {
//                    QueryResult<File> fileQueryResult = catalogManager.createFile(studyAdded.getId(), file.getType(), file.getFormat(), file.getBioformat(),
//                            file.getPath(), file.getDescription(), true, file.getJobId() > 0? file.getJobId() : -1, sessionId, file.getAttributes());
                    QueryResult<File> fileQueryResult = catalogManager.createFile(studyAdded.getId(), file.getFormat(), file.getBioformat(),
                            file.getPath(), file.getDescription(), true, file.getJobId() > 0? file.getJobId() : -1, sessionId);
                    file = fileQueryResult.getResult().get(0);
                    System.out.println("fileQueryResult = " + fileQueryResult);
                    studyAdded.getFiles().add(file);
                }
            } catch (CatalogManagerException | CatalogIOManagerException | IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        return createOkResponse(catalogStudies);
    }



    @GET
    @Path("/create")
    @Produces("application/json")
    @ApiOperation(value = "Create study with GET method", position = 2)
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

        } catch (CatalogManagerException | CatalogIOManagerException | IOException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }

    @GET
    @Path("/{studyId}/info")
    @Produces("application/json")
    @ApiOperation(value = "Study information", position = 3)

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
    @Path("/{studyId}/files")
    @Produces("application/json")
    @ApiOperation(value = "Study information", position = 5)
    public Response getAllFiles(@ApiParam(value = "studyId", required = true) @PathParam("studyId") int studyId) {
        QueryResult queryResult;
        try {
            queryResult = catalogManager.getAllFiles(studyId, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogManagerException e) {
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
    @ApiOperation(value = "Study modify", position = 4)
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
        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}