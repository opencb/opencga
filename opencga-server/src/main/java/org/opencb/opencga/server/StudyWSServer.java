/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.server;


import com.wordnik.swagger.annotations.*;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.Study;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Collections;
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
    @ApiOperation(value = "Create a file with POST method", response = QueryResult.class, position = 1, notes =
            "Wont't accept files, jobs, experiments, samples.<br>" +
            "Will accept (but not yet): acl, uri, cohorts, datasets.<br>" +
//            "Work in progress.<br>" +
//            "Only nested files parameter accepted, and only a few parameters.<br>" +
//            "<b>{ files:[ { format, bioformat, path, description, type, jobId, attributes } ] }</b><br>" +
            "<ul>" +
            "<il><b>id</b>, <b>lastActivity</b> and <b>diskUsage</b> parameters will be ignored.<br></il>" +
            "<il><b>type</b> accepted values: [<b>'CASE_CONTROL', 'CASE_SET', 'CONTROL_SET', 'FAMILY', 'PAIRED', 'TRIO'</b>].<br></il>" +
            "<il><b>creatorId</b> should be the same as que sessionId user (unless you are admin) </il>" +
            "<ul>")
    public Response createStudyPOST(
            @ApiParam(value = "projectId", required = true) @QueryParam("projectId") String projectIdStr,
            @ApiParam(value="studies", required = true) List<Study> studies
    ) {
//        List<Study> catalogStudies = new LinkedList<>();
        List<QueryResult<Study>> queryResults = new LinkedList<>();
        int projectId;
        try {
            projectId = catalogManager.getProjectId(projectIdStr);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
        for (Study study : studies) {
            System.out.println("study = " + study);
            try {
                QueryResult<Study> queryResult = catalogManager.createStudy(projectId, study.getName(),
                        study.getAlias(), study.getType(), study.getCreatorId(), study.getCreationDate(),
                        study.getDescription(), study.getStatus(), study.getCipher(), null, null, null, study.getStats(),
                        study.getAttributes(), this.getQueryOptions(), sessionId);
                Study studyAdded = queryResult.getResult().get(0);
                queryResults.add(queryResult);
//                List<File> files = study.getFiles();
//                if(files != null) {
//                    for (File file : files) {
//                        QueryResult<File> fileQueryResult = catalogManager.createFile(studyAdded.getId(), file.getType(), file.getFormat(),
//                                file.getBioformat(), file.getPath(), file.getOwnerId(), file.getCreationDate(),
//                                file.getDescription(), file.getStatus(), file.getDiskUsage(), file.getExperimentId(),
//                                file.getSampleIds(), file.getJobId(), file.getStats(), file.getAttributes(), true, sessionId);
//                        file = fileQueryResult.getResult().get(0);
//                        System.out.println("fileQueryResult = " + fileQueryResult);
//                        studyAdded.getFiles().add(file);
//                    }
//                }
            } catch (CatalogException | IOException e) {
                e.printStackTrace();
//                return createErrorResponse(e.getMessage());
                queryResults.add(new QueryResult<>("createStudy", 0, 0, 0, "", e.getMessage(), Collections.<Study>emptyList()));
            }
        }
        return createOkResponse(queryResults);
    }



    @GET
    @Path("/create")
    @Produces("application/json")
    @ApiOperation(value = "Create study with GET method", position = 2)
    public Response createStudy(
            @ApiParam(value = "projectId",    required = true)  @QueryParam("projectId") String projectIdStr,
            @ApiParam(value = "name",         required = true)  @QueryParam("name") String name,
            @ApiParam(value = "alias",        required = true)  @QueryParam("alias") String alias,
            @ApiParam(value = "type",         required = false) @DefaultValue("CASE_CONTROL") @QueryParam("type") Study.Type type,
            @ApiParam(value = "creatorId",    required = false) @QueryParam("creatorId") String creatorId,
            @ApiParam(value = "creationDate", required = false) @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "description",  required = false) @QueryParam("description") String description,
            @ApiParam(value = "status",       required = false) @QueryParam("status") String status,
            @ApiParam(value = "cipher",       required = false) @QueryParam("cipher") String cipher
            ) {


        QueryResult queryResult;
        try {
            int projectId = catalogManager.getProjectId(projectIdStr);
            queryResult = catalogManager.createStudy(projectId, name, alias, type, creatorId,
                    creationDate, description, status, cipher, null, null, null, null, null, this.getQueryOptions(), sessionId);

            return createOkResponse(queryResult);

        } catch (CatalogException | IOException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }

    }

    @GET
    @Path("/{studyId}/info")
    @Produces("application/json")
    @ApiOperation(value = "Study information", position = 3)

    public Response info(
            @ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdsStr
    ) {
        List<QueryResult<Study>> queryResults = new LinkedList<>();
        for (String studyIdStr : studyIdsStr.split(",")) {
            try {
                int studyId = catalogManager.getStudyId(studyIdStr);
                queryResults.add(catalogManager.getStudy(studyId, sessionId, this.getQueryOptions()));
            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse(e.getMessage());
            }
        }
        return createOkResponse(queryResults);
    }

    @GET
    @Path("/{studyId}/files")
    @Produces("application/json")
    @ApiOperation(value = "Study files information", position = 5)
    public Response getAllFiles(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        QueryResult queryResult;
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            queryResult = catalogManager.getAllFiles(studyId, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{studyId}/samples")
    @Produces("application/json")
    @ApiOperation(value = "Study samples information", position = 5)
    public Response getAllSamples(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        QueryResult queryResult;
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            queryResult = catalogManager.getAllSamples(studyId, this.getQueryOptions(), sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogException e) {
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
            @ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
            @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
            @ApiParam(value = "type", required = false) @DefaultValue("") @QueryParam("type") String type,
            @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description") String description,
            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status)
//            @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes,
//            @ApiParam(value = "stats", required = false) @QueryParam("stats") String stats)
            throws IOException {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
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
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{studyId}/job")
    @Produces("application/json")
    @ApiOperation(value = "Get all jobs")
    public Response getAllJobs(
            @ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            return createOkResponse(catalogManager.getAllJobs(studyId, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e.getMessage());
        }
    }

}