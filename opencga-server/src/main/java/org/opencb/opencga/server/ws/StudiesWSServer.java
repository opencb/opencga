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

package org.opencb.opencga.server.ws;

import com.wordnik.swagger.annotations.*;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.files.FileScanner;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.core.exception.VersionException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;


@Path("/{version}/studies")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Studies", position = 3, description = "Methods for working with 'studies' endpoint")
public class StudiesWSServer extends OpenCGAWSServer {


    public StudiesWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                           @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create study with GET method", position = 1)
    public Response createStudy(@ApiParam(value = "projectId",    required = true)  @QueryParam("projectId") String projectIdStr,
                                @ApiParam(value = "name",         required = true)  @QueryParam("name") String name,
                                @ApiParam(value = "alias",        required = true)  @QueryParam("alias") String alias,
                                @ApiParam(value = "type",         required = false) @DefaultValue("CASE_CONTROL") @QueryParam("type") Study.Type type,
                                @ApiParam(value = "creatorId",    required = false) @QueryParam("creatorId") String creatorId,
                                @ApiParam(value = "creationDate", required = false) @QueryParam("creationDate") String creationDate,
                                @ApiParam(value = "description",  required = false) @QueryParam("description") String description,
                                @ApiParam(value = "status",       required = false) @QueryParam("status") String status,
                                @ApiParam(value = "cipher",       required = false) @QueryParam("cipher") String cipher) {
        try {
            int projectId = catalogManager.getProjectId(projectIdStr);
            QueryResult queryResult = catalogManager.createStudy(projectId, name, alias, type, creatorId,
                    creationDate, description, status, cipher, null, null, null, null, null, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
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
    public Response createStudyPOST(@ApiParam(value = "projectId", required = true) @QueryParam("projectId") String projectIdStr,
                                    @ApiParam(value="studies", required = true) List<Study> studies) {
//        List<Study> catalogStudies = new LinkedList<>();
        List<QueryResult<Study>> queryResults = new LinkedList<>();
        int projectId;
        try {
            projectId = catalogManager.getProjectId(projectIdStr);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e);
        }
        for (Study study : studies) {
            System.out.println("study = " + study);
            try {
                QueryResult<Study> queryResult = catalogManager.createStudy(projectId, study.getName(),
                        study.getAlias(), study.getType(), study.getCreatorId(), study.getCreationDate(),
                        study.getDescription(), study.getStatus(), study.getCipher(), null, null, null, study.getStats(),
                        study.getAttributes(), queryOptions, sessionId);
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
            } catch (Exception e) {
//                queryResults.add(new QueryResult<>("createStudy", 0, 0, 0, "", e, Collections.<Study>emptyList()));
                return createErrorResponse(e);
            }
        }
        return createOkResponse(queryResults);
    }

    @GET
    @Path("/{studyId}/info")
    @ApiOperation(value = "Study information", position = 2)
    public Response info(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdsStr) {
        try {
            String[] studyIdArray = studyIdsStr.split(",");
            List<QueryResult<Study>> queryResults = new LinkedList<>();
            for (String studyIdStr : studyIdArray) {
                int studyId = catalogManager.getStudyId(studyIdStr);
                queryResults.add(catalogManager.getStudy(studyId, sessionId, queryOptions));
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/files")
    @ApiOperation(value = "Study files information", position = 3)
    public Response getAllFiles(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            QueryResult queryResult = catalogManager.getAllFiles(studyId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/jobs")
    @ApiOperation(value = "Get all jobs", position = 4)
    public Response getAllJobs(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            return createOkResponse(catalogManager.getAllJobs(studyId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/samples")
    @ApiOperation(value = "Study samples information", position = 5)
    public Response getAllSamples(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            QueryResult queryResult = catalogManager.getAllSamples(studyId, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/variants")
    @ApiOperation(value = "Study samples information", position = 6)
    public Response getVariants(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        return createOkResponse("PENDING");
    }

    @GET
    @Path("/{studyId}/alignments")
    @ApiOperation(value = "Study samples information", position = 7)
    public Response getAlignments(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        return createOkResponse("PENDING");
    }

    @GET
    @Path("/{studyId}/status")
    @ApiOperation(value = "Scans the study folder to find untracked or missing files", position = 8)
    public Response status(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        try {
            int studyId = catalogManager.getStudyId(studyIdStr);
            Study study = catalogManager.getStudy(studyId, sessionId).first();
            FileScanner fileScanner = new FileScanner(catalogManager);

            /** First, run CheckStudyFiles to find new missing files **/
            List<File> checkStudyFiles = fileScanner.checkStudyFiles(study, false, sessionId);
            List<File> found = checkStudyFiles.stream().filter(f -> f.getStatus().equals(File.Status.READY)).collect(Collectors.toList());

            /** Get untracked files **/
            Map<String, URI> untrackedFiles = fileScanner.untrackedFiles(study, sessionId);

            /** Get missing files **/
            List<File> missingFiles = catalogManager.getAllFiles(studyId, new QueryOptions("status", File.Status.MISSING), sessionId).getResult();

            ObjectMap fileStatus = new ObjectMap("untracked", untrackedFiles).append("found", found).append("missing", missingFiles);

            return createOkResponse(new QueryResult<>("status", 0, 1, 1, null, null, Collections.singletonList(fileStatus)));
//            /** Print pretty **/
//            int maxFound = found.stream().map(f -> f.getPath().length()).max(Comparator.<Integer>naturalOrder()).orElse(0);
//            int maxUntracked = untrackedFiles.keySet().stream().map(String::length).max(Comparator.<Integer>naturalOrder()).orElse(0);
//            int maxMissing = missingFiles.stream().map(f -> f.getPath().length()).max(Comparator.<Integer>naturalOrder()).orElse(0);
//
//            String format = "\t%-" + Math.max(Math.max(maxMissing, maxUntracked), maxFound) + "s  -> %s\n";
//
//            if (!untrackedFiles.isEmpty()) {
//                System.out.println("UNTRACKED files");
//                untrackedFiles.forEach((s, u) -> System.out.printf(format, s, u));
//                System.out.println("\n");
//            }
//
//            if (!missingFiles.isEmpty()) {
//                System.out.println("MISSING files");
//                for (File file : missingFiles) {
//                    System.out.printf(format, file.getPath(), catalogManager.getFileUri(file));
//                }
//                System.out.println("\n");
//            }
//
//            if (!found.isEmpty()) {
//                System.out.println("FOUND files");
//                for (File file : found) {
//                    System.out.printf(format, file.getPath(), catalogManager.getFileUri(file));
//                }
//            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/update")
    @ApiOperation(value = "Study modify", position = 9)
    public Response update(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
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
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/delete")
    @ApiOperation(value = "Delete a study [PENDING]", position = 10)
    public Response delete(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyId) {
        return createOkResponse("PENDING");
    }

}