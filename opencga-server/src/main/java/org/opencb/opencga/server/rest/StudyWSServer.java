/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.server.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.server.WebServiceException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;


@Path("/{apiVersion}/studies")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Studies", position = 3, description = "Methods for working with 'studies' endpoint")
public class StudyWSServer extends OpenCGAWSServer {

    private StudyManager studyManager;

    public StudyWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        studyManager = catalogManager.getStudyManager();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new study", response = Study.class)
    public Response createStudyPOST(@ApiParam(value = "Project id or alias", required = true) @QueryParam("projectId") String projectIdStr,
                                    @ApiParam(value = "study", required = true) StudyParams study) {
        try {
            return createOkResponse(catalogManager.getStudyManager().create(projectIdStr, study.name, study.alias, study
                    .type, null, study.description, null, null, null, null, null, study.stats, study.attributes, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search studies", response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to be skipped.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Get a count of the number of results obtained. Deactivated by default.",
                    dataType = "boolean", paramType = "query")
    })
    public Response getAllStudies(
            @Deprecated @ApiParam(value = "Project id or alias", hidden = true) @QueryParam("projectId") String projectId,
            @ApiParam(value = "Project id or alias", required = true) @QueryParam("project") String projectStr,
            @ApiParam(value = "Study name") @QueryParam("name") String name,
            @ApiParam(value = "Study alias") @QueryParam("alias") String alias,
            @ApiParam(value = "Type of study: CASE_CONTROL, CASE_SET...") @QueryParam("type") String type,
            @ApiParam(value = "Creation date") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Attributes") @QueryParam("attributes") String attributes,
            @ApiParam(value = "Numerical attributes") @QueryParam("nattributes") String nattributes,
            @ApiParam(value = "Boolean attributes") @QueryParam("battributes") boolean battributes,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            if (StringUtils.isNotEmpty(projectId) && StringUtils.isEmpty(projectStr)) {
                projectStr = projectId;
                query.remove(StudyDBAdaptor.QueryParams.PROJECT_ID.key());
            }

            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            QueryResult<Study> queryResult = catalogManager.getStudyManager().get(projectStr, query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some study attributes")
    public Response updateByPost(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                                 @ApiParam(value = "JSON containing the params to be updated.", required = true) StudyParams updateParams) {
        try {
            String userId = catalogManager.getUserManager().getUserId(sessionId);
            long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
            QueryResult queryResult = catalogManager.getStudyManager().update(String.valueOf((Long) studyId), new ObjectMap
                    (jsonObjectMapper.writeValueAsString(updateParams)), null, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studies}/delete")
    @ApiOperation(value = "Delete a study [WARNING]", response = Study.class,
            notes = "Usage of this webservice might lead to unexpected behaviour and therefore is discouraged to use. Deletes are " +
                    "planned to be fully implemented and tested in version 1.4.0")
    public Response delete(@ApiParam(value = "Comma separated list of study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("studies") String studyStr, @QueryParam("silent") boolean silent) {
        return createOkResponse("PENDING");
    }

    @GET
    @Path("/{studies}/info")
    @ApiOperation(value = "Fetch study information", response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query")
    })
    public Response info(@ApiParam(value = "Comma separated list of studies [[user@]project:]study where study and project can be either the id or alias up to a maximum of 100",
            required = true) @PathParam("studies") String studies,
                         @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(studies);
            return createOkResponse(studyManager.get(idList, queryOptions, silent, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studies}/summary")
    @ApiOperation(value = "Fetch study information plus some basic stats", notes = "Fetch study information plus some basic stats such as"
            + " the number of files, samples, cohorts...")
    public Response summary(@ApiParam(value = "Comma separated list of Studies [[user@]project:]study where study and project can be either the id or alias up to a maximum of 100", required = true)
                            @PathParam("studies") String studies,
                            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(studies);
            return createOkResponse(studyManager.getSummary(idList, queryOptions, silent, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/files")
    @ApiOperation(value = "Fetch files in study [WARNING]", response = File[].class,
            notes = "The use of this webservice is discouraged. The whole functionality is replicated in the files/search endpoint.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to be skipped.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Get a count of the number of results obtained. Deactivated by default.",
                    dataType = "boolean", paramType = "query")
    })
    public Response getAllFiles(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                                @ApiParam(value = "File id") @QueryParam("id") String id,
                                @ApiParam(value = "File name") @QueryParam("name") String name,
                                @ApiParam(value = "File path") @QueryParam("path") String path,
                                @ApiParam(value = "File type (FILE or DIRECTORY)") @QueryParam("type") String type,
                                @ApiParam(value = "Comma separated list of bioformat values. For existing Bioformats see files/bioformats")
                                @QueryParam("bioformat") String bioformat,
                                @ApiParam(value = "Comma separated list of format values. For existing Formats see files/formats")
                                @QueryParam("format") String formats,
                                @ApiParam(value = "File status") @QueryParam("status") File.FileStatus status,
                                @ApiParam(value = "Directory where the files will be looked for") @QueryParam("directory") String directory,
                                @ApiParam(value = "Creation date of the file") @QueryParam("creationDate") String creationDate,
                                @ApiParam(value = "Last modification date of the file") @QueryParam("modificationDate")
                                        String modificationDate,
                                @ApiParam(value = "File description") @QueryParam("description") String description,
                                @ApiParam(value = "File size") @QueryParam("size") Long size,
                                @ApiParam(value = "List of sample ids associated with the files") @QueryParam("sampleIds") String sampleIds,
                                @ApiParam(value = "Job id that generated the file") @QueryParam("jobId") String jobId,
                                @ApiParam(value = "Attributes") @QueryParam("attributes") String attributes,
                                @ApiParam(value = "Numerical attributes") @QueryParam("nattributes") String nattributes) {
        try {
            isSingleId(studyStr);
            String userId = catalogManager.getUserManager().getUserId(sessionId);
            long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
            QueryResult queryResult = catalogManager.getFileManager().get(studyId, query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/samples")
    @ApiOperation(value = "Fetch samples in study [WARNING]", response = Sample[].class,
            notes = "The use of this webservice is discouraged. The whole functionality is replicated in the samples/search endpoint.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to be skipped.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Get a count of the number of results obtained. Deactivated by default.",
                    dataType = "boolean", paramType = "query")
    })
    public Response getAllSamples(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                                  @ApiParam(value = "Sample name") @QueryParam("name") String name,
                                  @Deprecated @ApiParam(value = "source", hidden = true) @QueryParam("source") String source,
                                  @ApiParam(value = "individualId") @QueryParam("individualId") String individualId,
                                  @ApiParam(value = "annotationSetName") @QueryParam("annotationSetName") String annotationSetName,
                                  @ApiParam(value = "variableSetId", hidden = true) @QueryParam("variableSetId") String variableSetId,
                                  @ApiParam(value = "variableSet") @QueryParam("variableSet") String variableSet,
                                  @ApiParam(value = "annotation") @QueryParam("annotation") String annotation) {
        try {
            String userId = catalogManager.getUserManager().getUserId(sessionId);
            long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
            QueryResult queryResult = catalogManager.getSampleManager().get(studyId, query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/jobs")
    @ApiOperation(value = "Return filtered jobs in study [WARNING]", position = 9,
            notes = "The use of this webservice is discouraged. The whole functionality is replicated in the jobs/search endpoint.",
            response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response getAllJobs(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                               @ApiParam(value = "name") @DefaultValue("") @QueryParam("name") String name,
                               @ApiParam(value = "tool name") @DefaultValue("") @QueryParam("toolName") String tool,
                               @ApiParam(value = "status") @DefaultValue("") @QueryParam("status") String status,
                               @ApiParam(value = "ownerId") @DefaultValue("") @QueryParam("ownerId") String ownerId,
                               @ApiParam(value = "date") @DefaultValue("") @QueryParam("date") String date,
                               @ApiParam(value = "Comma separated list of output file ids") @DefaultValue("")
                               @QueryParam("inputFiles") String inputFiles,
                               @ApiParam(value = "Comma separated list of output file ids") @DefaultValue("")
                               @QueryParam("outputFiles") String outputFiles) {
        try {
            return createOkResponse(catalogManager.getJobManager().get(studyStr, new Query(), null, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/scanFiles")
    @ApiOperation(value = "Scan the study folder to find untracked or missing files", position = 12)
    public Response scanFiles(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr) {
        try {
            isSingleId(studyStr);
            Study study = catalogManager.getStudyManager().get(studyStr, null, sessionId).first();
            FileScanner fileScanner = new FileScanner(catalogManager);

            /** First, run CheckStudyFiles to find new missing files **/
            List<File> checkStudyFiles = fileScanner.checkStudyFiles(study, false, sessionId);
            List<File> found = checkStudyFiles
                    .stream()
                    .filter(f -> f.getStatus().getName().equals(File.FileStatus.READY))
                    .collect(Collectors.toList());

            /** Get untracked files **/
            Map<String, URI> untrackedFiles = fileScanner.untrackedFiles(study, sessionId);

            /** Get missing files **/
            List<File> missingFiles = catalogManager.getFileManager().get(studyStr,
                    query.append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.MISSING),
                    queryOptions, sessionId).getResult();

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
    @Path("/{study}/resyncFiles")
    @ApiOperation(value = "Scan the study folder to find untracked or missing files.", notes = "This method is intended to keep the "
            + "consistency between the database and the file system. It will check all the files and folders belonging to the study and "
            + "will keep track of those new files and/or folders found in the file system as well as update the status of those "
            + "files/folders that are no longer available in the file system setting their status to MISSING.")
    public Response resyncFiles(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr) {
        try {
            isSingleId(studyStr);
            String userId = catalogManager.getUserManager().getUserId(sessionId);
            long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
            Study study = catalogManager.getStudyManager().get(String.valueOf((Long) studyId), null, sessionId).first();
            FileScanner fileScanner = new FileScanner(catalogManager);

            /* Resync files */
            List<File> resyncFiles = fileScanner.reSync(study, false, sessionId);

            return createOkResponse(new QueryResult<>("status", 0, 1, 1, null, null, Arrays.asList(resyncFiles)));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studies}/groups")
    @ApiOperation(value = "Return the groups present in the studies", position = 13, response = Group[].class)
    public Response getGroups(
            @ApiParam(value = "Comma separated list of studies [[user@]project:]study where study and project can be either the id or alias up to a maximum of 100", required = true)
            @PathParam("studies") String studiesStr,
            @ApiParam(value = "Group name. If provided, it will only fetch information for the provided group.") @QueryParam("name") String groupId,
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(studiesStr);
            return createOkResponse(catalogManager.getStudyManager().getGroup(idList, groupId, silent, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/create")
    @ApiOperation(value = "Create a group", position = 14)
    public Response createGroupPOST(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
            @PathParam("study") String studyStr,
            @ApiParam(value = "JSON containing the parameters", required = true) GroupCreateParams params) {
        if (StringUtils.isNotEmpty(params.groupId) && StringUtils.isEmpty(params.name)) {
            params.name = params.groupId;
        }
        if (StringUtils.isEmpty(params.name)) {
            return createErrorResponse(new CatalogException("groupId key missing."));
        }
        try {
            QueryResult group = catalogManager.getStudyManager().createGroup(studyStr, params.name, params.users, sessionId);
            return createOkResponse(group);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/{group}/update")
    @ApiOperation(value = "Updates the members of the group")
    public Response addMembersToGroupPOST(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
            @PathParam("study") String studyStr,
            @ApiParam(value = "Group name", required = true) @PathParam("group") String groupId,
            @ApiParam(value = "JSON containing the action to be performed", required = true) GroupParams params) {
        try {
            isSingleId(studyStr);
            return createOkResponse(
                    catalogManager.getStudyManager().updateGroup(studyStr, groupId, params, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/members/update")
    @ApiOperation(value = "Add/Remove users with access to study")
    public Response registerUsersToStudy(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
            @PathParam("study") String studyStr,
            @ApiParam(value = "JSON containing the action to be performed", required = true) MemberParams params) {
        try {
            isSingleId(studyStr);
            return createOkResponse(
                    catalogManager.getStudyManager().updateGroup(studyStr, "@members", params.toGroupParams(), sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/admins/update")
    @ApiOperation(value = "Add/Remove users with administrative permissions to the study.",
            notes = "Only the owner of the study will be able to run this webservice")
    public Response registerAdministrativeUsersToStudy(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
            @PathParam("study") String studyStr,
            @ApiParam(value = "JSON containing the action to be performed", required = true) MemberParams params) {
        try {
            isSingleId(studyStr);
            return createOkResponse(
                    catalogManager.getStudyManager().updateGroup(studyStr, "@admins", params.toGroupParams(), sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/groups/{group}/delete")
    @ApiOperation(value = "Delete the group", position = 17, notes = "Delete the group selected from the study.")
    public Response deleteMembersFromGroup(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
            @PathParam("study") String studyStr,
            @ApiParam(value = "Group name", required = true) @PathParam("group") String groupId) {
        try {
            isSingleId(studyStr);
            return createOkResponse(catalogManager.getStudyManager().deleteGroup(studyStr, groupId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studies}/acl")
    @ApiOperation(value = "Return the acl of the study. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(
            @ApiParam(value = "Comma separated list of studies [[user@]project:]study where study and project can be either the id or alias up to a maximum of 100", required = true)
            @PathParam("studies") String studiesStr,
            @ApiParam(value = "User or group id") @QueryParam("member") String member,
            @ApiParam(value = "Boolean to accept either only complete (false) or partial (true) results", defaultValue = "false") @QueryParam("silent") boolean silent) throws WebServiceException {

        try {
            List<String> idList = getIdList(studiesStr);
            return createOkResponse(studyManager.getAcls(idList, member, silent, sessionId));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }
    }

    // Temporal method used by deprecated methods. This will be removed at some point.
    private Study.StudyAclParams getAclParams(
            @ApiParam(value = "Comma separated list of permissions to add", required = false) @QueryParam("add") String addPermissions,
            @ApiParam(value = "Comma separated list of permissions to remove", required = false) @QueryParam("remove") String removePermissions,
            @ApiParam(value = "Comma separated list of permissions to set", required = false) @QueryParam("set") String setPermissions,
            @ApiParam(value = "Template of permissions (only to create)", required = false) @QueryParam("template") String template)
            throws CatalogException {
        int count = 0;
        count += StringUtils.isNotEmpty(setPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(addPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(removePermissions) ? 1 : 0;
        if (count > 1) {
            throw new CatalogException("Only one of add, remove or set parameters are allowed.");
        } else if (count == 0) {
            if (StringUtils.isNotEmpty(template)) {
                throw new CatalogException("One of add, remove or set parameters is expected.");
            }
        }

        String permissions = null;
        AclParams.Action action = null;
        if (StringUtils.isNotEmpty(addPermissions) || StringUtils.isNotEmpty(template)) {
            permissions = addPermissions;
            action = AclParams.Action.ADD;
        }
        if (StringUtils.isNotEmpty(setPermissions)) {
            permissions = setPermissions;
            action = AclParams.Action.SET;
        }
        if (StringUtils.isNotEmpty(removePermissions)) {
            permissions = removePermissions;
            action = AclParams.Action.REMOVE;
        }
        return new Study.StudyAclParams(permissions, action, template);
    }

    public static class MemberAclUpdateOld {
        public String add;
        public String set;
        public String remove;
    }

    @POST
    @Path("/{study}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the user or group [DEPRECATED]", position = 21, hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. A different entrypoint /acl/{members}/update has been added "
                    + "to also support changing permissions using queries.")
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
            @PathParam("study") String studyStr,
            @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value = "JSON containing one of the keys 'add', 'set' or 'remove'", required = true) MemberAclUpdateOld params) {
        try {
            isSingleId(studyStr);
            isSingleId(memberId);
            Study.StudyAclParams aclParams = getAclParams(params.add, params.remove, params.set, null);
            List<String> idList = getIdList(studyStr);
            return createOkResponse(studyManager.updateAcl(idList, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = "JSON containing the parameters to modify ACLs. 'template' could be either 'admin', 'analyst' or 'view_only'",
                    required = true) StudyAcl params) {
        try {
            Study.StudyAclParams aclParams = new Study.StudyAclParams(params.getPermissions(), params.getAction(), params.template);
            List<String> idList = getIdList(params.study);
            return createOkResponse(studyManager.updateAcl(idList, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class StudyAcl extends AclParams {
        public String study;
        public String template;
    }

    public static class StudyParams {
        public String name;
        public String alias;
        public Study.Type type;
        public String description;

        public Map<String, Object> stats;
        public Map<String, Object> attributes;

        public boolean checkValidCreateParams() {
            if (StringUtils.isEmpty(name) || StringUtils.isEmpty(alias)) {
                return false;
            }
            return true;
        }
    }

    public static class GroupCreateParams {
        @JsonProperty(required = true)
        public String name;
        @Deprecated
        public String groupId;
        public String users;
    }

}
