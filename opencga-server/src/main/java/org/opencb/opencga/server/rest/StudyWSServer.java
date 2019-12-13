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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.server.WebServiceException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;


@Path("/{apiVersion}/studies")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Studies", description = "Methods for working with 'studies' endpoint")
public class StudyWSServer extends OpenCGAWSServer {

    private StudyManager studyManager;

    public StudyWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        studyManager = catalogManager.getStudyManager();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new study", response = Study.class)
    public Response createStudyPOST(
            @Deprecated @ApiParam(value = "Deprecated: Project id") @QueryParam("projectId") String projectId,
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = ParamConstants.STUDY_PARAM, required = true) StudyCreateParams study) {
        try {
            study = ObjectUtils.defaultIfNull(study, new StudyCreateParams());

            if (StringUtils.isNotEmpty(projectId) && StringUtils.isEmpty(project)) {
                project = projectId;
            }

            String studyId = StringUtils.isEmpty(study.id) ? study.alias : study.id;
            return createOkResponse(catalogManager.getStudyManager().create(project, studyId, study.alias, study.name, study.type, null,
                    study.description, null, null, null, null, null, study.stats, study.attributes, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search studies", response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query")
    })
    public Response getAllStudies(
            @Deprecated @ApiParam(value = "Project id or alias", hidden = true) @QueryParam("projectId") String projectId,
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION, required = true) @QueryParam(ParamConstants.PROJECT_PARAM) String projectStr,
            @ApiParam(value = ParamConstants.STUDY_NAME_DESCRIPTION) @QueryParam("name") String name,
            @ApiParam(value = ParamConstants.STUDY_ID_DESCRIPTION) @QueryParam("id") String id,
            @ApiParam(value = ParamConstants.STUDY_ALIAS_DESCRIPTION) @QueryParam("alias") String alias,
            @ApiParam(value = ParamConstants.STUDY_FQN_DESCRIPTION) @QueryParam("fqn") String fqn,
            @ApiParam(value = "Type of study: CASE_CONTROL, CASE_SET...") @QueryParam("type") String type,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION)
            @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION)
            @QueryParam("modificationDate") String modificationDate,
//            @ApiParam(value = "Boolean to retrieve deleted studies", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Attributes") @QueryParam("attributes") String attributes,
            @ApiParam(value = "Numerical attributes") @QueryParam("nattributes") String nattributes,
            @ApiParam(value = "Boolean attributes") @QueryParam("battributes") boolean battributes,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            if (StringUtils.isNotEmpty(projectId) && StringUtils.isEmpty(projectStr)) {
                projectStr = projectId;
                query.remove(StudyDBAdaptor.QueryParams.PROJECT_ID.key());
            }
            query.remove(ParamConstants.PROJECT_PARAM);

            DataResult<Study> queryResult = catalogManager.getStudyManager().get(projectStr, query, queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some study attributes")
    public Response updateByPost(@ApiParam(value = ParamConstants.STUDY_DESCRIPTION,
            required = true) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
                                 @ApiParam(value = "JSON containing the params to be updated.", required = true) StudyParams updateParams) {
        try {
            ObjectUtils.defaultIfNull(updateParams, new StudyParams());
            DataResult queryResult = catalogManager.getStudyManager().update(studyStr,
                    new ObjectMap(getUpdateObjectMapper().writeValueAsString(updateParams)), null, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studies}/info")
    @ApiOperation(value = "Fetch study information", response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response info(
            @ApiParam(value = ParamConstants.STUDIES_DESCRIPTION,
                    required = true) @PathParam(ParamConstants.STUDIES_PARAM) String studies) {
        try {
            List<String> idList = getIdList(studies);
            return createOkResponse(studyManager.get(idList, queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studies}/summary")
    @ApiOperation(value = "Fetch study information plus some basic stats", hidden = true,
            notes = "Fetch study information plus some basic stats such as the number of files, samples, cohorts...")
    public Response summary(@ApiParam(value = ParamConstants.STUDIES_DESCRIPTION, required = true)
                            @PathParam(ParamConstants.STUDIES_PARAM) String studies,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION,
                                    defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(studies);
            return createOkResponse(studyManager.getSummary(idList, queryOptions, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/files")
    @ApiOperation(value = "Fetch files in study [DEPRECATED]", response = File[].class, hidden = true,
            notes = "The use of this webservice is discouraged. The whole functionality is replicated in the files/search endpoint.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION,
                    dataType = "boolean", paramType = "query")
    })
    public Response getAllFiles(@ApiParam(value = ParamConstants.STUDY_DESCRIPTION,
            required = true) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
                                @ApiParam(value = ParamConstants.FILE_ID_DESCRIPTION) @QueryParam("id") String id,
                                @ApiParam(value = ParamConstants.FILE_NAME_DESCRIPTION) @QueryParam("name") String name,
                                @ApiParam(value = ParamConstants.FILE_PATH_DESCRIPTION) @QueryParam("path") String path,
                                @ApiParam(value = ParamConstants.FILE_TYPE_DESCRIPTION) @QueryParam("type") String type,
                                @ApiParam(value = ParamConstants.FILE_BIOFORMAT_DESCRIPTION) @QueryParam("bioformat") String bioformat,
                                @ApiParam(value = ParamConstants.FILE_FORMAT_DESCRIPTION) @QueryParam("format") String formats,
                                @ApiParam(value = ParamConstants.FILE_STATUS_DESCRIPTION) @QueryParam("status") File.FileStatus status,
                                @ApiParam(value = ParamConstants.FILE_DIRECTORY_DESCRIPTION) @QueryParam("directory") String directory,
                                @ApiParam(value = ParamConstants.FILE_CREATION_DATA_DESCRIPTION) @QueryParam("creationDate") String creationDate,
                                @ApiParam(value = ParamConstants.FILE_MODIFICATION_DATE_DESCRIPTION) @QueryParam("modificationDate") String modificationDate,
                                @ApiParam(value = ParamConstants.FILE_DESCRIPTION_DESCRIPTION) @QueryParam("description") String description,
                                @ApiParam(value = ParamConstants.FILE_SIZE_DESCRIPTION) @QueryParam("size") Long size,
                                @ApiParam(value = "List of sample ids associated with the files") @QueryParam("sampleIds") String sampleIds,
                                @ApiParam(value = "Job id that generated the file") @QueryParam("jobId") String jobId,
                                @ApiParam(value = "Attributes") @QueryParam("attributes") String attributes,
                                @ApiParam(value = "Numerical attributes") @QueryParam("nattributes") String nattributes) {
        try {
            ParamUtils.checkIsSingleID(studyStr);
            DataResult queryResult = catalogManager.getFileManager().search(studyStr, query, queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/samples")
    @ApiOperation(value = "Fetch samples in study [DEPRECATED]", response = Sample[].class, hidden = true,
            notes = "The use of this webservice is discouraged. The whole functionality is replicated in the samples/search endpoint.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION,
                    dataType = "boolean", paramType = "query")
    })
    public Response getAllSamples(@ApiParam(value = ParamConstants.STUDY_DESCRIPTION,
            required = true) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
                                  @ApiParam(value = ParamConstants.SAMPLE_NAME_DESCRIPTION) @QueryParam("name") String name,
                                  @Deprecated @ApiParam(value = "source", hidden = true) @QueryParam("source") String source,
                                  @ApiParam(value = "individualId") @QueryParam("individualId") String individualId,
                                  @ApiParam(value = "annotationSetName") @QueryParam("annotationSetName") String annotationSetName,
                                  @ApiParam(value = "variableSetId", hidden = true) @QueryParam("variableSetId") String variableSetId,
                                  @ApiParam(value = "variableSet") @QueryParam("variableSet") String variableSet,
                                  @ApiParam(value = "annotation") @QueryParam("annotation") String annotation) {
        try {
            DataResult queryResult = catalogManager.getSampleManager().search(studyStr, query, queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/jobs")
    @ApiOperation(value = "Return filtered jobs in study [DEPRECATED]", position = 9, hidden = true,
            notes = "The use of this webservice is discouraged. The whole functionality is replicated in the jobs/search endpoint.",
            response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response getAllJobs(@ApiParam(value = ParamConstants.STUDY_DESCRIPTION,
            required = true) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
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
            return createOkResponse(catalogManager.getJobManager().search(studyStr, new Query(), null, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/scanFiles")
    @ApiOperation(value = "Scan the study folder to find untracked or missing files", position = 12)
    public Response scanFiles(@ApiParam(value = ParamConstants.STUDY_DESCRIPTION,
            required = true) @PathParam(ParamConstants.STUDY_PARAM) String studyStr) {
        try {
            ParamUtils.checkIsSingleID(studyStr);
            Study study = catalogManager.getStudyManager().get(studyStr, null, token).first();
            FileScanner fileScanner = new FileScanner(catalogManager);

            /** First, run CheckStudyFiles to find new missing files **/
            List<File> checkStudyFiles = fileScanner.checkStudyFiles(study, false, token);
            List<File> found = checkStudyFiles
                    .stream()
                    .filter(f -> f.getStatus().getName().equals(File.FileStatus.READY))
                    .collect(Collectors.toList());

            /** Get untracked files **/
            Map<String, URI> untrackedFiles = fileScanner.untrackedFiles(study, token);

            /** Get missing files **/
            List<File> missingFiles = catalogManager.getFileManager().search(studyStr, query.append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.MISSING), queryOptions, token).getResults();

            ObjectMap fileStatus = new ObjectMap("untracked", untrackedFiles).append("found", found).append("missing", missingFiles);

            return createOkResponse(new DataResult<>(0, Collections.emptyList(), 1, Collections.singletonList(fileStatus), 1));
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
    public Response resyncFiles(@ApiParam(value = ParamConstants.STUDY_DESCRIPTION,
            required = true) @PathParam(ParamConstants.STUDY_PARAM) String studyStr) {
        try {
            ParamUtils.checkIsSingleID(studyStr);
            Study study = catalogManager.getStudyManager().get(studyStr, null, token).first();
            FileScanner fileScanner = new FileScanner(catalogManager);

            /* Resync files */
            List<File> resyncFiles = fileScanner.reSync(study, false, token);

            return createOkResponse(new DataResult<>(0, Collections.emptyList(), 1, Arrays.asList(resyncFiles), 1));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/groups")
    @ApiOperation(value = "Return the groups present in the study", response = Group[].class)
    public Response getGroups(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION, required = true)
                @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Group id. If provided, it will only fetch information for the provided group.") @QueryParam("id") String groupId,
            @ApiParam(value = "[DEPRECATED] Replaced by id.") @QueryParam("name") String groupName,
            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION,
                    defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            if (StringUtils.isNotEmpty(groupName)) {
                groupId = groupName;
            }
            return createOkResponse(catalogManager.getStudyManager().getGroup(studyStr, groupId, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/update")
    @ApiOperation(value = "Add or remove a group")
    public Response updateGroupPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Action to be performed: ADD or REMOVE a group", defaultValue = "ADD")
            @QueryParam("action") ParamUtils.BasicUpdateAction action,
            @ApiParam(value = "JSON containing the parameters", required = true) GroupCreateParams params) {
        try {
            if (action == null) {
                action = ParamUtils.BasicUpdateAction.ADD;
            }
            DataResult group;
            if (action == ParamUtils.BasicUpdateAction.ADD) {
                // TODO: Remove if condition in v2.0
                if (StringUtils.isEmpty(params.id)) {
                    params.id = params.name;
                }

                group = catalogManager.getStudyManager().createGroup(studyStr, params.id, params.name, params.users, token);
            } else {
                group = catalogManager.getStudyManager().deleteGroup(studyStr, params.id, token);
            }
            return createOkResponse(group);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/{group}/users/update")
    @ApiOperation(value = "Add, set or remove users from an existing group")
    public Response updateUsersFromGroupPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Group name") @PathParam("group") String groupId,
            @ApiParam(value = "Action to be performed: ADD, SET or REMOVE users to/from a group", defaultValue = "ADD")
            @QueryParam("action") GroupParams.Action action,
            @ApiParam(value = "JSON containing the parameters", required = true) Users users) {
        try {
            if (action == null) {
                action = GroupParams.Action.ADD;
            }

            GroupParams params = new GroupParams(users.users, action);
            return createOkResponse(catalogManager.getStudyManager().updateGroup(studyStr, groupId, params, token));

        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class Users {
        public String users;
    }


    @POST
    @Path("/{study}/groups/create")
    @ApiOperation(value = "Create a group [DEPRECATED]", position = 14)
    public Response createGroupPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION, required = true)
            @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "JSON containing the parameters", required = true) GroupCreateParams params) {
        params = ObjectUtils.defaultIfNull(params, new GroupCreateParams());

        if (StringUtils.isEmpty(params.name)) {
            params.name = params.id;
        }

        if (StringUtils.isEmpty(params.id)) {
            params.id = params.name;
        }
        if (StringUtils.isEmpty(params.id)) {
            return createErrorResponse(new CatalogException("Group 'id' key missing."));
        }

        List<String> userList = StringUtils.isEmpty(params.users) ? Collections.emptyList() : Arrays.asList(params.users.split(","));

        try {
            DataResult group = catalogManager.getStudyManager().createGroup(studyStr, new Group(params.id, params.name, userList, null),
                    token);
            return createOkResponse(group);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/{group}/update")
    @ApiOperation(value = "Updates the members of the group [DEPRECATED]", hidden = true)
    public Response addMembersToGroupPOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION, required = true)
            @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Group name", required = true) @PathParam("group") String groupId,
            @ApiParam(value = "JSON containing the action to be performed", required = true) GroupParams params) {
        try {
            ObjectUtils.defaultIfNull(params, new GroupParams());

            ParamUtils.checkIsSingleID(studyStr);
            return createOkResponse(
                    catalogManager.getStudyManager().updateGroup(studyStr, groupId, params, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/members/update")
    @ApiOperation(value = "Add/Remove users with access to study [DEPRECATED]", hidden = true)
    public Response registerUsersToStudy(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION, required = true)
            @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "JSON containing the action to be performed", required = true) MemberParams params) {
        try {
            ObjectUtils.defaultIfNull(params, new MemberParams());

            ParamUtils.checkIsSingleID(studyStr);
            return createOkResponse(
                    catalogManager.getStudyManager().updateGroup(studyStr, "@members", params.toGroupParams(), token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/admins/update")
    @ApiOperation(value = "Add/Remove users with administrative permissions to the study. [DEPRECATED]", hidden = true,
            notes = "Only the owner of the study will be able to run this webservice")
    public Response registerAdministrativeUsersToStudy(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION, required = true) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "JSON containing the action to be performed", required = true) MemberParams params) {
        try {
            ObjectUtils.defaultIfNull(params, new MemberParams());

            ParamUtils.checkIsSingleID(studyStr);
            return createOkResponse(
                    catalogManager.getStudyManager().updateGroup(studyStr, "@admins", params.toGroupParams(), token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/groups/{group}/delete")
    @ApiOperation(value = "Delete the group [DEPRECATED]", position = 17, hidden = true, notes = "Delete the group selected from the study.")
    public Response deleteMembersFromGroup(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION, required = true) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Group name", required = true) @PathParam("group") String groupId) {
        try {
            ParamUtils.checkIsSingleID(studyStr);
            return createOkResponse(catalogManager.getStudyManager().deleteGroup(studyStr, groupId, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/permissionRules")
    @ApiOperation(value = "Fetch permission rules")
    public Response getPermissionRules(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION, required = true) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Entity where the permission rules should be applied to", required = true) @QueryParam("entity") Study.Entity
                    entity) {
        try {
            ParamUtils.checkIsSingleID(studyStr);
            return createOkResponse(catalogManager.getStudyManager().getPermissionRules(studyStr, entity, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/permissionRules/update")
    @ApiOperation(value = "Add or remove a permission rule")
    public Response updatePermissionRules(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Entity where the permission rules should be applied to", required = true) @QueryParam("entity")
                    Study.Entity entity,
            @ApiParam(value = "Action to be performed: ADD to add a new permission rule; REMOVE to remove all permissions assigned by an "
                    + "existing permission rule (even if it overlaps any manual permission); REVERT to remove all permissions assigned by"
                    + " an existing permission rule (keep manual overlaps); NONE to remove an existing permission rule without removing "
                    + "any permissions that could have been assigned already by the permission rule.", defaultValue = "ADD")
            @QueryParam("action") PermissionRuleAction action,
            @ApiParam(value = "JSON containing the permission rule to be created or removed.", required = true) PermissionRule params) {
        try {
            if (action == null) {
                action = PermissionRuleAction.ADD;
            }
            if (action == PermissionRuleAction.ADD) {
                return createOkResponse(catalogManager.getStudyManager().createPermissionRule(studyStr, entity, params, token));
            } else {
                PermissionRule.DeleteAction deleteAction;
                switch (action) {
                    case REVERT:
                        deleteAction = PermissionRule.DeleteAction.REVERT;
                        break;
                    case NONE:
                        deleteAction = PermissionRule.DeleteAction.NONE;
                        break;
                    case REMOVE:
                    default:
                        deleteAction = PermissionRule.DeleteAction.REMOVE;
                        break;
                }
                catalogManager.getStudyManager().markDeletedPermissionRule(studyStr, entity, params.getId(), deleteAction, token);
                return createOkResponse(DataResult.empty());
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public enum PermissionRuleAction {
        ADD,
        REMOVE,
        REVERT,
        NONE
    }

    @GET
    @Path("/{studies}/acl")
    @ApiOperation(value = "Return the acl of the study. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(
            @ApiParam(value = ParamConstants.STUDIES_DESCRIPTION, required = true) @PathParam(ParamConstants.STUDIES_PARAM) String studiesStr,
            @ApiParam(value = "User or group id") @QueryParam("member") String member,
            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION, defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {

        return run(() -> {
            List<String> idList = getIdList(studiesStr);
            return studyManager.getAcls(idList, member, silent, token);
        });
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
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION, required = true)
            @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value = "JSON containing one of the keys 'add', 'set' or 'remove'", required = true) MemberAclUpdateOld params) {
        try {
            ParamUtils.checkIsSingleID(studyStr);
            ParamUtils.checkIsSingleID(memberId);
            Study.StudyAclParams aclParams = getAclParams(params.add, params.remove, params.set, null);
            List<String> idList = getIdList(studyStr);
            return createOkResponse(studyManager.updateAcl(idList, memberId, aclParams, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member")
    public Response updateAcl(
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = "JSON containing the parameters to modify ACLs. 'template' could be either 'admin', 'analyst' or 'view_only'",
                    required = true) StudyAcl params) {
        try {
            ObjectUtils.defaultIfNull(params, new StudyAcl());

            Study.StudyAclParams aclParams = new Study.StudyAclParams(params.getPermissions(), params.getAction(), params.template);
            List<String> idList = getIdList(params.study, false);
            return createOkResponse(studyManager.updateAcl(idList, memberId, aclParams, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/variableSets")
    @ApiOperation(value = "Fetch variableSets from a study")
    public Response getVariableSets(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION, required = true)
            @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Id of the variableSet to be retrieved. If no id is passed, it will show all the variableSets of the study")
            @QueryParam("id") String variableSetId) {
        try {
            DataResult<VariableSet> queryResult;
            if (StringUtils.isEmpty(variableSetId)) {
                QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.VARIABLE_SET.key());
                DataResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyStr, options, token);

                if (studyQueryResult.getNumResults() == 1) {
                    queryResult = new DataResult<>(studyQueryResult.getTime(), studyQueryResult.getEvents(),
                            studyQueryResult.first().getVariableSets().size(), studyQueryResult.first().getVariableSets(),
                            studyQueryResult.first().getVariableSets().size());
                } else {
                    queryResult = DataResult.empty();
                }
            } else {
                queryResult = catalogManager.getStudyManager().getVariableSet(studyStr, variableSetId, queryOptions, token);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studies}/stats")
    @ApiOperation(value = "Fetch catalog study stats", position = 15, hidden = true, response = QueryResponse.class)
    public Response getStats(
            @ApiParam(value = "Comma separated list of studies [[user@]project:]study up to a maximum of 100", required = true)
            @PathParam(ParamConstants.STUDIES_PARAM) String studies,
            @ApiParam(value = "Calculate default stats", defaultValue = "true") @QueryParam("default") Boolean defaultStats,
            @ApiParam(value = "List of file fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("fileFields") String fileFields,
            @ApiParam(value = "List of individual fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("individualFields") String individualFields,
            @ApiParam(value = "List of family fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("familyFields") String familyFields,
            @ApiParam(value = "List of sample fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("sampleFields") String sampleFields,
            @ApiParam(value = "List of cohort fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("cohortFields") String cohortFields) {
        try {
            if (defaultStats == null) {
                defaultStats = true;
            }
            List<String> idList = getIdList(studies);
            Map<String, Object> result = new HashMap<>();
            for (String study : idList) {
                result.put(study, catalogManager.getStudyManager().facet(study, fileFields, sampleFields, individualFields, cohortFields,
                        familyFields, defaultStats, token));
            }
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studies}/aggregationStats")
    @ApiOperation(value = "Fetch catalog study stats", position = 15, response = QueryResponse.class)
    public Response getAggregationStats(
            @ApiParam(value = "Comma separated list of studies [[user@]project:]study up to a maximum of 100", required = true)
            @PathParam(ParamConstants.STUDIES_PARAM) String studies,
            @ApiParam(value = "Calculate default stats", defaultValue = "true") @QueryParam("default") Boolean defaultStats,
            @ApiParam(value = "List of file fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("fileFields") String fileFields,
            @ApiParam(value = "List of individual fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("individualFields") String individualFields,
            @ApiParam(value = "List of family fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("familyFields") String familyFields,
            @ApiParam(value = "List of sample fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("sampleFields") String sampleFields,
            @ApiParam(value = "List of cohort fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: "
                    + "studies>>biotype;type") @QueryParam("cohortFields") String cohortFields) {
        try {
            if (defaultStats == null) {
                defaultStats = true;
            }
            List<String> idList = getIdList(studies);
            Map<String, Object> result = new HashMap<>();
            for (String study : idList) {
                result.put(study, catalogManager.getStudyManager().facet(study, fileFields, sampleFields, individualFields, cohortFields,
                        familyFields, defaultStats, token));
            }
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/variableSets/update")
    @ApiOperation(value = "Add or remove a variableSet")
    public Response createOrRemoveVariableSets(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Action to be performed: ADD or REMOVE a variableSet", defaultValue = "ADD")
            @QueryParam("action") ParamUtils.BasicUpdateAction action,
            @ApiParam(value = "JSON containing the VariableSet to be created or removed.", required = true) VariableSetParameters params) {
        try {
            if (action == null) {
                action = ParamUtils.BasicUpdateAction.ADD;
            }

            DataResult<VariableSet> queryResult;
            if (action == ParamUtils.BasicUpdateAction.ADD) {
                // Fix variable set params to support 1.3.x
                // TODO: Remove in version 2.0.0
                params.id = StringUtils.isNotEmpty(params.id) ? params.id : params.name;
                for (Variable variable : params.variables) {
                    fixVariable(variable);
                }

                queryResult = catalogManager.getStudyManager().createVariableSet(studyStr, params.id, params.name, params.unique,
                        params.confidential, params.description, null, params.variables, getAnnotableDataModelsList(params.entities),
                        token);
            } else {
                queryResult = catalogManager.getStudyManager().deleteVariableSet(studyStr, params.id, token);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/variableSets/{variableSet}/update")
    @ApiOperation(value = "Update fields of an existing VariableSet [PENDING]", hidden = true)
    public Response updateVariableSets(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "VariableSet id of the VariableSet to be updated") @PathParam("variableSet") String variableSetId,
            @ApiParam(value = "JSON containing the fields of the VariableSet to update.", required = true) UpdateableVariableSetParameters params) {
        return createErrorResponse(new NotImplementedException("Pending of implementation"));
    }

    @POST
    @Path("/{study}/variableSets/{variableSet}/variables/update")
    @ApiOperation(value = "Add or remove variables to a VariableSet")
    public Response updateVariablesFromVariableSet(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "VariableSet id of the VariableSet to be updated") @PathParam("variableSet") String variableSetId,
            @ApiParam(value = "Action to be performed: ADD or REMOVE a variable", defaultValue = "ADD")
            @QueryParam("action") ParamUtils.BasicUpdateAction action,
            @ApiParam(value = "JSON containing the variable to be added or removed. For removing, only the variable id will be needed.",
                    required = true) Variable variable) {
        try {
            if (action == null) {
                action = ParamUtils.BasicUpdateAction.ADD;
            }

            DataResult<VariableSet> queryResult;
            if (action == ParamUtils.BasicUpdateAction.ADD) {
                queryResult = catalogManager.getStudyManager().addFieldToVariableSet(studyStr, variableSetId, variable, token);
            } else {
                queryResult = catalogManager.getStudyManager().removeFieldFromVariableSet(studyStr, variableSetId, variable.getId(),
                        token);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private List<VariableSet.AnnotableDataModels> getAnnotableDataModelsList(List<String> entityStringList) {
        List<VariableSet.AnnotableDataModels> entities = new ArrayList<>();
        if (ListUtils.isEmpty(entityStringList)) {
            return entities;
        }

        for (String entity : entityStringList) {
            entities.add(VariableSet.AnnotableDataModels.valueOf(entity.toUpperCase()));
        }
        return entities;
    }

    private static class VariableSetParameters {
        public Boolean unique;
        public Boolean confidential;
        public String id;
        public String name;
        public String description;
        public List<String> entities;
        public List<Variable> variables;
    }

    private static class UpdateableVariableSetParameters {

    }

    private void fixVariable(Variable variable) {
        variable.setId(StringUtils.isNotEmpty(variable.getId()) ? variable.getId() : variable.getName());
        if (variable.getVariableSet() != null && variable.getVariableSet().size() > 0) {
            for (Variable variable1 : variable.getVariableSet()) {
                fixVariable(variable1);
            }
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

    public static class StudyCreateParams extends StudyParams {
        public String id;
    }

    public static class GroupCreateParams {
        @JsonProperty(required = true)
        public String id;
        public String name;
        public String users;
    }

}
