/*
 * Copyright 2015-2020 OpenCB
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

import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.tools.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.analysis.file.*;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.TsvAnnotationParams;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.util.*;

import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;

@Path("/{apiVersion}/files")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Files", description = "Methods for working with 'files' endpoint")
public class FileWSServer extends OpenCGAWSServer {

    private FileManager fileManager;

    public FileWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        fileManager = catalogManager.getFileManager();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create file or folder", response = File.class,
            notes = "Creates a file with some content in it or a folder <br>"
                    + "<ul>"
                    + "<il><b>path</b>: Mandatory parameter. Whole path containing the file or folder name to be created</il><br>"
                    + "<il><b>type</b>: Mandatory parameter. Type of file being created: FILE or DIRECTORY</il><br>"
                    + "<il><b>content</b>: Only applicable if type is FILE. Content of the file in UTF-8 format. If file format is IMAGE, "
                    + "a base64 content is expected.</il><br>"
                    + "<il><b>format</b>: File format.</il><br>"
                    + "<ul>"
    )
    public Response createFilePOST(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.FILE_PARENTS_DESCRIPTION) @QueryParam(ParamConstants.FILE_PARENTS_PARAM) boolean parents,
            @ApiParam(name = "body", value = "File parameters", required = true) FileCreateParams params) {
        try {
            DataResult<File> file = fileManager.create(studyStr, params, parents, token);
            return createOkResponse(file);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/fetch")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Download an external file to catalog and register it", response = Job.class)
    public Response downloadAndRegister(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobId,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(name = "body", value = "Fetch parameters", required = true) FileFetch fetchParams) {
        return submitJob(FetchAndRegisterTask.ID, studyStr, fetchParams, jobId, jobDescription, dependsOn, jobTags);
    }

    @GET
    @Path("/{files}/info")
    @ApiOperation(value = "File info", response = File.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response info(
            @ApiParam(value = ParamConstants.FILES_DESCRIPTION) @PathParam(value = "files") String fileStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Boolean to retrieve deleted files", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("files");

            List<String> idList = getIdList(fileStr.replaceAll(":", "/"));
            DataResult<File> fileQueryResult = fileManager.get(studyStr, idList, new Query("deleted", deleted), queryOptions, true, token);
            return createOkResponse(fileQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/image")
    @ApiOperation(value = "Obtain the base64 content of an image", response = FileContent.class)
    public Response base64Image(
            @ApiParam(value = ParamConstants.FILE_ID_DESCRIPTION) @PathParam(value = "file") String fileStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr) {
        try {
            OpenCGAResult<FileContent> fileQueryResult = fileManager.image(studyStr, fileStr, token);
            return createOkResponse(fileQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/bioformats")
    @ApiOperation(value = "List of accepted file bioformats", response = File.Bioformat.class)
    public Response getBioformats() {
        List<File.Bioformat> bioformats = Arrays.asList(File.Bioformat.values());
        DataResult<File.Bioformat> queryResult = new OpenCGAResult<>(0, Collections.emptyList(), bioformats.size(), bioformats,
                bioformats.size());
        return createOkResponse(queryResult);
    }

    @GET
    @Path("/formats")
    @ApiOperation(value = "List of accepted file formats", response = File.Format.class)
    public Response getFormats() {
        List<File.Format> formats = Arrays.asList(File.Format.values());
        DataResult<File.Format> queryResult = new OpenCGAResult<>(0, Collections.emptyList(), formats.size(), formats, formats.size());
        return createOkResponse(queryResult);
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(httpMethod = "POST", value = "Resource to upload a file by chunks", response = File.class)
    public Response upload(
            @ApiParam(value = "File to upload") @FormDataParam("file") InputStream fileInputStream,
            @FormDataParam("file") FormDataContentDisposition fileMetaData,

            @ApiParam(value = "File name to overwrite the input fileName") @FormDataParam("filename") String filename,
            @ApiParam(value = "File format") @DefaultValue("") @FormDataParam("fileFormat") File.Format fileFormat,
            @ApiParam(value = "File bioformat") @DefaultValue("") @FormDataParam("bioformat") File.Bioformat bioformat,
            @ApiParam(value = "Expected MD5 file checksum") @DefaultValue("") @FormDataParam("checksum") String expectedChecksum,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @FormDataParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Path within catalog where the file will be located (default: root folder)") @DefaultValue("") @FormDataParam("relativeFilePath") String relativeFilePath,
            @ApiParam(value = "description") @DefaultValue("") @FormDataParam("description")
                    String description,
            @ApiParam(value = "Create the parent directories if they do not exist", type = "form") @DefaultValue("true") @FormDataParam(
                    "parents") boolean parents) {
        if (StringUtils.isNotEmpty(relativeFilePath)) {
            if (relativeFilePath.equals(".")) {
                relativeFilePath = "";
            } else if (!relativeFilePath.endsWith("/")) {
                relativeFilePath = relativeFilePath + "/";
            }
        }

        if (relativeFilePath.startsWith("/")) {
            return createErrorResponse(new CatalogException("The path cannot be absolute"));
        }

        if (fileInputStream == null) {
            return createErrorResponse("Upload file", "No file or chunk found");
        }

        try {
            if (filename == null) {
                filename = fileMetaData.getFileName();
            }
            long expectedSize = fileMetaData.getSize();

            File file = new File()
                    .setName(filename)
                    .setPath(relativeFilePath + filename)
                    .setFormat(fileFormat)
                    .setBioformat(bioformat);
            return createOkResponse(fileManager.upload(studyStr, fileInputStream, file, false, parents, true,
                    expectedChecksum, expectedSize, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/download")
    @ApiOperation(value = "Download file", response = DataInputStream.class,
            notes = "The usage of /{file}/download webservice through Swagger is <b>discouraged</b>. Please, don't click the 'Try it "
                    + "out' button here as it may hang this web page. Instead, build the final URL in a different tab.<br>"
                    + "An special <b>DOWNLOAD</b> permission is needed to download files from OpenCGA.")
    public Response download(@ApiParam(value = "File id, name or path. Paths must be separated by : instead of /") @PathParam("file") String fileIdStr,
                             @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                             @QueryParam(ParamConstants.STUDY_PARAM) String studyStr) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            DataInputStream stream = catalogManager.getFileManager().download(studyStr, fileIdStr, -1, -1, token);
            return createOkResponse(stream, MediaType.APPLICATION_OCTET_STREAM_TYPE, fileIdStr);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/head")
    @ApiOperation(value = "Show the first lines of a file (up to a limit)", response = FileContent.class)
    public Response head(
            @ApiParam(value = "File uuid, id, or name.") @PathParam("file") String fileIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Starting byte from which the file will be read") @QueryParam("offset") long offset,
            @ApiParam(value = ParamConstants.MAXIMUM_LINES_CONTENT_DESCRIPTION, defaultValue = "20") @QueryParam("lines") Integer lines) {
        try {
            if (lines == null) {
                lines = 20;
            }
            ParamUtils.checkIsSingleID(fileIdStr);
            return createOkResponse(catalogManager.getFileManager().head(studyStr, fileIdStr, offset, lines, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/tail")
    @ApiOperation(value = "Show the last lines of a file (up to a limit)", response = FileContent.class)
    public Response tail(
            @ApiParam(value = "File uuid, id, or name.") @PathParam("file") String fileIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.MAXIMUM_LINES_CONTENT_DESCRIPTION, defaultValue = "20") @QueryParam("lines") Integer lines) {
        try {
            if (lines == null) {
                lines = 20;
            }
            ParamUtils.checkIsSingleID(fileIdStr);
            return createOkResponse(catalogManager.getFileManager().tail(studyStr, fileIdStr, lines, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/grep")
    @ApiOperation(value = "Filter lines of the file containing the pattern", response = FileContent.class)
    public Response downloadGrep(
            @ApiParam(value = "File uuid, id, or name.") @PathParam("file") String fileIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "String pattern") @QueryParam("pattern") String pattern,
            @ApiParam(value = "Flag to perform a case insensitive search") @DefaultValue("false") @QueryParam("ignoreCase")
                    boolean ignoreCase,
            @ApiParam(value = "Stop reading a file after 'n' matching lines. 0 means no limit.") @DefaultValue("10") @QueryParam(
                    "maxCount") int maxCount) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            return createOkResponse(catalogManager.getFileManager().grep(studyStr, fileIdStr, pattern, ignoreCase, maxCount, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "File search method.", response = File.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType =
                    "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType =
                    "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType =
                    "boolean", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = ParamConstants.FLATTEN_ANNOTATION_DESCRIPTION,
                    defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.FILES_ID_DESCRIPTION) @QueryParam("id") String id,
            @ApiParam(value = ParamConstants.FILES_UUID_DESCRIPTION) @QueryParam("uuid") String uuid,
            @ApiParam(value = ParamConstants.FILE_NAMES_DESCRIPTION) @QueryParam("name") String name,
            @ApiParam(value = ParamConstants.FILE_PATHS_DESCRIPTION) @QueryParam("path") String path,
            @ApiParam(value = ParamConstants.FILE_URIS_DESCRIPTION) @QueryParam("uri") String uri,
            @ApiParam(value = ParamConstants.FILE_TYPE_DESCRIPTION) @QueryParam("type") String type,
            @ApiParam(value = ParamConstants.FILE_BIOFORMAT_DESCRIPTION) @QueryParam("bioformat") String bioformat,
            @ApiParam(value = ParamConstants.FILE_FORMAT_DESCRIPTION) @QueryParam("format") String formats,
            @ApiParam(value = ParamConstants.FILE_EXTERNAL_DESCRIPTION) @QueryParam("external") Boolean external,
            @ApiParam(value = ParamConstants.STATUS_DESCRIPTION) @QueryParam(ParamConstants.STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.INTERNAL_VARIANT_INDEX_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_VARIANT_INDEX_STATUS_PARAM) String internalVariantIndexStatus,
            @ApiParam(value = ParamConstants.FILE_SOFTWARE_NAME_DESCRIPTION) @QueryParam(ParamConstants.FILE_SOFTWARE_NAME_PARAM) String softwareName,
            @ApiParam(value = ParamConstants.FILE_DIRECTORY_DESCRIPTION) @QueryParam("directory") String directory,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = ParamConstants.FILE_DESCRIPTION_DESCRIPTION) @QueryParam("description") String description,
            @ApiParam(value = ParamConstants.FILE_TAGS_DESCRIPTION) @QueryParam("tags") String tags,
            @ApiParam(value = ParamConstants.FILE_SIZE_DESCRIPTION) @QueryParam("size") String size,
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION) @QueryParam("sampleIds") String samples,
            @ApiParam(value = ParamConstants.FILE_JOB_ID_DESCRIPTION) @QueryParam("jobId") String jobId,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam("release") String release) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(fileManager.search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "File distinct method", response = Object.class)
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.FILES_ID_DESCRIPTION) @QueryParam("id") String id,
            @ApiParam(value = ParamConstants.FILES_UUID_DESCRIPTION) @QueryParam("uuid") String uuid,
            @ApiParam(value = ParamConstants.FILE_NAMES_DESCRIPTION) @QueryParam("name") String name,
            @ApiParam(value = ParamConstants.FILE_PATHS_DESCRIPTION) @QueryParam("path") String path,
            @ApiParam(value = ParamConstants.FILE_URIS_DESCRIPTION) @QueryParam("uri") String uri,
            @ApiParam(value = ParamConstants.FILE_TYPE_DESCRIPTION) @QueryParam("type") String type,
            @ApiParam(value = ParamConstants.FILE_BIOFORMAT_DESCRIPTION) @QueryParam("bioformat") String bioformat,
            @ApiParam(value = ParamConstants.FILE_FORMAT_DESCRIPTION) @QueryParam("format") String formats,
            @ApiParam(value = ParamConstants.FILE_EXTERNAL_DESCRIPTION) @QueryParam("external") Boolean external,
            @ApiParam(value = ParamConstants.STATUS_DESCRIPTION) @QueryParam(ParamConstants.STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.INTERNAL_VARIANT_INDEX_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_VARIANT_INDEX_STATUS_PARAM) String internalIndexStatus,
            @ApiParam(value = ParamConstants.FILE_SOFTWARE_NAME_DESCRIPTION) @QueryParam(ParamConstants.FILE_SOFTWARE_NAME_PARAM) String softwareName,
            @ApiParam(value = ParamConstants.FILE_DIRECTORY_DESCRIPTION) @QueryParam("directory") String directory,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = ParamConstants.FILE_DESCRIPTION_DESCRIPTION) @QueryParam("description") String description,
            @ApiParam(value = ParamConstants.FILE_TAGS_DESCRIPTION) @QueryParam("tags") String tags,
            @ApiParam(value = ParamConstants.FILE_SIZE_DESCRIPTION) @QueryParam("size") String size,
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION) @QueryParam("sampleIds") String samples,
            @ApiParam(value = ParamConstants.FILE_JOB_ID_DESCRIPTION) @QueryParam("jobId") String jobId,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
            @ApiParam(value = ParamConstants.ACL_DESCRIPTION) @QueryParam(ParamConstants.ACL_PARAM) String acl,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = ParamConstants.RELEASE_DESCRIPTION) @QueryParam("release") String release,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return createOkResponse(fileManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{folder}/list")
    @ApiOperation(value = "List all the files inside the folder", response = File.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType =
                    "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType =
                    "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType =
                    "boolean",
                    paramType = "query")
    })
    public Response list(@ApiParam(value = ParamConstants.FILE_FOLDER_DESCRIPTION) @PathParam(ParamConstants.FILE_FOLDER) String folder,
                         @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr) {
        try {
            ParamUtils.checkIsSingleID(folder);
            DataResult<File> result = catalogManager.getFileManager().getFilesFromFolder(folder, studyStr, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{folder}/tree")
    @ApiOperation(value = "Obtain a tree view of the files and folders within a folder", response = FileTree.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType =
                    "string", paramType = "query")
    })
    public Response treeView(
            @ApiParam(value = "Folder id or name. Paths must be separated by : instead of /") @DefaultValue(":") @PathParam("folder") String folderId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Maximum depth to get files from") @DefaultValue("5") @QueryParam("maxDepth") int maxDepth) {
        try {
            queryOptions.remove(QueryOptions.LIMIT);

            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("folder");
            query.remove("maxDepth");

            ParamUtils.checkIsSingleID(folderId);
            query.remove("maxDepth");

            DataResult result = fileManager.getTree(studyStr, folderId.replace(":", "/"), maxDepth, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private ObjectMap getResumeFileJSON(java.nio.file.Path folderPath) throws IOException {
        ObjectMap objectMap = new ObjectMap();

        if (Files.exists(folderPath)) {
            try (DirectoryStream<java.nio.file.Path> folderStream = Files.newDirectoryStream(folderPath, "*_partial")) {
                for (java.nio.file.Path partPath : folderStream) {
                    String[] nameSplit = partPath.getFileName().toString().split("_");
                    ObjectMap chunkInfo = new ObjectMap();
                    chunkInfo.put("size", Integer.parseInt(nameSplit[1]));
                    objectMap.put(nameSplit[0], chunkInfo);
                }
            }
        }
        return objectMap;
    }

    private List<java.nio.file.Path> getSortedChunkList(java.nio.file.Path folderPath) throws IOException {
        List<java.nio.file.Path> files = new ArrayList<>();
        try (DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(folderPath, "*_partial")) {
            for (java.nio.file.Path p : stream) {
                logger.info("adding to ArrayList: " + p.getFileName());
                files.add(p);
            }
        }
        logger.info("----ordered files length: " + files.size());
        Collections.sort(files, new Comparator<java.nio.file.Path>() {
            public int compare(java.nio.file.Path o1, java.nio.file.Path o2) {
                int id_o1 = Integer.parseInt(o1.getFileName().toString().split("_")[0]);
                int id_o2 = Integer.parseInt(o2.getFileName().toString().split("_")[0]);
                return id_o1 - id_o2;
            }
        });
        return files;
    }

//    @POST
//    @Path("/update")
//    @ApiOperation(value = "Update some file attributes", response = File.class, hidden = true)
//    public Response updateQuery(
//            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
//            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = "Comma separated list of file names") @QueryParam("name") String name,
//            @ApiParam(value = "Comma separated list of paths") @QueryParam("path") String path,
//            @ApiParam(value = ParamConstants.FILE_TYPE_DESCRIPTION) @QueryParam("type") String type,
//            @ApiParam(value = ParamConstants.FILE_BIOFORMAT_DESCRIPTION) @QueryParam("bioformat") String bioformat,
//            @ApiParam(value = ParamConstants.FILE_FORMAT_DESCRIPTION) @QueryParam("format") String formats,
//            @ApiParam(value = ParamConstants.FILE_STATUS_DESCRIPTION) @QueryParam("status") String status,
//            @ApiParam(value = ParamConstants.FILE_DIRECTORY_DESCRIPTION) @QueryParam("directory") String directory,
//            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam("creationDate") String creationDate,
//            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam("modificationDate") String modificationDate,
//            @ApiParam(value = ParamConstants.FILE_DESCRIPTION_DESCRIPTION) @QueryParam("description") String description,
//            @ApiParam(value = ParamConstants.FILE_SIZE_DESCRIPTION) @QueryParam("size") String size,
//            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION) @QueryParam("samples") String samples,
//            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,
//            @ApiParam(value = "Job id that created the file(s) or folder(s)") @QueryParam("job.id") String jobId,
//            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @QueryParam("attributes") String attributes,
//            @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)") @QueryParam("nattributes") String nattributes,
//            @ApiParam(value = "Release value") @QueryParam("release") String release,
//
//            @ApiParam(value = "Action to be performed if the array of samples is being updated.", allowableValues = "ADD,SET,REMOVE",
//            defaultValue = "ADD") @QueryParam("samplesAction") ParamUtils.BasicUpdateAction samplesAction,
//            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET,
//            REMOVE", defaultValue = "ADD") @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
//            @ApiParam(value = "Action to be performed if the array of relatedFiles is being updated.", allowableValues = "ADD,SET,
//            REMOVE", defaultValue = "ADD") @QueryParam("relatedFilesAction") ParamUtils.BasicUpdateAction relatedFilesAction,
//            @ApiParam(value = "Action to be performed if the array of tags is being updated.", allowableValues = "ADD,SET,REMOVE",
//            defaultValue = "ADD") @QueryParam("tagsAction") ParamUtils.BasicUpdateAction tagsAction,
//            @ApiParam(name = "body", value = "Parameters to modify", required = true) FileUpdateParams updateParams) {
//        try {
//            query.remove(ParamConstants.STUDY_PARAM);
//            if (samplesAction == null) {
//                samplesAction = ParamUtils.BasicUpdateAction.ADD;
//            }
//            if (annotationSetsAction == null) {
//                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
//            }
//
//            Map<String, Object> actionMap = new HashMap<>();
//            actionMap.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), samplesAction.name());
//            actionMap.put(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
//            actionMap.put(FileDBAdaptor.QueryParams.RELATED_FILES.key(), relatedFilesAction);
//            actionMap.put(FileDBAdaptor.QueryParams.TAGS.key(), tagsAction);
//            queryOptions.put(Constants.ACTIONS, actionMap);
//
//            DataResult<File> queryResult = fileManager.update(studyStr, query, updateParams, true, queryOptions, token);
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @POST
    @Path("/{files}/update")
    @ApiOperation(value = "Update some file attributes", response = File.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updatePOST(
            @ApiParam(value = "Comma separated list of file ids, names or paths. Paths must be separated by : instead of /")
            @PathParam(value = "files") String fileIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Action to be performed if the array of samples is being updated.", allowableValues = "ADD,SET,REMOVE",
                    defaultValue = "ADD") @QueryParam("sampleIdsAction") ParamUtils.BasicUpdateAction samplesAction,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", allowableValues = "ADD,SET," +
                    "REMOVE", defaultValue = "ADD") @QueryParam("annotationSetsAction") ParamUtils.BasicUpdateAction annotationSetsAction,
            @ApiParam(value = "Action to be performed if the array of relatedFiles is being updated.", allowableValues = "ADD,SET,REMOVE"
                    , defaultValue = "ADD") @QueryParam("relatedFilesAction") ParamUtils.BasicUpdateAction relatedFilesAction,
            @ApiParam(value = "Action to be performed if the array of tags is being updated.", allowableValues = "ADD,SET,REMOVE",
                    defaultValue = "ADD") @QueryParam("tagsAction") ParamUtils.BasicUpdateAction tagsAction,

            @ApiParam(name = "body", value = "Parameters to modify", required = true) FileUpdateParams updateParams) {
        try {
            if (samplesAction == null) {
                samplesAction = ParamUtils.BasicUpdateAction.ADD;
            }
            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.BasicUpdateAction.ADD;
            }

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), samplesAction.name());
            actionMap.put(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            actionMap.put(FileDBAdaptor.QueryParams.RELATED_FILES.key(), relatedFilesAction);
            actionMap.put(FileDBAdaptor.QueryParams.TAGS.key(), tagsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            List<String> fileIds = getIdList(fileIdStr);

            DataResult<File> queryResult = fileManager.update(studyStr, fileIds, updateParams, true, queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    //    @JsonIgnoreProperties({"status"})
//    public static class FileUpdateParams extends org.opencb.opencga.core.models.file.FileUpdateParams {
//    }
    @POST
    @Path("/annotationSets/load")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Load annotation sets from a TSV file", response = Job.class)
    public Response loadTsvAnnotations(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.VARIABLE_SET_DESCRIPTION, required = true) @QueryParam("variableSetId") String variableSetId,
            @ApiParam(value = "Path where the TSV file is located in OpenCGA or where it should be located.", required = true)
            @QueryParam("path") String path,
            @ApiParam(value = "Flag indicating whether to create parent directories if they don't exist (only when TSV file was not " +
                    "previously associated).")
            @DefaultValue("false") @QueryParam("parents") boolean parents,
            @ApiParam(value = "Annotation set id. If not provided, variableSetId will be used.") @QueryParam("annotationSetId") String annotationSetId,
            @ApiParam(value = ParamConstants.TSV_ANNOTATION_DESCRIPTION) TsvAnnotationParams params) {
        try {
            ObjectMap additionalParams = new ObjectMap()
                    .append("parents", parents)
                    .append("annotationSetId", annotationSetId);

            return createOkResponse(catalogManager.getFileManager().loadTsvAnnotations(studyStr, variableSetId, path, params,
                    additionalParams, FileTsvAnnotationLoader.ID, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{file}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet", response = File.class)
    public Response updateAnnotations(
            @ApiParam(value = "File id, name or path. Paths must be separated by : instead of /", required = true)
            @PathParam("file") String fileStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_ID) @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_ACTION_DESCRIPTION, allowableValues = "ADD,SET,REMOVE", defaultValue =
                    "ADD")
            @QueryParam("action") ParamUtils.CompleteUpdateAction action,
            @ApiParam(value = ParamConstants.ANNOTATION_SET_UPDATE_PARAMS_DESCRIPTION) Map<String, Object> updateParams) {
        try {
            if (action == null) {
                action = ParamUtils.CompleteUpdateAction.ADD;
            }

            return createOkResponse(catalogManager.getFileManager().updateAnnotations(studyStr, fileStr, annotationSetId,
                    updateParams, action, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @GET
//    @Path("/link")
//    @ApiOperation(value = "Link an external file into catalog.", hidden = true, response = File.class)
//    @Deprecated
//    public Response linkGet(
//            @ApiParam(value = "Uri of the file", required = true) @QueryParam("uri") String uriStr,
//            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = "Path where the external file will be allocated in catalog", required = true) @QueryParam("path") String
//            path,
//            @ApiParam(value = ParamConstants.FILE_DESCRIPTION_DESCRIPTION) @QueryParam("description") String description,
//            @ApiParam(value = "Create the parent directories if they do not exist") @DefaultValue("false") @QueryParam("parents")
//            boolean parents,
//            @ApiParam(value = "Size of the folder/file") @QueryParam("size") long size,
//            @ApiParam(value = "Checksum of something") @QueryParam("checksum") String checksum) {
//        try {
//            logger.debug("study: {}", studyStr);
//
//            path = path.replace(":", "/");
//
//            ObjectMap objectMap = new ObjectMap()
//                    .append("parents", parents)
//                    .append("description", description);
//            objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.CHECKSUM.key(), checksum);
//
//            List<DataResult<File>> queryResultList = new ArrayList<>();
//            logger.info("path: {}", path);
//            // If it is just one uri to be linked, it will return an error response if there is some kind of error.
//            URI myUri = UriUtils.createUri(uriStr);
//            queryResultList.add(catalogManager.getFileManager().link(studyStr, myUri, path, objectMap, token));
//
//            return createOkResponse(queryResultList);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @POST
    @Path("/link")
    @ApiOperation(value = "Link an external file into catalog.", response = File.class)
    public Response link(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Create the parent directories if they do not exist") @DefaultValue("false") @QueryParam("parents") boolean parents,
            @ApiParam(name = "body", value = "File parameters", required = true) FileLinkParams params) {
        try {
            return createOkResponse(catalogManager.getFileManager().link(studyStr, params, parents, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/postlink/run")
    @ApiOperation(value = "Associate non-registered samples for files with high volumes of samples.", response = Job.class)
    public Response postlink(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobId,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(name = "body", value = "File parameters", required = true) PostLinkToolParams params) {
        return submitJob(PostLinkSampleAssociation.ID, studyStr, params, jobId, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/link/run")
    @ApiOperation(value = "Link an external file into catalog asynchronously.", response = Job.class)
    public Response linkAsync(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobId,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(name = "body", value = "File parameters", required = true) FileLinkToolParams params) {
        return submitJob(FileLinkTask.ID, studyStr, params, jobId, jobDescription, dependsOn, jobTags);
    }

    @DELETE
    @Path("/{files}/unlink")
    @ApiOperation(value = "Unlink linked files and folders", response = Job.class)
    public Response unlink(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of file ids, names or paths.") @PathParam("files") String files) {
        try {
            getIdList(files);

            ObjectMap params = new ObjectMap()
                    .append("files", files)
                    .append("study", studyStr);
            OpenCGAResult<Job> result = catalogManager.getJobManager().submit(studyStr, "files-unlink", Enums.Priority.MEDIUM, params,
                    token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @Deprecated
//    @GET
//    @Path("/{fileId}/relink")
//    @ApiOperation(value = "Change file location. Provided file must be either STAGE or be an external file. [DEPRECATED]", hidden = true,
//            position = 21)
//    public Response relink(@ApiParam(value = "File Id") @PathParam("fileId") @DefaultValue("") String fileIdStr,
//                           @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
//                           @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//                           @ApiParam(value = "New URI", required = true) @QueryParam("uri") String uriStr,
//                           @ApiParam(value = "Do calculate checksum for new files") @DefaultValue("false")
//                           @QueryParam("calculateChecksum") boolean calculateChecksum) {
//        try {
//            URI uri = UriUtils.createUri(uriStr);
//            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(uri);
//
//            if (!ioManager.exists(uri)) {
//                throw new CatalogIOException("File " + uri + " does not exist");
//            }
//
//            File file = fileManager.get(studyStr, fileIdStr, null, token).first();
//
//            new FileUtils(catalogManager).link(file, calculateChecksum, uri, false, true, token);
//            file = catalogManager.getFileManager().get(file.getUid(), queryOptions, token).first();
//            file = FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, new QueryOptions(queryOptions), token,
//                    false);
//
//            return createOkResponse(new DataResult<>(0, Collections.emptyList(), 1, Collections.singletonList(file), 1));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

//    @POST
//    @Path("/scan")
//    @ApiOperation(value = "Scan the study folder to find untracked or missing files")
//    public Response scanFiles(@ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String
//    studyStr) {
//        try {
//            ParamUtils.checkIsSingleID(studyStr);
//            Study study = catalogManager.getStudyManager().get(studyStr, null, token).first();
//            FileScanner fileScanner = new FileScanner(catalogManager);
//
//            /** First, run CheckStudyFiles to find new missing files **/
//            List<File> checkStudyFiles = fileScanner.checkStudyFiles(study, false, token);
//            List<File> found = checkStudyFiles
//                    .stream()
//                    .filter(f -> f.getStatus().getName().equals(File.FileStatus.READY))
//                    .collect(Collectors.toList());
//
//            /** Get untracked files **/
//            Map<String, URI> untrackedFiles = fileScanner.untrackedFiles(study, token);
//
//            /** Get missing files **/
//            List<File> missingFiles = catalogManager.getFileManager().search(studyStr, query.append(FileDBAdaptor.QueryParams
//            .STATUS_NAME.key(), File.FileStatus.MISSING), queryOptions, token).getResults();
//
//            ObjectMap fileStatus = new ObjectMap("untracked", untrackedFiles).append("found", found).append("missing", missingFiles);
//
//            return createOkResponse(new DataResult<>(0, Collections.emptyList(), 1, Collections.singletonList(fileStatus), 1));
////            /** Print pretty **/
////            int maxFound = found.stream().map(f -> f.getPath().length()).max(Comparator.<Integer>naturalOrder()).orElse(0);
////            int maxUntracked = untrackedFiles.keySet().stream().map(String::length).max(Comparator.<Integer>naturalOrder()).orElse(0);
////            int maxMissing = missingFiles.stream().map(f -> f.getPath().length()).max(Comparator.<Integer>naturalOrder()).orElse(0);
////
////            String format = "\t%-" + Math.max(Math.max(maxMissing, maxUntracked), maxFound) + "s  -> %s\n";
////
////            if (!untrackedFiles.isEmpty()) {
////                System.out.println("UNTRACKED files");
////                untrackedFiles.forEach((s, u) -> System.out.printf(format, s, u));
////                System.out.println("\n");
////            }
////
////            if (!missingFiles.isEmpty()) {
////                System.out.println("MISSING files");
////                for (File file : missingFiles) {
////                    System.out.printf(format, file.getPath(), catalogManager.getFileUri(file));
////                }
////                System.out.println("\n");
////            }
////
////            if (!found.isEmpty()) {
////                System.out.println("FOUND files");
////                for (File file : found) {
////                    System.out.printf(format, file.getPath(), catalogManager.getFileUri(file));
////                }
////            }
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @POST
//    @Path("/resync")
//    @ApiOperation(value = "Scan the study folder to find untracked or missing files.", notes = "This method is intended to keep the "
//            + "consistency between the database and the file system. It will check all the files and folders belonging to the study and "
//            + "will keep track of those new files and/or folders found in the file system as well as update the status of those "
//            + "files/folders that are no longer available in the file system setting their status to MISSING.")
//    public Response resyncFiles(@ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String
//    studyStr) {
//        try {
//            ParamUtils.checkIsSingleID(studyStr);
//            Study study = catalogManager.getStudyManager().get(studyStr, null, token).first();
//            FileScanner fileScanner = new FileScanner(catalogManager);
//
//            /* Resync files */
//            List<File> resyncFiles = fileScanner.reSync(study, false, token);
//
//            return createOkResponse(new DataResult<>(0, Collections.emptyList(), 1, Collections.singletonList(resyncFiles), 1));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/{file}/refresh")
    @ApiOperation(value = "Refresh metadata from the selected file or folder. Return updated files.", response = File.class)
    public Response refresh(
            @ApiParam(value = "File id, name or path. Paths must be separated by : instead of /") @PathParam(value = "file") String fileIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            File file = fileManager.get(studyStr, fileIdStr, null, token).first();

            List<File> files;
            FileUtils catalogFileUtils = new FileUtils(catalogManager);
            FileMetadataReader fileMetadataReader = FileMetadataReader.get(catalogManager);
            if (file.getType() == File.Type.FILE) {
                File file1 = catalogFileUtils.checkFile(studyStr, file, false, token);
                file1 = fileMetadataReader.updateMetadataInformation(studyStr, file1, token);
                if (file == file1) {    //If the file is the same, it was not modified. Only return modified files.
                    files = Collections.emptyList();
                } else {
                    files = Collections.singletonList(file);
                }
            } else {
                List<File> result = catalogManager.getFileManager().getFilesFromFolder(fileIdStr, studyStr, null, token).getResults();
                files = new ArrayList<>(result.size());
                for (File f : result) {
                    File file1 = fileMetadataReader.updateMetadataInformation(studyStr, f, token);
                    if (f != file1) {    //Add only modified files.
                        files.add(file1);
                    }
                }
            }
            return createOkResponse(new DataResult<>(0, Collections.emptyList(), files.size(), files, files.size()));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @DELETE
//    @Path("/delete")
//    @ApiOperation(value = "Delete existing files and folders")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = Constants.SKIP_TRASH, value = "Skip trash and delete the files/folders from disk directly (CANNOT BE"
//                    + " RECOVERED)", dataType = "boolean", defaultValue = "false", paramType = "query")
//    })
//    public Response delete(
//            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
//            @QueryParam("study") String studyStr,
//            @ApiParam(value = "Comma separated list of file names") @QueryParam("name") String name,
//            @ApiParam(value = "Comma separated list of paths") @QueryParam("path") String path,
//            @ApiParam(value = "Available types (FILE, DIRECTORY)") @QueryParam("type") String type,
//            @ApiParam(value = "Comma separated bioformat values. For existing bioformats see files/bioformats")
//            @QueryParam("bioformat")String bioformat,
//            @ApiParam(value = "Comma separated format values. For existing formats see files/formats")
//            @QueryParam("format") String formats,
//            @ApiParam(value = "Status") @QueryParam("status") String status,
//            @ApiParam(value = "Directory under which we want to look for files or folders") @QueryParam("directory") String directory,
//            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
//            @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss)") @QueryParam("modificationDate") String modificationDate,
//            @ApiParam(value = "Description") @QueryParam("description") String description,
//            @ApiParam(value = "Size") @QueryParam("size") String size,
//            @ApiParam(value = "Comma separated list of sample ids or names") @QueryParam("samples") String samples,
//            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
//            @ApiParam(value = "Job id that created the file(s) or folder(s)") @QueryParam("job.id") String jobId,
//            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @QueryParam("attributes") String attributes,
//            @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)") @QueryParam("nattributes") String nattributes,
//            @ApiParam(value = "Release value") @QueryParam("release") String release) {
//        try {
//            query.remove("study");
//
//            return createOkResponse(fileManager.delete(studyStr, query, queryOptions, true, token));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @DELETE
    @Path("/{files}/delete")
    @ApiOperation(value = "Delete existing files and folders", response = Job.class)
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of file ids, names or paths.") @PathParam("files") String files,
            @ApiParam(value = "Skip trash and delete the files/folders from disk directly (CANNOT BE RECOVERED)", defaultValue = "false")
            @QueryParam(Constants.SKIP_TRASH) boolean skipTrash) {
        try {
            List<String> fileIds = getIdList(files);

            ObjectMap params = new ObjectMap()
                    .append("files", files)
                    .append("study", studyStr)
                    .append(Constants.SKIP_TRASH, skipTrash);
            OpenCGAResult<Job> result = catalogManager.getJobManager().submit(studyStr, FileDeleteTask.ID, Enums.Priority.MEDIUM, params,
                    token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{files}/acl")
    @ApiOperation(value = "Return the acl defined for the file or folder. If member is provided, it will only return the acl for the " +
            "member.", response = AclEntryList.class)
    public Response getAcls(@ApiParam(value = ParamConstants.FILES_DESCRIPTION, required = true) @PathParam("files") String fileIdStr,
                            @ApiParam(value = ParamConstants.STUDIES_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION,
                                    defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(fileIdStr);
            return createOkResponse(fileManager.getAcls(studyStr, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = AclEntryList.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
                    String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) FileAclUpdateParams params) {
        try {
            ObjectUtils.defaultIfNull(params, new FileAclUpdateParams());
            FileAclParams aclParams = new FileAclParams(
                    params.getSample(), params.getPermissions());
            List<String> idList = StringUtils.isEmpty(params.getFile()) ? Collections.emptyList() : getIdList(params.getFile(), false);
            return createOkResponse(fileManager.updateAcl(studyStr, idList, memberId, aclParams, action, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @GET
//    @Path("/{folder}/scan")
//    @ApiOperation(value = "Scans a folder", position = 6)
//    public Response scan(@ApiParam(value = "Folder id, name or path. Paths must be separated by : instead of /") @PathParam("folder")
//    String folderIdStr,
//                         @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
//                         @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//                         @ApiParam(value = "calculateChecksum") @QueryParam("calculateChecksum") @DefaultValue("false")
//                                 boolean calculateChecksum) {
//        try {
//            ParamUtils.checkIsSingleID(folderIdStr);
//            File file = fileManager.get(studyStr, folderIdStr, null, token).first();
//
//            List<File> scan = new FileScanner(catalogManager)
//                    .scan(file, null, FileScanner.FileScannerPolicy.REPLACE, calculateChecksum, false, token);
//            return createOkResponse(new DataResult<>(0, Collections.emptyList(), scan.size(), scan, scan.size()));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog file stats", response = FacetField.class)
    public Response getAggregationStats(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
            @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Name") @QueryParam("name") String name,
            @ApiParam(value = "Type") @QueryParam("type") String type,
            @ApiParam(value = "Format") @QueryParam("format") String format,
            @ApiParam(value = "Bioformat") @QueryParam("bioformat") String bioformat,
            @ApiParam(value = "Creation year") @QueryParam("creationYear") String creationYear,
            @ApiParam(value = "Creation month (JANUARY, FEBRUARY...)") @QueryParam("creationMonth") String creationMonth,
            @ApiParam(value = "Creation day") @QueryParam("creationDay") String creationDay,
            @ApiParam(value = "Creation day of week (MONDAY, TUESDAY...)") @QueryParam("creationDayOfWeek") String creationDayOfWeek,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Release") @QueryParam("release") String release,
            @ApiParam(value = "External") @QueryParam("external") Boolean external,
            @ApiParam(value = "Size") @QueryParam("size") String size,
            @ApiParam(value = "Software") @QueryParam("software") String software,
            @ApiParam(value = "Experiment") @QueryParam("experiment") String experiment,
            @ApiParam(value = "Number of samples") @QueryParam("numSamples") String numSamples,
            @ApiParam(value = "Number of related files") @QueryParam("numRelatedFiles") String numRelatedFiles,
            @ApiParam(value = ParamConstants.ANNOTATION_DESCRIPTION) @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: " +
                    "studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            DataResult<FacetField> queryResult = catalogManager.getFileManager().facet(studyStr, query, queryOptions, defaultStats, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}
