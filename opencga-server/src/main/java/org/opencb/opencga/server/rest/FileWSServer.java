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
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.catalog.models.update.FileUpdateParams;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.FileTree;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.results.OpenCGAResult;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;


@Path("/{apiVersion}/files")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Files", position = 4, description = "Methods for working with 'files' endpoint")
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
    @ApiOperation(value = "Create file or folder", response = File[].class,
            notes = "Creates a file with some content in it or a folder <br>"
                    + "<ul>"
                    + "<il><b>path</b>: Mandatory parameter. Whole path containing the file or folder name to be created</il><br>"
                    + "<il><b>content</b>: Content of the file. Only applicable if <b>directory</b> parameter set to false</il><br>"
                    + "<il><b>description</b>: Description of the file or folder to store as metadata.</il><br>"
                    + "<il><b>parents</b>: Create the parent directories if they do not exist.</il><br>"
                    + "<il><b>directory</b>: Boolean indicating whether to create a file or a directory</il><br>"
                    + "<ul>"
    )
    public Response createFilePOST(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                   @QueryParam("study") String studyStr,
                                   @ApiParam(name = "params", value = "File parameters", required = true) FileCreateParams params) {
        try {
            ObjectUtils.defaultIfNull(params, new FileCreateParams());
            DataResult<File> file;
            if (params.directory) {
                // Create directory
                file = fileManager.createFolder(studyStr, params.path, new File.FileStatus(File.FileStatus.READY), params.parents,
                        params.description, queryOptions, token);
            } else {
                // Create a file
                file = fileManager.createFile(studyStr, params.path, params.description, params.parents, params.content, token);
            }
            return createOkResponse(file);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{files}/info")
    @ApiOperation(value = "File info", position = 3, response = File[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "lazy", value = "False to return entire job and experiment object", defaultValue = "true",
                    dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response info(
            @ApiParam(value = "Comma separated list of file ids or names up to a maximum of 100")
                @PathParam(value = "files") String fileStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
            @ApiParam(value = "Boolean to retrieve deleted files", defaultValue = "false") @QueryParam("deleted") boolean deleted) {
        try {
            query.remove("study");
            query.remove("files");

            List<String> idList = getIdList(fileStr);
            DataResult<File> fileQueryResult = fileManager.get(studyStr, idList, new Query("deleted", deleted), queryOptions, true, token);
            return createOkResponse(fileQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/bioformats")
    @ApiOperation(value = "List of accepted file bioformats", position = 3)
    public Response getBioformats() {
        List<File.Bioformat> bioformats = Arrays.asList(File.Bioformat.values());
        DataResult<File.Bioformat> queryResult = new DataResult(0, Collections.emptyList(), bioformats.size(), bioformats, bioformats.size());
        return createOkResponse(queryResult);
    }

    @GET
    @Path("/formats")
    @ApiOperation(value = "List of accepted file formats", position = 3)
    public Response getFormats() {
        List<File.Format> formats = Arrays.asList(File.Format.values());
        DataResult<File.Format> queryResult = new DataResult(0, Collections.emptyList(), formats.size(), formats, formats.size());
        return createOkResponse(queryResult);
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(httpMethod = "POST", position = 4, value = "Resource to upload a file by chunks", response = File.class)
    public Response upload(
            @ApiParam(hidden = true) @FormDataParam("chunk_content") byte[] chunkBytes,
            @ApiParam(hidden = true) @FormDataParam("chunk_content") FormDataContentDisposition contentDisposition,
            @FormDataParam("file") InputStream fileInputStream,
            @FormDataParam("file") FormDataContentDisposition fileMetaData,

            @ApiParam(hidden = true) @DefaultValue("") @FormDataParam("chunk_id") String chunk_id,
            @ApiParam(hidden = true) @DefaultValue("false") @FormDataParam("last_chunk") String last_chunk,
            @ApiParam(hidden = true) @DefaultValue("") @FormDataParam("chunk_total") String chunk_total,
            @ApiParam(hidden = true) @DefaultValue("") @FormDataParam("chunk_size") String chunk_size,
            @ApiParam(hidden = true) @DefaultValue("") @FormDataParam("chunk_hash") String chunkHash,
            @ApiParam(hidden = true) @DefaultValue("false") @FormDataParam("resume_upload") String resume_upload,

            @ApiParam(value = "filename", required = false) @FormDataParam("filename") String filename,
            @ApiParam(value = "fileFormat", required = true) @DefaultValue("") @FormDataParam("fileFormat") File.Format fileFormat,
            @ApiParam(value = "bioformat", required = true) @DefaultValue("") @FormDataParam("bioformat") File.Bioformat bioformat,
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @FormDataParam("studyId") String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @FormDataParam("study") String studyStr,
            @ApiParam(value = "Path within catalog where the file will be located (default: root folder)") @DefaultValue("") @FormDataParam("relativeFilePath") String relativeFilePath,
            @ApiParam(value = "description", required = false) @DefaultValue("") @FormDataParam("description")
                    String description,
            @ApiParam(value = "Create the parent directories if they do not exist", required = false) @DefaultValue("true") @FormDataParam("parents") boolean parents) {

        if (StringUtils.isNotEmpty(studyIdStr)) {
            studyStr = studyIdStr;
        }

        long t = System.currentTimeMillis();

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

        java.nio.file.Path filePath;
        final Study study;
        try {
            String userId = catalogManager.getUserManager().getUserId(token);
            study = catalogManager.getStudyManager().resolveId(studyStr, userId);
            catalogManager.getAuthorizationManager().checkStudyPermission(study.getUid(), userId,
                    StudyAclEntry.StudyPermissions.UPLOAD_FILES);
            // TODO: Improve upload method. Check upload permission not only at study level.
        } catch (Exception e) {
            return createErrorResponse(e);
        }

        try {
            filePath = Paths.get(catalogManager.getFileManager().getUri(study.getUid(), relativeFilePath));
        } catch (CatalogException e) {
            return createErrorResponse(e);
        }

        if (chunkBytes != null && filePath != null) {

            java.nio.file.Path completedFilePath = filePath.getParent().resolve("_" + filename);
            java.nio.file.Path folderPath = filePath.getParent().resolve("__" + filename);

            logger.info(relativeFilePath + "");
            logger.info(folderPath + "");
            logger.info(filePath + "");
            boolean resume = Boolean.parseBoolean(resume_upload);

            try {
                logger.info("---resume is: " + resume);
                if (resume) {
                    logger.info("Resume ms :" + (System.currentTimeMillis() - t));
                    return createOkResponse(getResumeFileJSON(folderPath));
                }

                int chunkId = Integer.parseInt(chunk_id);
                int chunkSize = Integer.parseInt(chunk_size);
                boolean lastChunk = Boolean.parseBoolean(last_chunk);

                logger.info("---saving chunk: " + chunkId);
                logger.info("lastChunk: " + lastChunk);

                // WRITE CHUNK TYPE_FILE
                if (!Files.exists(folderPath)) {
                    logger.info("createDirectory(): " + folderPath);
                    Files.createDirectory(folderPath);
                }
                logger.info("check dir " + Files.exists(folderPath));
                // String hash = StringUtils.sha1(new String(chunkBytes));
                // logger.info("bytesHash: " + hash);
                // logger.info("chunkHash: " + chunkHash);
                // hash = chunkHash;
                if (chunkBytes.length == chunkSize) {
                    Files.write(folderPath.resolve(chunkId + "_" + chunkBytes.length + "_partial"), chunkBytes);
                } else {
                    String errorMessage = "Chunk content size (" + chunkBytes.length + ") " +
                            "!= chunk_size (" + chunk_size + ").";
                    logger.error(errorMessage);
                    return createErrorResponse(new IOException(errorMessage));
                }

                if (lastChunk) {
                    logger.info("lastChunk is true...");
                    Files.deleteIfExists(completedFilePath);
                    Files.createFile(completedFilePath);
                    List<java.nio.file.Path> chunks = getSortedChunkList(folderPath);
                    logger.info("----ordered chunks length: " + chunks.size());
                    for (java.nio.file.Path partPath : chunks) {
                        logger.info(partPath.getFileName().toString());
                        Files.write(completedFilePath, Files.readAllBytes(partPath), StandardOpenOption.APPEND);
                    }
                    IOUtils.deleteDirectory(folderPath);
                    try {
                        DataResult<File> queryResult1 = catalogManager.getFileManager().create(studyStr, File.Type.FILE,
                                fileFormat, bioformat, relativeFilePath, description, new File.FileStatus(File.FileStatus.STAGE), 0, null, -1, null, null, parents, null, null, token);
                        new FileUtils(catalogManager).upload(completedFilePath.toUri(), queryResult1.first(), null, token, false, false, true, true, Long.MAX_VALUE);
                        DataResult<File> queryResult = catalogManager.getFileManager().get(queryResult1.first().getUid(), null, token);
                        File file = new FileMetadataReader(catalogManager).setMetadataInformation(queryResult.first(), null,
                                new QueryOptions(queryOptions), token, false);
                        queryResult.setResults(Collections.singletonList(file));
                        return createOkResponse(queryResult);
                    } catch (Exception e) {
                        logger.error(e.toString());
                        return createErrorResponse(e);
                    }
                }
            } catch (IOException e) {
                System.out.println("e = " + e);
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            logger.info("chunk saved ms :" + (System.currentTimeMillis() - t));
            return createOkResponse("ok");

        } else if (fileInputStream != null) {
            if (filename == null) {
                filename = fileMetaData.getFileName();
            }

            File file = new File()
                    .setName(filename)
                    .setPath(relativeFilePath + filename)
                    .setFormat(fileFormat)
                    .setBioformat(bioformat);
            try {
                return createOkResponse(fileManager.upload(studyStr, fileInputStream, file, false, parents, true, token));
            } catch (Exception e) {
                return createErrorResponse("Upload file", e.getMessage());
            }
        } else {
            return createErrorResponse("Upload file", "No file or chunk found");
        }
    }

    @GET
    @Path("/{file}/download")
    @ApiOperation(value = "Download file", position = 5, response = QueryResponse.class,
            notes = "The usage of /{file}/download webservice through Swagger is <b>discouraged</b>. Please, don't click the 'Try it "
                    + "out' button here as it may hang this web page. Instead, build the final URL in a different tab.<br>"
                    + "An special <b>DOWNLOAD</b> permission is needed to download files from OpenCGA.")
    public Response download(@ApiParam(value = "File id, name or path. Paths must be separated by : instead of /") @PathParam("file") String fileIdStr,
                             @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                             @QueryParam("study") String studyStr) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            DataInputStream stream = catalogManager.getFileManager().download(studyStr, fileIdStr, -1, -1, token);
            return createOkResponse(stream, MediaType.APPLICATION_OCTET_STREAM_TYPE, fileIdStr);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/content")
    @ApiOperation(value = "Show the content of a file (up to a limit)", position = 6, response = String.class)
    public Response content(@ApiParam(value = "File id, name or path. Paths must be separated by : instead of /") @PathParam("file") String fileIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "start", required = false) @QueryParam("start") @DefaultValue("-1") int start,
                            @ApiParam(value = "limit", required = false) @QueryParam("limit") @DefaultValue("-1") int limit) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            String userId = catalogManager.getUserManager().getUserId(token);
            Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
            File file = fileManager.get(studyStr, fileIdStr, FileManager.INCLUDE_FILE_IDS, token).first();
            catalogManager.getAuthorizationManager().checkFilePermission(study.getUid(), file.getUid(), userId,
                    FileAclEntry.FilePermissions.VIEW_CONTENT);

            DataInputStream stream = catalogManager.getFileManager().download(studyStr, fileIdStr, start, limit, token);
            return createOkResponse(stream, MediaType.TEXT_PLAIN_TYPE);

        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/grep")
    @ApiOperation(value = "Filter lines of the file containing a match of the pattern [NOT TESTED]", position = 7, response = String.class)
    public Response downloadGrep(
            @ApiParam(value = "File id, name or path. Paths must be separated by : instead of /") @PathParam("file") String fileIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Pattern", required = false) @QueryParam("pattern") @DefaultValue(".*") String pattern,
            @ApiParam(value = "Do a case insensitive search", required = false) @DefaultValue("false") @QueryParam("ignoreCase")
                    Boolean ignoreCase,
            @ApiParam(value = "Return multiple matches", required = false) @DefaultValue("true") @QueryParam("multi") Boolean multi) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            QueryOptions options = new QueryOptions("ignoreCase", ignoreCase);
            options.put("multi", multi);
            try (DataInputStream stream = catalogManager.getFileManager().grep(studyStr, fileIdStr, pattern, options, token)) {
                return createOkResponse(stream, MediaType.TEXT_PLAIN_TYPE);
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{file}/set-header")
    @ApiOperation(value = "Set file header [DEPRECATED]", position = 10, notes = "Deprecated method. Moved to update.", hidden = true)
    public Response setHeader(@PathParam(value = "file") @FormDataParam("fileId") String fileStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr,
                              @ApiParam(value = "header", required = true) @DefaultValue("") @QueryParam("header") String header) {
        String content = "";
        DataInputStream stream;
        DataResult<File> fileQueryResult;
        InputStream streamBody = null;
        try {
            ParamUtils.checkIsSingleID(fileStr);
            String userId = catalogManager.getUserManager().getUserId(token);
            Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
            File file = fileManager.get(studyStr, fileStr, FileManager.INCLUDE_FILE_URI, token).first();
            catalogManager.getAuthorizationManager().checkFilePermission(study.getUid(), file.getUid(), userId,
                    FileAclEntry.FilePermissions.WRITE);

            /** Obtain file uri **/
            URI fileUri = catalogManager.getFileManager().getUri(file);
            System.out.println("getUri: " + fileUri.getPath());

            /** Set header **/
            stream = catalogManager.getFileManager().download(studyStr, fileStr, -1, -1, token);
            content = org.apache.commons.io.IOUtils.toString(stream);
            String lines[] = content.split(System.getProperty("line.separator"));
            StringBuilder body = new StringBuilder();
            body.append(header);
            body.append(System.getProperty("line.separator"));
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i];
                if (!line.startsWith("#")) {
                    body.append(line);
                    if (i != lines.length - 1)
                        body.append(System.getProperty("line.separator"));
                }
            }
            /** Write/Copy  file **/
            streamBody = new ByteArrayInputStream(body.toString().getBytes(StandardCharsets.UTF_8));
            Files.copy(streamBody, Paths.get(fileUri), StandardCopyOption.REPLACE_EXISTING);

        } catch (Exception e) {
            return createErrorResponse(e);
        }
//        createOkResponse(content, MediaType.TEXT_PLAIN)
        return createOkResponse(streamBody, MediaType.TEXT_PLAIN_TYPE);
    }

    @Deprecated
    @GET
    @Path("/{folder}/files")
    @ApiOperation(value = "File content [DEPRECATED]", position = 11, notes = "Deprecated method. Moved to /list.", hidden = true)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query")
    })
    public Response getAllFilesInFolder(@PathParam(value = "folder") @FormDataParam("folderId") String folderStr,
                                        @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or "
                                                + "alias") @QueryParam("study") String studyStr) {
        DataResult<File> results;
        try {
            results = catalogManager.getFileManager().getFilesFromFolder(folderStr, studyStr, queryOptions, token);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return createOkResponse(results);
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "File search method.", position = 12, response = File[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "lazy", value = "False to return entire job and experiment object", defaultValue = "true",
                    dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = Constants.FLATTENED_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId")
                    String studyIdStr,
            @ApiParam(value = "Study [[user@]project:]{study}  where study and project can be either the id or alias.")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Comma separated list of file names") @DefaultValue("") @QueryParam("name") String name,
            @ApiParam(value = "Comma separated list of paths", required = false) @DefaultValue("") @QueryParam("path") String path,
            @ApiParam(value = "Available types (FILE, DIRECTORY)", required = false) @DefaultValue("") @QueryParam("type") String type,
            @ApiParam(value = "Comma separated Bioformat values. For existing Bioformats see files/bioformats", required = false) @DefaultValue("") @QueryParam("bioformat") String bioformat,
            @ApiParam(value = "Comma separated Format values. For existing Formats see files/formats", required = false) @DefaultValue("") @QueryParam("format") String formats,
            @ApiParam(value = "Status", required = false) @DefaultValue("") @QueryParam("status") String status,
            @ApiParam(value = "Directory under which we want to look for files or folders", required = false) @DefaultValue("") @QueryParam("directory") String directory,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
            @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)")
            @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "Description", required = false) @DefaultValue("") @QueryParam("description") String description,
            @ApiParam(value = "Tags") @QueryParam("tags") String tags,
            @ApiParam(value = "Size", required = false) @DefaultValue("") @QueryParam("size") String size,
            @ApiParam(value = "Comma separated list of sample ids", hidden = true) @QueryParam("sample") String sample,
            @ApiParam(value = "Comma separated list of sample ids") @QueryParam("samples") String samples,
            @ApiParam(value = "(DEPRECATED) Job id that created the file(s) or folder(s)", hidden = true) @QueryParam("jobId") String jobIdOld,
            @ApiParam(value = "Job id that created the file(s) or folder(s)", required = false) @QueryParam("job.id") String jobId,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Boolean to retrieve deleted files", defaultValue = "false") @QueryParam("deleted") boolean deleted,
            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
            @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)", required = false) @DefaultValue("")
            @QueryParam("nattributes") String nattributes,
            @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
            @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            query.remove("study");
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            if (StringUtils.isNotEmpty(sample) && !query.containsKey(FileDBAdaptor.QueryParams.SAMPLES.key())) {
                query.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sample);
            }

            if (query.containsKey(FileDBAdaptor.QueryParams.NAME.key())
                    && (query.get(FileDBAdaptor.QueryParams.NAME.key()) == null
                    || query.getString(FileDBAdaptor.QueryParams.NAME.key()).isEmpty())) {
                query.remove(FileDBAdaptor.QueryParams.NAME.key());
                logger.debug("Name attribute empty, it's been removed");
            }
            // TODO: jobId is deprecated. Remember to remove this if after next release
            if (query.containsKey("jobId") && !query.containsKey(FileDBAdaptor.QueryParams.JOB_UID.key())) {
                query.put(FileDBAdaptor.QueryParams.JOB_UID.key(), query.get("jobId"));
                query.remove("jobId");
            }

            DataResult<File> result;
            if (count) {
                result = fileManager.count(studyStr, query, token);
            } else {
                result = fileManager.search(studyStr, query, queryOptions, token);
            }

            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{folder}/list")
    @ApiOperation(value = "List all the files inside the folder", position = 13, response = File[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", defaultValue = "false", dataType = "boolean",
                    paramType = "query")
    })
    public Response list(@ApiParam(value = "Folder id, name or path") @PathParam("folder") String folder,
                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                         @QueryParam("study") String studyStr) {
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
    @ApiOperation(value = "Obtain a tree view of the files and folders within a folder", position = 15, response = FileTree[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "[TO BE IMPLEMENTED] Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
    })
    public Response treeView(@ApiParam(value = "Folder id, name or path. Paths must be separated by : instead of /") @DefaultValue(":")
                             @PathParam("folder") String folderId,
                             @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                             @QueryParam("study") String studyStr,
                             @ApiParam(value = "Maximum depth to get files from") @DefaultValue("5") @QueryParam("maxDepth") int maxDepth) {
        try {
            query.remove("study");
            query.remove("folder");
            query.remove("maxDepth");

            ParamUtils.checkIsSingleID(folderId);
            query.remove("maxDepth");
            DataResult result = fileManager.getTree(studyStr, folderId.replace(":", "/"), query, queryOptions, maxDepth, token);
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

    @POST
    @Path("/update")
    @ApiOperation(value = "Update some file attributes", response = File.class)
    public Response updateQuery(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Comma separated list of file names") @QueryParam("name") String name,
            @ApiParam(value = "Comma separated list of paths") @QueryParam("path") String path,
            @ApiParam(value = "Available types (FILE, DIRECTORY)") @QueryParam("type") String type,
            @ApiParam(value = "Comma separated bioformat values. For existing bioformats see files/bioformats")
                @QueryParam("bioformat")String bioformat,
            @ApiParam(value = "Comma separated format values. For existing formats see files/formats")
                @QueryParam("format") String formats,
            @ApiParam(value = "Status") @QueryParam("status") String status,
            @ApiParam(value = "Directory under which we want to look for files or folders") @QueryParam("directory") String directory,
            @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @QueryParam("creationDate") String creationDate,
            @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss)") @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "Description") @QueryParam("description") String description,
            @ApiParam(value = "Size") @QueryParam("size") String size,
            @ApiParam(value = "Comma separated list of sample ids or names") @QueryParam("samples") String samples,
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,
            @ApiParam(value = "Job id that created the file(s) or folder(s)") @QueryParam("job.id") String jobId,
            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @QueryParam("attributes") String attributes,
            @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)") @QueryParam("nattributes") String nattributes,
            @ApiParam(value = "Release value") @QueryParam("release") String release,

            @ApiParam(value = "Action to be performed if the array of samples is being updated.", defaultValue = "ADD") @QueryParam("samplesAction") ParamUtils.UpdateAction samplesAction,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", defaultValue = "ADD") @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(name = "params", value = "Parameters to modify", required = true) FileUpdateParams updateParams) {
        try {
            query.remove("study");
            if (samplesAction == null) {
                samplesAction = ParamUtils.UpdateAction.ADD;
            }
            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.UpdateAction.ADD;
            }

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(FileDBAdaptor.QueryParams.SAMPLES.key(), samplesAction.name());
            actionMap.put(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            DataResult<File> queryResult = fileManager.update(studyStr, query, updateParams, true, queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{files}/update")
    @ApiOperation(value = "Update some file attributes", response = File.class)
    public Response updatePOST(
            @ApiParam(value = "Comma separated list of file ids, names or paths. Paths must be separated by : instead of /")
                @PathParam(value = "files") String fileIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Action to be performed if the array of samples is being updated.", defaultValue = "ADD") @QueryParam("samplesAction") ParamUtils.UpdateAction samplesAction,
            @ApiParam(value = "Action to be performed if the array of annotationSets is being updated.", defaultValue = "ADD") @QueryParam("annotationSetsAction") ParamUtils.UpdateAction annotationSetsAction,
            @ApiParam(name = "params", value = "Parameters to modify", required = true) FileUpdateParams updateParams) {
        try {
            if (samplesAction == null) {
                samplesAction = ParamUtils.UpdateAction.ADD;
            }
            if (annotationSetsAction == null) {
                annotationSetsAction = ParamUtils.UpdateAction.ADD;
            }

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(FileDBAdaptor.QueryParams.SAMPLES.key(), samplesAction.name());
            actionMap.put(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key(), annotationSetsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            List<String> fileIds = getIdList(fileIdStr);

            DataResult<File> queryResult = fileManager.update(studyStr, fileIds, updateParams, true, queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{file}/annotationSets/{annotationSet}/annotations/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update annotations from an annotationSet")
    public Response updateAnnotations(
            @ApiParam(value = "File id, name or path. Paths must be separated by : instead of /", required = true)
            @PathParam("file") String fileStr,
            @ApiParam(value = "Study [[user@]project:]study.") @QueryParam("study") String studyStr,
            @ApiParam(value = "AnnotationSet id to be updated.") @PathParam("annotationSet") String annotationSetId,
            @ApiParam(value = "Action to be performed: ADD to add new annotations; REPLACE to replace the value of an already existing "
                    + "annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE to remove some "
                    + "annotations; RESET to set some annotations to the default value configured in the corresponding variables of the "
                    + "VariableSet if any.", defaultValue = "ADD") @QueryParam("action") ParamUtils.CompleteUpdateAction action,
            @ApiParam(value = "Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key "
                    + "'remove' containing the comma separated variables to be removed as a value when the action is REMOVE or a json "
                    + "with only the key 'reset' containing the comma separated variables that will be set to the default value"
                    + " when the action is RESET") Map<String, Object> updateParams) {
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

    @GET
    @Path("/link")
    @ApiOperation(value = "Link an external file into catalog.", hidden = true, position = 19, response = QueryResponse.class)
    @Deprecated
    public Response linkGet(@ApiParam(value = "Uri of the file", required = true) @QueryParam("uri") String uriStr,
                            @ApiParam(value = "(DEPRECATED) Use study instead") @QueryParam("studyId") String studyIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
                            @ApiParam(value = "Path where the external file will be allocated in catalog", required = true) @QueryParam("path") String path,
                            @ApiParam(value = "Description") @QueryParam("description") String description,
                            @ApiParam(value = "Create the parent directories if they do not exist") @DefaultValue("false") @QueryParam("parents") boolean parents,
                            @ApiParam(value = "Size of the folder/file") @QueryParam("size") long size,
                            @ApiParam(value = "Checksum of something") @QueryParam("checksum") String checksum) {
        try {

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            logger.debug("study: {}", studyStr);

            path = path.replace(":", "/");

            ObjectMap objectMap = new ObjectMap()
                    .append("parents", parents)
                    .append("description", description);
            objectMap.putIfNotEmpty(FileDBAdaptor.QueryParams.CHECKSUM.key(), checksum);

            List<DataResult<File>> queryResultList = new ArrayList<>();
            logger.info("path: {}", path);
            // If it is just one uri to be linked, it will return an error response if there is some kind of error.
            URI myUri = UriUtils.createUri(uriStr);
            queryResultList.add(catalogManager.getFileManager().link(studyStr, myUri, path, objectMap, token));

            return createOkResponse(queryResultList);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/link")
    @ApiOperation(value = "Link an external file into catalog.", response = QueryResponse.class)
    public Response link(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
            @ApiParam(value = "Create the parent directories if they do not exist") @DefaultValue("false") @QueryParam("parents") boolean parents,
            @ApiParam(name = "params", value = "File parameters", required = true) FileLinkParams params) {
        try {
            if (StringUtils.isEmpty(params.uri)) {
                throw new CatalogException("Missing mandatory field 'uri'");
            }

            logger.debug("study: {}", studyStr);
            logger.debug("uri: {}", params.uri);
            logger.debug("params: {}", params);

            // TODO: We should stop doing this at some point. As the parameters are now passed through the body, users can already pass "/" characters
            if (params.path == null) {
                params.path = "";
            }
            params.path = params.path.replace(":", "/");

            ObjectMap objectMap = new ObjectMap("parents", parents);
            objectMap.putIfNotEmpty("description", params.description);
            objectMap.putIfNotNull("relatedFiles", params.getRelatedFiles());

            List<DataResult<File>> queryResultList = new ArrayList<>();
            URI myUri = UriUtils.createUri(params.uri);
            queryResultList.add(catalogManager.getFileManager().link(studyStr, myUri, params.path, objectMap, token));

            return createOkResponse(queryResultList);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{files}/unlink")
    @ApiOperation(value = "Unlink linked files and folders")
    public Response unlink(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
            @ApiParam(value = "Comma separated list of file ids, names or paths.") @PathParam("files") String files) {
        try {
            List<String> fileIds = getIdList(files);

            ObjectMap params = new ObjectMap()
                    .append("files", files)
                    .append("study", studyStr);
            OpenCGAResult<Job> result = catalogManager.getJobManager().submit(studyStr, "files", "unlink", Enums.Priority.MEDIUM, params, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{fileId}/relink")
    @ApiOperation(value = "Change file location. Provided file must be either STAGE or be an external file. [DEPRECATED]", hidden = true,
            position = 21)
    public Response relink(@ApiParam(value = "File Id") @PathParam("fileId") @DefaultValue("") String fileIdStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                           @QueryParam("study") String studyStr,
                           @ApiParam(value = "New URI", required = true) @QueryParam("uri") String uriStr,
                           @ApiParam(value = "Do calculate checksum for new files", required = false) @DefaultValue("false")
                           @QueryParam("calculateChecksum") boolean calculateChecksum) {
        try {
            URI uri = UriUtils.createUri(uriStr);
            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(uri);

            if (!ioManager.exists(uri)) {
                throw new CatalogIOException("File " + uri + " does not exist");
            }

            File file = fileManager.get(studyStr, fileIdStr, null, token).first();

            new FileUtils(catalogManager).link(file, calculateChecksum, uri, false, true, token);
            file = catalogManager.getFileManager().get(file.getUid(), queryOptions, token).first();
            file = FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, new QueryOptions(queryOptions), token,
                    false);

            return createOkResponse(new DataResult<>(0, Collections.emptyList(), 1, Collections.singletonList(file), 1));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/refresh")
    @ApiOperation(value = "Refresh metadata from the selected file or folder. Return updated files.", position = 22,
            response = QueryResponse.class)
    public Response refresh(@ApiParam(value = "File id, name or path. Paths must be separated by : instead of /") @PathParam(value = "file") String fileIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr) {
        try {
            ParamUtils.checkIsSingleID(fileIdStr);
            File file = fileManager.get(studyStr, fileIdStr, null, token).first();

            List<File> files;
            FileUtils catalogFileUtils = new FileUtils(catalogManager);
            FileMetadataReader fileMetadataReader = FileMetadataReader.get(catalogManager);
            if (file.getType() == File.Type.FILE) {
                File file1 = catalogFileUtils.checkFile(studyStr, file, false, token);
                file1 = fileMetadataReader.setMetadataInformation(file1, null, new QueryOptions(queryOptions), token, false);
                if (file == file1) {    //If the file is the same, it was not modified. Only return modified files.
                    files = Collections.emptyList();
                } else {
                    files = Collections.singletonList(file);
                }
            } else {
                List<File> result = catalogManager.getFileManager().getFilesFromFolder(fileIdStr, studyStr, null, token).getResults();
                files = new ArrayList<>(result.size());
                for (File f : result) {
                    File file1 = fileMetadataReader.setMetadataInformation(f, null, new QueryOptions(queryOptions), token, false);
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
    @ApiOperation(value = "Delete existing files and folders")
    public Response delete(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                @QueryParam("study") String studyStr,
            @ApiParam(value = "Comma separated list of file ids, names or paths.") @PathParam("files") String files,
            @ApiParam(value = "Skip trash and delete the files/folders from disk directly (CANNOT BE RECOVERED)", defaultValue = "false")
                    @QueryParam(Constants.SKIP_TRASH) boolean skipTrash) {
        try {
            List<String> fileIds = getIdList(files);

            ObjectMap params = new ObjectMap()
                    .append("files", files)
                    .append("study", studyStr)
                    .append(Constants.SKIP_TRASH, skipTrash);
            OpenCGAResult<Job> result = catalogManager.getJobManager().submit(studyStr, "files", "delete", Enums.Priority.MEDIUM, params, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group files by several fields", position = 24, response = QueryResponse.class, hidden = true,
            notes = "Only group by categorical variables. Grouping by continuous variables might cause unexpected behaviour")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "count", value = "Count the number of elements matching the group", dataType = "boolean",
                    paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Maximum number of documents (groups) to be returned", dataType = "integer",
                    paramType = "query", defaultValue = "50")
    })
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                            @QueryParam("fields") String fields,
                            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @DefaultValue("") @QueryParam("studyId")
                                    String studyIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "Comma separated list of names.", required = false) @DefaultValue("") @QueryParam("name")
                                    String names,
                            @ApiParam(value = "Comma separated Type values.", required = false) @DefaultValue("") @QueryParam("type")
                                    String type,
                            @ApiParam(value = "Comma separated Bioformat values.", required = false) @DefaultValue("")
                            @QueryParam("bioformat") String bioformat,
                            @ApiParam(value = "Comma separated Format values.", required = false) @DefaultValue("") @QueryParam("format")
                                    String formats,
                            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
                            @ApiParam(value = "directory", required = false) @DefaultValue("") @QueryParam("directory") String directory,
                            @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate")
                                    String creationDate,
                            @ApiParam(value = "size", required = false) @DefaultValue("") @QueryParam("size") String size,
                            @ApiParam(value = "Comma separated sampleIds", hidden = true) @QueryParam("sampleIds") String sampleIds,
                            @ApiParam(value = "Comma separated list of sample ids or names") @QueryParam("samples") String samples) {
        try {
            query.remove("study");
            query.remove("fields");

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            if (StringUtils.isNotEmpty(sampleIds) && !query.containsKey(FileDBAdaptor.QueryParams.SAMPLES.key())) {
                query.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleIds);
            }
            DataResult result = fileManager.groupBy(studyStr, query, fields, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{files}/acl")
    @ApiOperation(value = "Return the acl defined for the file or folder. If member is provided, it will only return the acl for the member.", position = 18, response = QueryResponse.class)
    public Response getAcls(@ApiParam(value = "Comma separated list of file ids or names up to a maximum of 100.", required = true)
                            @PathParam("files") String fileIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias up to a maximum of 100")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "User or group id") @QueryParam("member") String member,
                            @ApiParam(value = "Boolean to retrieve all possible entries that are queried for, false to raise an "
                                    + "exception whenever one of the entries looked for cannot be shown for whichever reason",
                                    defaultValue = "false") @QueryParam("silent") boolean silent) {
        try {
            List<String> idList = getIdList(fileIdStr);
            return createOkResponse(fileManager.getAcls(studyStr, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    // Temporal method used by deprecated methods. This will be removed at some point.
    @Override
    protected File.FileAclParams getAclParams(
            @ApiParam(value = "Comma separated list of permissions to add", required = false) @QueryParam("add") String addPermissions,
            @ApiParam(value = "Comma separated list of permissions to remove", required = false) @QueryParam("remove") String removePermissions,
            @ApiParam(value = "Comma separated list of permissions to set", required = false) @QueryParam("set") String setPermissions)
            throws CatalogException {
        int count = 0;
        count += StringUtils.isNotEmpty(setPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(addPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(removePermissions) ? 1 : 0;
        if (count > 1) {
            throw new CatalogException("Only one of add, remove or set parameters are allowed.");
        } else if (count == 0) {
            throw new CatalogException("One of add, remove or set parameters is expected.");
        }

        String permissions = null;
        AclParams.Action action = null;
        if (StringUtils.isNotEmpty(addPermissions)) {
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
        return new File.FileAclParams(permissions, action, null);
    }

    @POST
    @Path("/{file}/acl/{memberId}/update")
    @ApiOperation(value = "Update the permissions granted for the user or group [DEPRECATED]", position = 21,
            hidden = true, response = QueryResponse.class,
            notes = "DEPRECATED: The usage of this webservice is discouraged. A different entrypoint /acl/{members}/update has been added "
                    + "to also support changing permissions using queries.")
    public Response updateAclPOST(
            @ApiParam(value = "File id, name or path. Paths must be separated by : instead of /", required = true) @PathParam("file") String fileIdStr,
            @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "JSON containing one of the keys 'add', 'set' or 'remove'", required = true) StudyWSServer.MemberAclUpdateOld params) {
        try {
            File.FileAclParams aclParams = getAclParams(params.add, params.remove, params.set);
            List<String> idList = StringUtils.isEmpty(fileIdStr) ? Collections.emptyList() : getIdList(fileIdStr);
            return createOkResponse(fileManager.updateAcl(studyStr, idList, memberId, aclParams, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class FileAcl extends AclParams {
        public String file;
        public String sample;
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("members") String memberId,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) FileAcl params) {
        try {
            ObjectUtils.defaultIfNull(params, new FileAcl());

            File.FileAclParams aclParams = new File.FileAclParams(
                    params.getPermissions(), params.getAction(), params.sample);
            List<String> idList = StringUtils.isEmpty(params.file) ? Collections.emptyList() : getIdList(params.file, false);
            return createOkResponse(fileManager.updateAcl(studyStr, idList, memberId, aclParams, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{folder}/scan")
    @ApiOperation(value = "Scans a folder", position = 6)
    public Response scan(@ApiParam(value = "Folder id, name or path. Paths must be separated by : instead of /") @PathParam("folder") String folderIdStr,
                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                         @QueryParam("study") String studyStr,
                         @ApiParam(value = "calculateChecksum") @QueryParam("calculateChecksum") @DefaultValue("false")
                                 boolean calculateChecksum) {
        try {
            ParamUtils.checkIsSingleID(folderIdStr);
            File file = fileManager.get(studyStr, folderIdStr, null, token).first();

            List<File> scan = new FileScanner(catalogManager)
                    .scan(file, null, FileScanner.FileScannerPolicy.REPLACE, calculateChecksum, false, token);
            return createOkResponse(new DataResult<>(0, Collections.emptyList(), scan.size(), scan, scan.size()));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Fetch catalog file stats", position = 15, hidden = true, response = QueryResponse.class)
    public Response getStats(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
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
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove("study");
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            DataResult<FacetField> queryResult = catalogManager.getFileManager().facet(studyStr, query, queryOptions, defaultStats, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog file stats", position = 15, response = QueryResponse.class)
    public Response getAggregationStats(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
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
            @ApiParam(value = "Annotation, e.g: key1=value(;key2=value)") @QueryParam("annotation") String annotation,

            @ApiParam(value = "Calculate default stats", defaultValue = "false") @QueryParam("default") boolean defaultStats,

            @ApiParam(value = "List of fields separated by semicolons, e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1") @QueryParam("field") String facet) {
        try {
            query.remove("study");
            query.remove("field");

            queryOptions.put(QueryOptions.FACET, facet);

            DataResult<FacetField> queryResult = catalogManager.getFileManager().facet(studyStr, query, queryOptions, defaultStats, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class FileCreateParams {
        @JsonProperty(required = true)
        public String path;
        public String content;
        public String description;
        @JsonProperty(defaultValue = "false")
        public boolean parents;
        @JsonProperty(defaultValue = "false")
        public boolean directory;
    }

    public static class RelatedFile {
        public String file;
        public File.RelatedFile.Relation relation;
    }

    private static class FileLinkParams {
        public String uri;
        public String path;
        public String description;
        public List<RelatedFile> relatedFiles;

        @Override
        public String toString() {
            return "FileLinkParams{" +
                    "uri='" + uri + '\'' +
                    ", path='" + path + '\'' +
                    ", description='" + description + '\'' +
                    ", relatedFiles=" + relatedFiles +
                    '}';
        }

        public List<File.RelatedFile> getRelatedFiles() {
            if (ListUtils.isEmpty(relatedFiles)) {
                return null;
            }
            List<File.RelatedFile> relatedFileList = new ArrayList<>(relatedFiles.size());
            for (RelatedFile relatedFile : relatedFiles) {
                relatedFileList.add(new File.RelatedFile(new File().setId(relatedFile.file), relatedFile.relation));
            }
            return relatedFileList;
        }
    }

}
