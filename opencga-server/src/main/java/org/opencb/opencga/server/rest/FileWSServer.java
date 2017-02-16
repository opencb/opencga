/*
 * Copyright 2015-2016 OpenCB
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
import org.apache.commons.lang3.time.StopWatch;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.api.IFileManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.FileIndex;
import org.opencb.opencga.catalog.models.FileTree;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.*;


@Path("/{version}/files")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Files", position = 4, description = "Methods for working with 'files' endpoint")
public class FileWSServer extends OpenCGAWSServer {

    private IFileManager fileManager;
    public FileWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException,
            ClassNotFoundException, IllegalAccessException, InstantiationException, VersionException {
        super(uriInfo, httpServletRequest);
        fileManager = catalogManager.getFileManager();
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
            QueryResult<File> file;
            if (params.directory) {
                // Create directory
                file = fileManager.createFolder(studyStr, params.path, new File.FileStatus(File.FileStatus.READY), params.parents,
                        params.description, queryOptions, sessionId);
            } else {
                // Create a file
                file = fileManager.create(studyStr, File.Type.FILE, File.Format.PLAIN, File.Bioformat.UNKNOWN, params.path, null,
                        params.description, new File.FileStatus(File.FileStatus.READY), 0, -1, null, -1, null, null, params.parents,
                        params.content, queryOptions, sessionId);
            }
            return createOkResponse(file);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/create-folder")
    @ApiOperation(value = "Create a folder in the catalog environment [WARNING]", position = 2, response = File.class,
            notes = "WARNING: the usage of this web service is discouraged, please use the POST /create version instead. Be aware that "
                    + "this is web service is not tested and this can be deprecated in a future version.")
    public Response createFolder(@ApiParam(value = "(DEPRECATED) Use study instead", hidden = true)
                                 @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                 @QueryParam("study") String studyStr,
                                 @ApiParam(value = "CSV list of paths where the folders will be created", required = true)
                                 @QueryParam("folders") String folders,
                                 @ApiParam(value = "Paths where the folder will be created", required = true)
                                 @QueryParam("path") String path,
                                 @ApiParam(value = "Create the parent directories if they do not exist")
                                 @QueryParam("parents") @DefaultValue("false") boolean parents) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            if (StringUtils.isNotEmpty(path)) {
                folders = path;
                query.put("folders", folders);
            }

            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            List<String> folderList = Arrays.asList(folders.replace(":", "/").split(","));

            List<QueryResult> queryResultList = new ArrayList<>(folderList.size());
            for (String folder : folderList) {
                try {
                    java.nio.file.Path folderPath = Paths.get(folder);
                    QueryResult<File> newFolder = catalogManager.getFileManager().createFolder(Long.toString(studyId), folderPath.toString(),
                            null, parents, null, queryOptions, sessionId);
                    newFolder.setId("Create folder");
                    queryResultList.add(newFolder);
                } catch (CatalogException e) {
                    queryResultList.add(new QueryResult<>("Create folder", -1, 0, 0, "", e.getMessage(), Collections.emptyList()));
                }
            }
            return createOkResponse(queryResultList);
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
                    dataType = "boolean", paramType = "query")
    })
    public Response info(@ApiParam(value="Comma separated list of file ids") @PathParam(value = "files") String fileStr,
                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                         @QueryParam("study") String studyStr) {
        try {
            List<QueryResult<File>> queryResults = new LinkedList<>();
            AbstractManager.MyResourceIds resourceIds = fileManager.getIds(fileStr, studyStr, sessionId);

            for (Long fileId : resourceIds.getResourceIds()) {
                queryResults.add(catalogManager.getFile(fileId, queryOptions, sessionId));
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{fileId}/uri")
    @ApiOperation(value = "File uri [DEPRECATED]", position = 3, notes = "Deprecated method. Use /info with include query options instead.",
            hidden = true)
    public Response getUri(@ApiParam(value = "fileId") @PathParam(value = "fileId") String fileStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                           @QueryParam("study") String studyStr) {
        try {
            List<QueryResult> results = new LinkedList<>();
            AbstractManager.MyResourceIds resourceIds = fileManager.getIds(fileStr, studyStr, sessionId);

            for (long fileId : resourceIds.getResourceIds()) {
                System.out.println("fileId = " + fileId);
                QueryResult<File> result = catalogManager.getFile(fileId, queryOptions, sessionId);
                URI fileUri = result.first().getUri();
                results.add(new QueryResult<>(Long.toString(fileId), 0, 1, 1, "", "", Collections.singletonList(fileUri)));
            }
            return createOkResponse(results);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/bioformats")
    @ApiOperation(value = "List of accepted file bioformats", position = 3)
    public Response getBioformats() {
        List<File.Bioformat> bioformats = Arrays.asList(File.Bioformat.values());
        QueryResult<File.Bioformat> queryResult = new QueryResult("Bioformats", 0, bioformats.size(), bioformats.size(), "", "", bioformats);
        return createOkResponse(queryResult);
    }

    @GET
    @Path("/formats")
    @ApiOperation(value = "List of accepted file formats", position = 3)
    public Response getFormats() {
        List<File.Format> formats = Arrays.asList(File.Format.values());
        QueryResult<File.Format> queryResult = new QueryResult("Formats", 0, formats.size(), formats.size(), "", "", formats);
        return createOkResponse(queryResult);
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(httpMethod = "POST", position = 4, value = "Resource to upload a file by chunks", response = File.class)
    public Response upload(@ApiParam(hidden = true) @FormDataParam("chunk_content") byte[] chunkBytes,
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
                           @ApiParam(value = "fileFormat", required = true) @DefaultValue("") @FormDataParam("fileFormat")
                                   String fileFormat,
                           @ApiParam(value = "bioformat", required = true) @DefaultValue("") @FormDataParam("bioformat")
                                   String bioformat,
                           @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @FormDataParam("studyId")
                                   String studyIdStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                           @FormDataParam("study") String studyStr,
                           @ApiParam(value = "Path within catalog where the file will be located (default: root folder)",
                                   required = true) @DefaultValue(".") @FormDataParam("relativeFilePath") String relativeFilePath,
                           @ApiParam(value = "description", required = false) @DefaultValue("") @FormDataParam("description")
                                   String description,
                           @ApiParam(value = "Create the parent directories if they do not exist", required = false)
                           @DefaultValue("true") @FormDataParam("parents") boolean parents) {

        if (StringUtils.isNotEmpty(studyIdStr)) {
            studyStr = studyIdStr;
        }

        try {
            File.Format.valueOf(fileFormat.toUpperCase());
        } catch (IllegalArgumentException e) {
            return createErrorResponse(new CatalogException("The file format " + fileFormat + " introduced is not valid."));
        }

        try {
            File.Bioformat.valueOf(bioformat.toUpperCase());
        } catch (IllegalArgumentException e) {
            return createErrorResponse(new CatalogException("The file bioformat " + bioformat + " introduced is not valid."));
        }

        long t = System.currentTimeMillis();

        if (relativeFilePath.endsWith("/")) {
            relativeFilePath = relativeFilePath.substring(0, relativeFilePath.length() - 1);
        }

        if (relativeFilePath.startsWith("/")) {
            return createErrorResponse(new CatalogException("The path cannot be absolute"));
        }

        java.nio.file.Path filePath = null;
        final long studyId;
        try {
            studyId = catalogManager.getStudyId(studyStr, sessionId);
            String userId = catalogManager.getUserManager().getId(sessionId);
            catalogManager.getAuthorizationManager().checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.UPLOAD_FILES);
            // TODO: Improve upload method. Check upload permission not only at study level.
        } catch (Exception e) {
            return createErrorResponse(e);
        }

        try {
            filePath = Paths.get(catalogManager.getFileUri(studyId, relativeFilePath));
            System.out.println(filePath);
        } catch (CatalogIOException e) {
            System.out.println("catalogManager.getFilePath");
            e.printStackTrace();
        } catch (CatalogException e) {
            e.printStackTrace();
        }

        if (chunkBytes != null) {

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
                        QueryResult<File> queryResult = catalogManager.createFile(studyId, File.Format.valueOf(fileFormat.toUpperCase()),
                                File.Bioformat.valueOf(bioformat.toUpperCase()), relativeFilePath, completedFilePath.toUri(), description,
                                parents, sessionId);
                        File file = new FileMetadataReader(catalogManager).setMetadataInformation(queryResult.first(), null,
                                new QueryOptions(queryOptions), sessionId, false);
                        queryResult.setResult(Collections.singletonList(file));
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
            logger.info("filePath: {}", filePath.toString());

            // We obtain the basic studyPath where we will upload the file temporarily
            java.nio.file.Path studyPath = null;

            try {
                studyPath = Paths.get(catalogManager.getStudyUri(studyId));
            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse("Upload file", e.getMessage());
            }

            if (filename == null) {
                filename = fileMetaData.getFileName();
            }

            java.nio.file.Path tempFilePath = studyPath.resolve("tmp_" + filename).resolve(filename);
            logger.info("tempFilePath: {}", tempFilePath.toString());
            logger.info("tempParent: {}", tempFilePath.getParent().toString());

            // Create the temporal directory and upload the file
            try {
                if (!Files.exists(tempFilePath.getParent())) {
                    logger.info("createDirectory(): " + tempFilePath.getParent());
                    Files.createDirectory(tempFilePath.getParent());
                }
                logger.info("check dir " + Files.exists(tempFilePath.getParent()));

                // Start uploading the file to the temporal directory
                int read;
                byte[] bytes = new byte[1024];

                // Upload the file to a temporary folder
                OutputStream out = new FileOutputStream(new java.io.File(tempFilePath.toString()));
                while ((read = fileInputStream.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Register the file in catalog
            try {
                String destinationPath;
                // Check if the relativeFilePath is not the root folder
                if (relativeFilePath.length() > 1 && !relativeFilePath.equals("./")) {
                    try {
                        // Create parents directory if necessary
                        catalogManager.getFileManager().createFolder(Long.toString(studyId), Paths.get(relativeFilePath).toString(),
                                null, parents, null, QueryOptions.empty(), sessionId);
                    } catch (CatalogException e) {
                        logger.debug("The folder {} already exists", relativeFilePath);
                    }
                    destinationPath = Paths.get(relativeFilePath).resolve(filename).toString();
                } else {
                    destinationPath = filename;
                }

                logger.debug("Relative path: {}", relativeFilePath);
                logger.debug("Destination path: {}", destinationPath);
                logger.debug("File name {}", filename);
                logger.debug("Bioformat {}", bioformat);
                logger.debug("Fileformat {}", fileFormat);

                // Register the file and move it to the proper directory
                QueryResult<File> queryResult = catalogManager.createFile(studyId, File.Format.valueOf(fileFormat.toUpperCase()),
                        File.Bioformat.valueOf(bioformat.toUpperCase()), destinationPath, tempFilePath.toUri(), description, parents,
                        sessionId);
                File file = new FileMetadataReader(catalogManager).setMetadataInformation(queryResult.first(), null,
                        new QueryOptions(queryOptions), sessionId, false);
                queryResult.setResult(Collections.singletonList(file));

                // Remove the temporal directory
                Files.delete(tempFilePath.getParent());

                return createOkResponse(queryResult);

            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse("Upload file", e.getMessage());
            } catch (IOException e) {
                e.printStackTrace();
                return createErrorResponse("Upload file", e.getMessage());
            }
        } else {
            return createErrorResponse("Upload file", "No file or chunk found");
        }
    }

    @GET
    @Path("/{file}/download")
    @ApiOperation(value = "Download file", position = 5, response = QueryResponse.class)
    public Response download(@ApiParam(value = "File id") @PathParam("file") String fileIdStr,
                             @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                             @QueryParam("study") String studyStr) {
        try {
            DataInputStream stream;
            AbstractManager.MyResourceId resource = fileManager.getId(fileIdStr, studyStr, sessionId);
            catalogManager.getAuthorizationManager().checkFilePermission(resource.getResourceId(), resource.getUser(),
                    FileAclEntry.FilePermissions.DOWNLOAD);

            QueryResult<File> queryResult = catalogManager.getFile(resource.getResourceId(), this.queryOptions, sessionId);
            File file = queryResult.getResult().get(0);
            stream = catalogManager.downloadFile(resource.getResourceId(), sessionId);
            return createOkResponse(stream, MediaType.APPLICATION_OCTET_STREAM_TYPE, file.getName());
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/ranges")
    @ApiOperation(value = "Fetchs alignment files using HTTP Ranges protocol")
    @Produces("text/plain")
    public Response getRanges(@Context HttpHeaders headers,
                              @ApiParam(value = "File id, name or path") @QueryParam("file") String fileIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr) {
        DataInputStream stream = null;
        try {
            AbstractManager.MyResourceId resource = catalogManager.getFileManager().getId(fileIdStr, studyStr, sessionId);
            catalogManager.getAuthorizationManager().checkFilePermission(resource.getResourceId(), resource.getUser(),
                    FileAclEntry.FilePermissions.DOWNLOAD);
            QueryResult<File> queryResult = catalogManager.getFile(resource.getResourceId(), this.queryOptions, sessionId);
            File file = queryResult.getResult().get(0);

            List<String> rangeList = headers.getRequestHeader("range");
            if (rangeList != null) {
                long from;
                long to;
                String[] acceptedRanges = rangeList.get(0).split("=")[1].split("-");
                from = Long.parseLong(acceptedRanges[0]);
                to = Long.parseLong(acceptedRanges[1]);
                int length = (int) (to - from) + 1;
                ByteBuffer buf = ByteBuffer.allocate(length);

                logger.debug("from: {} , to: {}, length:{}", from, to, length);
                StopWatch t = StopWatch.createStarted();

                java.nio.file.Path filePath = Paths.get(file.getUri());
                try (FileChannel fc = (FileChannel.open(filePath, StandardOpenOption.READ))) {
                    fc.position(from);
                    fc.read(buf);
                }

                t.stop();
                logger.debug("Skip {}B and read {}B in {}s", from, length, t.getTime(TimeUnit.MILLISECONDS) / 1000.0);

                return Response.ok(buf.array(), MediaType.APPLICATION_OCTET_STREAM_TYPE)
                        .header("Accept-Ranges", "bytes")
                        .header("Access-Control-Allow-Origin", "*")
                        .header("Access-Control-Allow-Headers", "x-requested-with, content-type, range")
                        .header("Access-Control-Allow-Credentials", "true")
                        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                        .header("Content-Range", "bytes " + from + "-" + to + "/" + file.getSize())
                        .header("Content-length", to - from + 1)
                        .status(Response.Status.PARTIAL_CONTENT).build();

            } else {
                stream = catalogManager.downloadFile(resource.getResourceId(), sessionId);
                return createOkResponse(stream, MediaType.APPLICATION_OCTET_STREAM_TYPE, file.getName());
            }
        } catch (Exception e) {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException ignore) { }
            }
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/content")
    @ApiOperation(value = "Show the content of a file (up to a limit)", position = 6, response = String.class)
    public Response content(@ApiParam(value = "File id") @PathParam("file") String fileIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "start", required = false) @QueryParam("start") @DefaultValue("-1") int start,
                            @ApiParam(value = "limit", required = false) @QueryParam("limit") @DefaultValue("-1") int limit) {
        try {
            AbstractManager.MyResourceId resource = fileManager.getId(fileIdStr, studyStr, sessionId);
            catalogManager.getAuthorizationManager().checkFilePermission(resource.getResourceId(), resource.getUser(),
                    FileAclEntry.FilePermissions.VIEW_CONTENT);

            DataInputStream stream = catalogManager.downloadFile(resource.getResourceId(), start, limit, sessionId);
//             String content = org.apache.commons.io.IOUtils.toString(stream);
            return createOkResponse(stream, MediaType.TEXT_PLAIN_TYPE);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/grep")
    @ApiOperation(value = "Filter lines of the file containing a match of the pattern [TOCHECK]", position = 7, response = String.class)
    public Response downloadGrep(
            @ApiParam(value = "File id") @PathParam("file") String fileIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
            @QueryParam("study") String studyStr,
            @ApiParam(value = "Pattern", required = false) @QueryParam("pattern") @DefaultValue(".*") String pattern,
            @ApiParam(value = "Do a case insensitive search", required = false) @DefaultValue("false") @QueryParam("ignoreCase")
                    Boolean ignoreCase,
            @ApiParam(value = "Return multiple matches", required = false)  @DefaultValue("true") @QueryParam("multi") Boolean multi) {
        try {
            AbstractManager.MyResourceId resource = fileManager.getId(fileIdStr, studyStr, sessionId);
            catalogManager.getAuthorizationManager().checkFilePermission(resource.getResourceId(), resource.getUser(),
                    FileAclEntry.FilePermissions.VIEW_CONTENT);

            DataInputStream stream = catalogManager.grepFile(resource.getResourceId(), pattern, ignoreCase, multi, sessionId);
            return createOkResponse(stream, MediaType.TEXT_PLAIN_TYPE);
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
        QueryResult<File> fileQueryResult;
        InputStream streamBody = null;
        try {
            AbstractManager.MyResourceId resource = fileManager.getId(fileStr, studyStr, sessionId);
            catalogManager.getAuthorizationManager().checkFilePermission(resource.getResourceId(), resource.getUser(),
                    FileAclEntry.FilePermissions.WRITE);

            /** Obtain file uri **/
            File file = catalogManager.getFile(resource.getResourceId(), sessionId).getResult().get(0);
            URI fileUri = catalogManager.getFileUri(file);
            System.out.println("getUri: " + fileUri.getPath());

            /** Set header **/
            stream = catalogManager.downloadFile(resource.getResourceId(), sessionId);
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
        QueryResult<File> results;
        try {
            long folderId = catalogManager.getFileId(folderStr, studyStr, sessionId);
            results = catalogManager.getAllFilesInFolder(folderId, queryOptions, sessionId);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return createOkResponse(results);
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Multi-study search that allows the user to look for files from from different studies of the same project " +
            "applying filters.", position = 12, response = File[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response search(@ApiParam(value = "Comma separated list of file ids", required = false) @DefaultValue("") @QueryParam("id") String id,
                           @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @QueryParam("studyId")
                                   String studyIdStr,
                           @ApiParam(value = "Study [[user@]project:]{study1,study2|*}  where studies and project can be either the id or"
                                   + " alias.") @QueryParam("study") String studyStr,
                           @ApiParam(value = "Comma separated list of file names") @DefaultValue("") @QueryParam("name") String name,
                           @ApiParam(value = "Comma separated list of paths", required = false) @DefaultValue("") @QueryParam("path") String path,
                           @ApiParam(value = "Available types (FILE, DIRECTORY)", required = false) @DefaultValue("") @QueryParam("type") String type,
                           @ApiParam(value = "Comma separated Bioformat values. For existing Bioformats see files/help", required = false) @DefaultValue("") @QueryParam("bioformat") String bioformat,
                           @ApiParam(value = "Comma separated Format values. For existing Formats see files/help", required = false) @DefaultValue("") @QueryParam("format") String formats,
                           @ApiParam(value = "Status", required = false) @DefaultValue("") @QueryParam("status") String status,
                           @ApiParam(value = "Directory under which we want to look for files or folders", required = false) @DefaultValue("") @QueryParam("directory") String directory,
                           @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
                           @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss)", required = false) @DefaultValue("") @QueryParam("modificationDate") String modificationDate,
                           @ApiParam(value = "Description", required = false) @DefaultValue("") @QueryParam("description") String description,
                           @ApiParam(value = "Size", required = false) @DefaultValue("") @QueryParam("size") Long size,
                           @ApiParam(value = "DEPRECATED: use sample instead", hidden = true) @DefaultValue("") @QueryParam("sampleIds") String sampleIds,
                           @ApiParam(value = "Comma separated list of sample ids", required = false) @DefaultValue("") @QueryParam("sample") String samples,
                           @ApiParam(value = "(DEPRECATED) Job id that created the file(s) or folder(s)", hidden = true) @QueryParam("jobId") String jobIdOld,
                           @ApiParam(value = "Job id that created the file(s) or folder(s)", required = false) @QueryParam("job.id") String jobId,
                           @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                           @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)", required = false) @DefaultValue("")
                           @QueryParam("nattributes") String nattributes,
                           @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }

            if (StringUtils.isNotEmpty(samples)) {
                query.put("sampleIds", samples);
            }

            if (query.containsKey(FileDBAdaptor.QueryParams.NAME.key())
                    && (query.get(FileDBAdaptor.QueryParams.NAME.key()) == null
                    || query.getString(FileDBAdaptor.QueryParams.NAME.key()).isEmpty())) {
                query.remove(FileDBAdaptor.QueryParams.NAME.key());
                logger.debug("Name attribute empty, it's been removed");
            }
            // TODO: jobId is deprecated. Remember to remove this if after next release
            if (query.containsKey("jobId") && !query.containsKey(FileDBAdaptor.QueryParams.JOB_ID.key())) {
                query.put(FileDBAdaptor.QueryParams.JOB_ID.key(), query.get("jobId"));
                query.remove("jobId");
            }
            QueryResult<File> result = fileManager.search(studyStr, query, queryOptions, sessionId);
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
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response list(@ApiParam(value = "Folder id") @PathParam("folder") String folder,
                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                         @QueryParam("study") String studyStr) {
        try {
            AbstractManager.MyResourceId resource = fileManager.getId(folder, studyStr, sessionId);
            QueryResult result = catalogManager.getAllFilesInFolder(resource.getResourceId(), queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{file}/index")
    @ApiOperation(value = "Index variant files [DEPRECATED]", position = 14, notes = "Moved to analysis/[variant|alignment]/{file}/index",
            hidden = true, response = QueryResponse.class)
    public Response index(@ApiParam("Comma separated list of file ids (files or directories)") @PathParam(value = "file") String fileIdStr,
                          @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                          @QueryParam("study") String studyStr,
                          // Study id is not ingested by the analysis index command line. No longer needed.
//                          @ApiParam("Study id") @QueryParam("studyId") String studyId,
                          @ApiParam("Output directory id") @QueryParam("outDir") String outDirStr,
                          @ApiParam("Boolean indicating that only the transform step will be run") @DefaultValue("false")
                          @QueryParam("transform") boolean transform,
                          @ApiParam("Boolean indicating that only the load step will be run") @DefaultValue("false")
                          @QueryParam("load") boolean load,
                          @ApiParam("Comma separated list of fields to be include in the index")
                          @QueryParam("includeExtraFields") String includeExtraFields,
                          @ApiParam("Type of aggregated VCF file: none, basic, EVS or ExAC") @DefaultValue("none")
                          @QueryParam("aggregated") String aggregated,
                          @ApiParam("Calculate indexed variants statistics after the load step") @DefaultValue("false")
                          @QueryParam("calculateStats") boolean calculateStats,
                          @ApiParam("Annotate indexed variants after the load step") @DefaultValue("false")
                          @QueryParam("annotate") boolean annotate,
                          @ApiParam("Overwrite annotations already present in variants") @DefaultValue("false")
                          @QueryParam("overwrite") boolean overwriteAnnotations) {

        Map<String, String> params = new LinkedHashMap<>();
//        addParamIfNotNull(params, "studyId", studyId);
        addParamIfNotNull(params, "outdir", outDirStr);
        addParamIfTrue(params, "transform", transform);
        addParamIfTrue(params, "load", load);
        addParamIfNotNull(params, EXTRA_GENOTYPE_FIELDS.key(), includeExtraFields);
        addParamIfNotNull(params, AGGREGATED_TYPE.key(), aggregated);
        addParamIfTrue(params, CALCULATE_STATS.key(), calculateStats);
        addParamIfTrue(params, ANNOTATE.key(), annotate);
        addParamIfTrue(params, VariantAnnotationManager.OVERWRITE_ANNOTATIONS, overwriteAnnotations);

        Set<String> knownParams = new HashSet<>();
        knownParams.add("outDir");
        knownParams.add("transform");
        knownParams.add("load");
        knownParams.add("includeExtraFields");
        knownParams.add("aggregated");
        knownParams.add("calculateStats");
        knownParams.add("annotate");
        knownParams.add("overwrite");
        knownParams.add("sid");
        knownParams.add("include");
        knownParams.add("exclude");

        // Add other params
        query.forEach((key, value) -> {
            if (!knownParams.contains(key)) {
                if (value != null) {
                    params.put(key, value.toString());
                }
            }
        });

        logger.info("ObjectMap: {}", params);

        try {
            AbstractManager.MyResourceIds resource = fileManager.getIds(fileIdStr, studyStr, sessionId);
            QueryResult queryResult = fileManager.index(StringUtils.join(resource.getResourceIds(), ","),
                    Long.toString(resource.getStudyId()), "VCF", params, sessionId);
            return createOkResponse(queryResult);
        } catch(Exception e) {
            return createErrorResponse(e);
        }


//        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
//
//        try {
//            long outDirId = catalogManager.getFileId(outDirStr, sessionId);
//            long fileId = catalogManager.getFileId(fileIdStr, sessionId);
//            if(outDirId < 0) {
//                outDirId = catalogManager.getFileParent(fileId, null, sessionId).first().getId();
//            }
//            // TODO: Change it to query
//            queryOptions.add(VariantStorageEngine.Options.CALCULATE_STATS.key(), calculateStats);
//            queryOptions.add(VariantStorageEngine.Options.ANNOTATE.key(), annotate);
//            QueryResult<Job> queryResult = analysisFileIndexer.index(fileId, outDirId, sessionId, new QueryOptions(queryOptions));
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
    }

    @GET
    @Path("/{folder}/tree")
    @ApiOperation(value = "Obtain a tree view of the files and folders within a folder", position = 15, response = FileTree[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "[TO BE IMPLEMENTED] Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
    })
    public Response treeView(@ApiParam(value = "Folder id or path. Paths must be separated by : instead of /") @DefaultValue(":")
                             @PathParam ("folder") String folderId,
                             @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                             @QueryParam("study") String studyStr,
                             @ApiParam(value = "Maximum depth to get files from") @DefaultValue("5") @QueryParam("maxDepth") int maxDepth) {
        try {
            query.remove("maxDepth");
            QueryResult result = fileManager
                    .getTree(folderId.replace(":", "/"), studyStr, query, queryOptions, maxDepth, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{file}/fetch")
    @ApiOperation(value = "File fetch [DEPRECATED]", notes = "DEPRECATED. Use .../files/{fileId}/[variants|alignments] or "
            + ".../studies/{studyId}/[variants|alignments] instead", hidden = true, position = 15)
    public Response fetch(@PathParam(value = "file") @DefaultValue("") String fileIds,
                          @ApiParam(value = "region", allowMultiple = true, required = true) @DefaultValue("") @QueryParam("region") String region,
                          @ApiParam(value = "view_as_pairs", required = false) @DefaultValue("false") @QueryParam("view_as_pairs") boolean view_as_pairs,
                          @ApiParam(value = "include_coverage", required = false) @DefaultValue("true") @QueryParam("include_coverage") boolean include_coverage,
                          @ApiParam(value = "process_differences", required = false) @DefaultValue("true") @QueryParam("process_differences") boolean process_differences,
                          @ApiParam(value = "histogram", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                          @ApiParam(value = "GroupBy: [ct, gene, ensemblGene]", required = false) @DefaultValue("") @QueryParam("groupBy") String groupBy,
                          @ApiParam(value = "variantSource", required = false) @DefaultValue("false") @QueryParam("variantSource") boolean variantSource,
                          @ApiParam(value = "interval", required = false) @DefaultValue("2000") @QueryParam("interval") int interval) {
        List<Region> regions = new LinkedList<>();
        String[] splitFileId = fileIds.split(",");
        List<Object> results = new LinkedList<>();
        for (String r : region.split(",")) {
            regions.add(new Region(r));
        }

        for (String fileId : splitFileId) {
            long fileIdNum;
            File file;
            URI fileUri;

            try {
                fileIdNum = catalogManager.getFileId(fileId, null, sessionId);
                QueryResult<File> queryResult = catalogManager.getFile(fileIdNum, sessionId);
                file = queryResult.getResult().get(0);
                fileUri = catalogManager.getFileUri(file);
            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse(e);
            }

//            if (!file.getType().equals(File.Type.INDEX)) {
            if (file.getIndex() == null || !file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
                return createErrorResponse("", "File {id:" + file.getId() + " name:'" + file.getName() + "'} " +
                        " is not an indexed file.");
            }
//            List<Index> indices = file.getIndices();
//            Index index = null;
//            for (Index i : indices) {
//                if (i.getStorageEngine().equals(backend)) {
//                    index = i;
//                }
//            }
            ObjectMap indexAttributes = new ObjectMap(file.getIndex().getAttributes());
            DataStore dataStore = null;
            try {
                dataStore = StorageOperation.getDataStore(catalogManager, catalogManager.getStudyIdByFileId(file.getId()),
                        file.getBioformat(), sessionId);
            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse(e);
            }
            String storageEngine = dataStore.getStorageEngine();
            String dbName = dataStore.getDbName();
//            QueryResult result;
            QueryResult result;
            switch (file.getBioformat()) {
                case ALIGNMENT: {
                    //TODO: getChunkSize from file.index.attributes?  use to be 200
                    int chunkSize = indexAttributes.getInt("coverageChunkSize", 200);
                    QueryOptions queryOptions = new QueryOptions();
                    queryOptions.put(AlignmentDBAdaptor.QO_FILE_ID, Long.toString(fileIdNum));
                    queryOptions.put(AlignmentDBAdaptor.QO_BAM_PATH, fileUri.getPath());     //TODO: Make uri-compatible
                    queryOptions.put(AlignmentDBAdaptor.QO_VIEW_AS_PAIRS, view_as_pairs);
                    queryOptions.put(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, include_coverage);
                    queryOptions.put(AlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, process_differences);
                    queryOptions.put(AlignmentDBAdaptor.QO_INTERVAL_SIZE, interval);
                    queryOptions.put(AlignmentDBAdaptor.QO_HISTOGRAM, histogram);
                    queryOptions.put(AlignmentDBAdaptor.QO_COVERAGE_CHUNK_SIZE, chunkSize);

                    if (indexAttributes.containsKey("baiFileId")) {
                        File baiFile = null;
                        try {
                            baiFile = catalogManager.getFile(indexAttributes.getInt("baiFileId"), sessionId).getResult().get(0);
                            URI baiUri = catalogManager.getFileUri(baiFile);
                            queryOptions.put(AlignmentDBAdaptor.QO_BAI_PATH, baiUri.getPath());  //TODO: Make uri-compatible
                        } catch (CatalogException e) {
                            e.printStackTrace();
                            logger.error("Can't obtain bai file for file " + fileIdNum, e);
                        }
                    }

                    AlignmentDBAdaptor dbAdaptor;
                    try {
                        AlignmentStorageEngine alignmentStorageManager = storageEngineFactory.getAlignmentStorageEngine(storageEngine);
                        dbAdaptor = alignmentStorageManager.getDBAdaptor(dbName);
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageEngineException e) {
                        return createErrorResponse(e);
                    }
//                    QueryResult alignmentsByRegion;
                    QueryResult alignmentsByRegion;
                    if (histogram) {
                        if (regions.size() != 1) {
                            return createErrorResponse("", "Histogram fetch only accepts one region.");
                        }
                        alignmentsByRegion = dbAdaptor.getAllIntervalFrequencies(regions.get(0), new QueryOptions(queryOptions));
                    } else {
                        alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(regions, new QueryOptions(queryOptions));
                    }
                    result = alignmentsByRegion;
                    break;
                }

                case VARIANT: {
                    String warningMsg = null;
                    Query query = VariantStorageManager.getVariantQuery(queryOptions);
                    query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), region);

//                    for (Map.Entry<String, List<String>> entry : params.entrySet()) {
//                        List<String> values = entry.getValue();
//                        String csv = values.get(0);
//                        for (int i = 1; i < values.size(); i++) {
//                            csv += "," + values.get(i);
//                        }
//                        queryOptions.add(entry.getKey(), csv);
//                    }
//                    queryOptions.put("files", Arrays.asList(Integer.toString(fileIdNum)));
                    query.put(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileIdNum);

                    if (params.containsKey("fileId")) {
                        warningMsg = "Do not use param \"fileI\". Use \"" + VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key() + "\" instead";
                        if (params.get("fileId").get(0).isEmpty()) {
                            query.put(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), fileId);
                        } else {
                            List<String> files = params.get("fileId");
                            query.put(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), files);
                        }
                    }

                    VariantDBAdaptor dbAdaptor;
                    try {
                        dbAdaptor = storageEngineFactory.getVariantStorageEngine(storageEngine).getDBAdaptor(dbName);
//                        dbAdaptor = new CatalogVariantDBAdaptor(catalogManager, dbAdaptor);
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageEngineException e) {
                        return createErrorResponse(e);
                    }
//                    QueryResult queryResult;
                    QueryResult queryResult;
                    if (histogram) {
                        queryOptions.put("interval", interval);
                        queryResult = dbAdaptor.get(new Query(query), new QueryOptions(queryOptions));
//                    } else if (variantSource) {
//                        queryOptions.put("fileId", Integer.toString(fileIdNum));
//                        queryResult = dbAdaptor.getVariantSourceDBAdaptor().getAllSources(queryOptions);
                    } else if (!groupBy.isEmpty()) {
                        queryResult = dbAdaptor.groupBy(new Query(query), groupBy, new QueryOptions(queryOptions));
                    } else {
                        //With merge = true, will return only one result.
//                        queryOptions.put("merge", true);
//                        queryResult = dbAdaptor.getAllVariantsByRegionList(regions, queryOptions).get(0);
                        queryResult = dbAdaptor.get(new Query(query), new QueryOptions(queryOptions));
                    }
                    result = queryResult;
                    if (warningMsg != null) {
                        result.setWarningMsg(result.getWarningMsg() == null ? warningMsg : (result.getWarningMsg() + warningMsg));
                    }
                    break;

                }
                default:
                    return createErrorResponse("", "Unknown bioformat '" + file.getBioformat() + '\'');
            }

            result.setId(Long.toString(fileIdNum));
            System.out.println("result = " + result);
            results.add(result);
        }
        System.out.println("results = " + results);
        return createOkResponse(results);
    }

    @Deprecated
    @GET
    @Path("/{file}/variants")
    @ApiOperation(value = "Fetch variants from a VCF/gVCF file [DEPRECATED]", position = 15,
            notes = "Moved to analysis/variants/query", hidden = true, response = QueryResponse.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getVariants(@ApiParam(value = "", required = true) @PathParam("file") String fileIdCsv,
                                @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                                @QueryParam("study") String studyStr,
                                @ApiParam(value = "List of variant ids") @QueryParam("ids") String ids,
                                @ApiParam(value = "List of regions: {chr}:{start}-{end}") @QueryParam("region") String region,
                                @ApiParam(value = "List of chromosomes") @QueryParam("chromosome") String chromosome,
                                @ApiParam(value = "List of genes") @QueryParam("gene") String gene,
                                @ApiParam(value = "Variant type: [SNV, MNV, INDEL, SV, CNV]") @QueryParam("type") String type,
                                @ApiParam(value = "Reference allele") @QueryParam("reference") String reference,
                                @ApiParam(value = "Main alternate allele") @QueryParam("alternate") String alternate,
//                                @ApiParam(value = "") @QueryParam("studies") String studies,
                                @ApiParam(value = "List of studies to be returned") @QueryParam("returnedStudies") String returnedStudies,
                                @ApiParam(value = "List of samples to be returned") @QueryParam("returnedSamples") String returnedSamples,
                                @ApiParam(value = "List of files to be returned.") @QueryParam("returnedFiles") String returnedFiles,
                                @ApiParam(value = "Variants in specific files") @QueryParam("files") String files,
                                @ApiParam(value = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("maf") String maf,
                                @ApiParam(value = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("mgf") String mgf,
                                @ApiParam(value = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingAlleles") String missingAlleles,
                                @ApiParam(value = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingGenotypes") String missingGenotypes,
                                @ApiParam(value = "Specify if the variant annotation must exists.") @QueryParam("annotationExists") boolean annotationExists,
                                @ApiParam(value = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1") @QueryParam("genotype") String genotype,
                                @ApiParam(value = "Consequence type SO term list. e.g. missense_variant,stop_lost or SO:0001583,SO:0001578") @QueryParam("annot-ct") String annot_ct,
                                @ApiParam(value = "XRef") @QueryParam("annot-xref") String annot_xref,
                                @ApiParam(value = "Biotype") @QueryParam("annot-biotype") String annot_biotype,
                                @ApiParam(value = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. <=0.9 , =benign") @QueryParam("polyphen") String polyphen,
                                @ApiParam(value = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. >0.1 , ~=tolerant") @QueryParam("sift") String sift,
//                                @ApiParam(value = "") @QueryParam("protein_substitution") String protein_substitution,
                                @ApiParam(value = "Conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1") @QueryParam("conservation") String conservation,
                                @ApiParam(value = "Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("annot-population-maf") String annotPopulationMaf,
                                @ApiParam(value = "Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("alternate_frequency") String alternate_frequency,
                                @ApiParam(value = "Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("reference_frequency") String reference_frequency,
                                @ApiParam(value = "List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno") @QueryParam("annot-transcription-flags") String transcriptionFlags,
                                @ApiParam(value = "List of gene trait association id. e.g. \"umls:C0007222\" , \"OMIM:269600\"") @QueryParam("annot-gene-trait-id") String geneTraitId,
                                @ApiParam(value = "List of gene trait association names. e.g. \"Cardiovascular Diseases\"") @QueryParam("annot-gene-trait-name") String geneTraitName,
                                @ApiParam(value = "List of HPO terms. e.g. \"HP:0000545\"") @QueryParam("annot-hpo") String hpo,
                                @ApiParam(value = "List of GO (Genome Ontology) terms. e.g. \"GO:0002020\"") @QueryParam("annot-go") String go,
                                @ApiParam(value = "List of tissues of interest. e.g. \"tongue\"") @QueryParam("annot-expression") String expression,
                                @ApiParam(value = "List of protein variant annotation keywords") @QueryParam("annot-protein-keywords") String proteinKeyword,
                                @ApiParam(value = "List of drug names") @QueryParam("annot-drug") String drug,
                                @ApiParam(value = "Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3") @QueryParam("annot-functional-score") String functional,

                                @ApiParam(value = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]") @QueryParam("unknownGenotype") String unknownGenotype,
//                                @ApiParam(value = "Limit the number of returned variants. Max value: " + VariantFetcher.LIMIT_MAX) @DefaultValue(""+VariantFetcher.LIMIT_DEFAULT) @QueryParam("limit") int limit,
//                                @ApiParam(value = "Skip some number of variants.") @QueryParam("skip") int skip,
                                @ApiParam(value = "Returns the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.", required = false) @QueryParam("samplesMetadata") boolean samplesMetadata,
                                @ApiParam(value = "Count results", required = false) @QueryParam("count") boolean count,
                                @ApiParam(value = "Sort the results", required = false) @QueryParam("sort") boolean sort,
                                @ApiParam(value = "Group variants by: [ct, gene, ensemblGene]", required = false) @DefaultValue("") @QueryParam("groupBy") String groupBy,
                                @ApiParam(value = "Calculate histogram. Requires one region.", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                                @ApiParam(value = "Histogram interval size", required = false) @DefaultValue("2000") @QueryParam("interval") int interval,
                                @ApiParam(value = "Merge results", required = false) @DefaultValue("false") @QueryParam("merge") boolean merge) {


        List<QueryResult> queryResults = new LinkedList<>();
        try {
            AbstractManager.MyResourceIds resource = fileManager.getIds(fileIdCsv, studyStr, sessionId);
//            String[] splitFileId = fileIdCsv.split(",");
            for (long fileId : resource.getResourceIds()) {
                QueryResult queryResult;
                // Get all query options
                QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
                Query query = VariantStorageManager.getVariantQuery(queryOptions);
                query.put(VariantDBAdaptor.VariantQueryParams.FILES.key(), fileId);
                if (count) {
                    queryResult = variantManager.count(query, sessionId);
                } else if (histogram) {
                    queryResult = variantManager.getFrequency(query, interval, sessionId);
                } else if (StringUtils.isNotEmpty(groupBy)) {
                    queryResult = variantManager.groupBy(groupBy, query, queryOptions, sessionId);
                } else {
                    queryResult = variantManager.get(query, queryOptions, sessionId);
                }
                queryResults.add(queryResult);
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return createOkResponse(queryResults);
    }

    private ObjectMap getResumeFileJSON(java.nio.file.Path folderPath) throws IOException {
        ObjectMap objectMap = new ObjectMap();

        if (Files.exists(folderPath)) {
            DirectoryStream<java.nio.file.Path> folderStream = Files.newDirectoryStream(folderPath, "*_partial");
            for (java.nio.file.Path partPath : folderStream) {
                String[] nameSplit = partPath.getFileName().toString().split("_");
                ObjectMap chunkInfo = new ObjectMap();
                chunkInfo.put("size", Integer.parseInt(nameSplit[1]));
                objectMap.put(nameSplit[0], chunkInfo);
            }
        }
        return objectMap;
    }

    private List<java.nio.file.Path> getSortedChunkList(java.nio.file.Path folderPath) throws IOException {
        List<java.nio.file.Path> files = new ArrayList<>();
        DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(folderPath, "*_partial");
        for (java.nio.file.Path p : stream) {
            logger.info("adding to ArrayList: " + p.getFileName());
            files.add(p);
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

    @GET
    @Path("/{file}/update")
    @ApiOperation(value = "Update fields of a file [WARNING]", position = 16, response = File.class,
            notes = "WARNING: the usage of this web service is discouraged, please use the POST version instead. Be aware that this is web "
                    + "service is not tested and this can be deprecated in a future version.")
    public Response update(@ApiParam(value = "File id") @PathParam(value = "file") String fileIdStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                           @QueryParam("study") String studyStr,
                           @ApiParam(value = "File name", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "Format of the file (VCF, BCF, GVCF, SAM, BAM, BAI...UNKNOWN)", required = false) @QueryParam("format") String format,
                           @ApiParam(value = "Bioformat of the file (VARIANT, ALIGNMENT, SEQUENCE, PEDIGREE...NONE)", required = false) @QueryParam("bioformat") String bioformat,
                           @ApiParam(value = "Description of the file", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "Attributes", required = false) @QueryParam("attributes") String attributes,
                           @ApiParam(value = "Stats", required = false) @QueryParam("stats") String stats,
                           @ApiParam(value = "Sample ids", required = false) @QueryParam("sampleIds") String sampleIds,
                           @ApiParam(value = "(DEPRECATED) Job id", hidden = true) @QueryParam("jobId") String jobIdOld,
                           @ApiParam(value = "Job id", required = false) @QueryParam("job.id") String jobId) {
        try {
            /*ObjectMap parameters = new ObjectMap();
            QueryOptions qOptions = new QueryOptions();
            parseQueryParams(params, CatalogFileDBAdaptor.QueryParams::getParam, parameters, qOptions);*/
            ObjectMap params = new ObjectMap(query);
            // TODO: jobId is deprecated. Remember to remove this if after next release
            if (params.containsKey("jobId") && !params.containsKey(FileDBAdaptor.QueryParams.JOB_ID.key())) {
                params.put(FileDBAdaptor.QueryParams.JOB_ID.key(), params.get("jobId"));
                params.remove("jobId");
            }
            params.remove(FileDBAdaptor.QueryParams.STUDY.key());
//            params.putIfNotEmpty(FileDBAdaptor.QueryParams.NAME.key(), name);
//            params.putIfNotEmpty(FileDBAdaptor.QueryParams.FORMAT.key(), format);
//            params.putIfNotEmpty(FileDBAdaptor.QueryParams.BIOFORMAT.key(), bioformat);
//            params.putIfNotEmpty(FileDBAdaptor.QueryParams.DESCRIPTION.key(), description);
//            params.putIfNotEmpty(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
//            params.putIfNotEmpty(FileDBAdaptor.QueryParams.STATS.key(), stats);
//            params.putIfNotEmpty(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleIds);
//            params.putIfNotEmpty(FileDBAdaptor.QueryParams.JOB_ID.key(), jobIdOld);
//            params.putIfNotEmpty(FileDBAdaptor.QueryParams.JOB_ID.key(), jobId);
            AbstractManager.MyResourceId resource = fileManager.getId(fileIdStr, studyStr, sessionId);
            QueryResult queryResult = fileManager.update(resource.getResourceId(), params, queryOptions, sessionId);
            queryResult.setId("Update file");
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class UpdateFile {
        public String name;
        public File.Format format;
        public File.Bioformat bioformat;
        //        public String path;
//        public String ownerId;
//        public String creationDate;
//        public String modificationDate;
        public String description;
        //        public Long size;
//        public int experimentId;
        public String sampleIds;
        public Long jobId;
        public Map<String, Object> stats;
        public Map<String, Object> attributes;
    }

    @POST
    @Path("/{file}/update")
    @ApiOperation(value = "Modify file", position = 16, response = File.class)
    public Response updatePOST(@ApiParam(value = "File id") @PathParam(value = "file") String fileIdStr,
                               @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                               @QueryParam("study") String studyStr,
                               @ApiParam(name = "params", value = "Parameters to modify", required = true) ObjectMap params) {
        try {
            AbstractManager.MyResourceId resource = fileManager.getId(fileIdStr, studyStr, sessionId);

            ObjectMap map = new ObjectMap(jsonObjectMapper.writeValueAsString(params));
            // TODO: jobId is deprecated. Remember to remove this if after next release
            if (map.get("jobId") != null) {
                map.put(FileDBAdaptor.QueryParams.JOB_ID.key(), map.get("jobId"));
                map.remove("jobId");
            }

            QueryResult<File> queryResult = fileManager.update(resource.getResourceId(), map, queryOptions, sessionId);
            queryResult.setId("Update file");
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/link")
    @ApiOperation(value = "Link an external file into catalog.", hidden = true, position = 19, response = QueryResponse.class)
    public Response link(@ApiParam(value = "Uri of the file", required = true) @QueryParam("uri") String uriStr,
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
            List<String> uriList = Arrays.asList(uriStr.split(","));

            List<QueryResult<File>> queryResultList = new ArrayList<>();
            logger.info("path: {}", path);
            if (uriList.size() == 1) {
                // If it is just one uri to be linked, it will return an error response if there is some kind of error.
                URI myUri = UriUtils.createUri(uriList.get(0));
                queryResultList.add(catalogManager.link(myUri, path, studyStr, objectMap, sessionId));
            } else {
                for (String uri : uriList) {
                    logger.info("uri: {}", uri);
                    try {
                        URI myUri = UriUtils.createUri(uri);
                        queryResultList.add(catalogManager.link(myUri, path, studyStr, objectMap, sessionId));
                    } catch (URISyntaxException | CatalogException | IOException e) {
                        queryResultList.add(new QueryResult<>("Link file", -1, 0, 0, "", e.getMessage(), Collections.emptyList()));
                    }
                }
            }
            return createOkResponse(queryResultList);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/unlink")
    @ApiOperation(value = "Unlink an external file from catalog.", position = 20, response = QueryResponse.class)
    public Response unlink(@ApiParam(value = "File id", required = true) @QueryParam("fileId") String fileIdStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                           @QueryParam("study") String studyStr) throws CatalogException {
        try {
            QueryResult<File> queryResult = catalogManager.getFileManager().unlink(fileIdStr, studyStr, sessionId);
            return createOkResponse(new QueryResult<>("unlink", 0, 1, 1, null, null, queryResult.getResult()));
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
                           @ApiParam(value = "New URI" ,required = true) @QueryParam("uri") String uriStr,
                           @ApiParam(value = "Do calculate checksum for new files", required = false) @DefaultValue("false")
                           @QueryParam("calculateChecksum") boolean calculateChecksum ) {
        try {
            URI uri = UriUtils.createUri(uriStr);
            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(uri);

            if (!ioManager.exists(uri)) {
                throw new CatalogIOException("File " + uri + " does not exist");
            }

            AbstractManager.MyResourceId resource = fileManager.getId(fileIdStr, studyStr, sessionId);
            File file = catalogManager.getFile(resource.getResourceId(), sessionId).first();

            new CatalogFileUtils(catalogManager).link(file, calculateChecksum, uri, false, true, sessionId);
            file = catalogManager.getFile(file.getId(), queryOptions, sessionId).first();
            file = FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, new QueryOptions(queryOptions), sessionId,
                    false);

            return createOkResponse(new QueryResult<>("relink", 0, 1, 1, null, null, Collections.singletonList(file)));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/refresh")
    @ApiOperation(value = "Refresh metadata from the selected file or folder. Return updated files.", position = 22,
            response = QueryResponse.class)
    public Response refresh(@ApiParam(value = "File id") @PathParam(value = "file") String fileIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr) {
        try {
            AbstractManager.MyResourceId resource = fileManager.getId(fileIdStr, studyStr, sessionId);

            File file = catalogManager.getFile(resource.getResourceId(), sessionId).first();

            List<File> files;
            CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);
            FileMetadataReader fileMetadataReader = FileMetadataReader.get(catalogManager);
            if (file.getType() == File.Type.FILE) {
                File file1 = catalogFileUtils.checkFile(file, false, sessionId);
                file1 = fileMetadataReader.setMetadataInformation(file1, null, new QueryOptions(queryOptions), sessionId, false);
                if (file == file1) {    //If the file is the same, it was not modified. Only return modified files.
                    files = Collections.emptyList();
                } else {
                    files = Collections.singletonList(file);
                }
            } else {
                List<File> result = catalogManager.getAllFilesInFolder(file.getId(), null, sessionId).getResult();
                files = new ArrayList<>(result.size());
                for (File f : result) {
                    File file1 = fileMetadataReader.setMetadataInformation(f, null, new QueryOptions(queryOptions), sessionId, false);
                    if (f != file1) {    //Add only modified files.
                        files.add(file1);
                    }
                }
            }
            return createOkResponse(new QueryResult<>("refresh", 0, files.size(), files.size(), null, null, files));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/delete")
    @ApiOperation(value = "Delete file", position = 23, response = QueryResponse.class)
    public Response deleteGET(@ApiParam(value = "File id") @PathParam(value = "file") String fileIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr,
                              @ApiParam(value = "Delete files and folders from disk (only applicable for linked files/folders)",
                                      required = false) @DefaultValue("false") @QueryParam("deleteExternal") boolean deleteExternal,
                              @ApiParam(value="Skip trash and delete the files/folders from disk directly (CANNOT BE RECOVERED)",
                                      required = false) @DefaultValue("false") @QueryParam("skipTrash") boolean skipTrash) {
        try {
//            QueryOptions qOptions = new QueryOptions(queryOptions)
            ObjectMap params = new ObjectMap()
                    .append(FileManager.DELETE_EXTERNAL_FILES, deleteExternal)
                    .append(FileManager.SKIP_TRASH, skipTrash);
            List<QueryResult<File>> result = fileManager.delete(fileIdStr, studyStr, params, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group files by several fields", position = 24, response = QueryResponse.class)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("")
                            @QueryParam("fields") String fields,
                            @ApiParam(value = "(DEPRECATED) Use study instead", hidden = true) @DefaultValue("") @QueryParam("studyId")
                                    String studyIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr,
                            @ApiParam(value = "Comma separated list of ids.", required = false) @DefaultValue("") @QueryParam("id")
                                    String ids,
                            @ApiParam(value = "Comma separated list of names.", required = false) @DefaultValue("") @QueryParam("name")
                                    String names,
                            @ApiParam(value = "path", required = false) @DefaultValue("") @QueryParam("path") String path,
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
                            @ApiParam(value = "modificationDate", required = false) @DefaultValue("") @QueryParam("modificationDate")
                                    String modificationDate,
                            @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description")
                                    String description,
                            @ApiParam(value = "size", required = false) @DefaultValue("") @QueryParam("size") Long size,
                            @ApiParam(value = "Comma separated sampleIds", required = false) @DefaultValue("") @QueryParam("sampleIds")
                                    String sampleIds,
                            @ApiParam(value = "(DEPRECATED) Job id", hidden = true) @QueryParam("jobId") String jobIdOld,
                            @ApiParam(value = "Job id", required = false) @QueryParam("job.id") String jobId,
                            @ApiParam(value = "attributes", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                            @ApiParam(value = "numerical attributes", required = false) @DefaultValue("") @QueryParam("nattributes")
                                    String nattributes) {
        try {
            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            QueryResult result = fileManager.groupBy(studyStr, query, queryOptions, fields, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{files}/acl")
    @ApiOperation(value = "Return the acl defined for the file or folder", position = 18, response = QueryResponse.class)
    public Response getAcls(@ApiParam(value = "Comma separated list of file ids", required = true) @PathParam("files") String fileIdStr,
                            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                            @QueryParam("study") String studyStr) {
        try {
            return createOkResponse(catalogManager.getAllFileAcls(fileIdStr, studyStr, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{files}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of users or groups", hidden = true, position = 19,
            response = QueryResponse.class)
    public Response createAcl(@ApiParam(value = "Comma separated list of file ids", required = true) @PathParam("files")
                                      String fileIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr,
                              @ApiParam(value = "Comma separated list of permissions that will be granted to the member list",
                                      required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                              @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                                      required = true) @DefaultValue("") @QueryParam("members") String members) {
        try {
            return createOkResponse(catalogManager.getFileManager().createAcls(fileIdStr, studyStr, members, permissions,
                    sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{files}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of users or groups", response = QueryResponse.class)
    public Response createAclPOST(
            @ApiParam(value = "Comma separated list of file ids", required = true) @PathParam("files") String fileIdStr,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing the parameters defined in GET. Mandatory keys: 'members'", required = true)
                    StudyWSServer.CreateAclCommands params) {
        try {
            return createOkResponse(catalogManager.getFileManager().createAcls(fileIdStr, studyStr, params.members, params.permissions,
                    sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/acl/{memberId}/info")
    @ApiOperation(value = "Return the permissions granted for the user or group", position = 20, response = QueryResponse.class)
    public Response getAcl(@ApiParam(value = "File id", required = true) @PathParam("file") String fileIdStr,
                           @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                           @QueryParam("study") String studyStr,
                           @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.getFileAcl(fileIdStr, studyStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{file}/acl/{memberId}/update")
    @ApiOperation(value = "Update the permissions granted for the user or group", hidden = true, position = 21,
            response = QueryResponse.class)
    public Response updateAcl(@ApiParam(value = "File id", required = true) @PathParam("file") String fileIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr,
                              @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false)
                              @QueryParam("add") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false)
                              @QueryParam("remove") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false)
                              @QueryParam("set") String setPermissions) {
        try {
            return createOkResponse(catalogManager.getFileManager().updateAcls(fileIdStr, studyStr, memberId, addPermissions,
                    removePermissions, setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{file}/acl/{memberId}/update")
    @ApiOperation(value = "Update the permissions granted for the user or group", position = 21, response = QueryResponse.class)
    public Response updateAclPOST(
            @ApiParam(value = "File id", required = true) @PathParam("file") String fileIdStr,
            @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study")
                    String studyStr,
            @ApiParam(value="JSON containing one of the keys 'add', 'set' or 'remove'", required = true)
                    StudyWSServer.MemberAclUpdate params) {
        try {
            return createOkResponse(catalogManager.getFileManager().updateAcls(fileIdStr, studyStr, memberId, params.add, params.remove,
                    params.set, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{files}/acl/{memberIds}/delete")
    @ApiOperation(value = "Remove all the permissions granted for the user or group", position = 22,
            response = QueryResponse.class)
    public Response deleteAcl(@ApiParam(value = "Comma separated list of file ids", required = true) @PathParam("files")
                                      String fileIdsStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr,
                              @ApiParam(value = "Comma separated list of members", required = true) @PathParam("memberIds") String
                                          members) {
        try {
            return createOkResponse(catalogManager.getFileManager().removeFileAcls(fileIdsStr, studyStr, members, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{folder}/scan")
    @ApiOperation(value = "Scans a folder", position = 6)
    public Response scan(@ApiParam(value = "Folder id") @PathParam("folder") String folderIdStr,
                         @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                         @QueryParam("study") String studyStr,
                         @ApiParam(value = "calculateChecksum") @QueryParam("calculateChecksum") @DefaultValue("false")
                                 boolean calculateChecksum) {
        try {
            AbstractManager.MyResourceId resource = fileManager.getId(folderIdStr, studyStr, sessionId);

            File directory = catalogManager.getFile(resource.getResourceId(), sessionId).first();
            List<File> scan = new FileScanner(catalogManager)
                    .scan(directory, null, FileScanner.FileScannerPolicy.REPLACE, calculateChecksum, false, sessionId);
            return createOkResponse(new QueryResult<>("Scan", 0, scan.size(), scan.size(), "", "", scan));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    public static String convertPath(String path, String sessionId) throws CatalogException {
        return convertPath(path, sessionId, catalogManager);
    }

    // Visible only for testing purposes
    @Deprecated
    static String convertPath(String path, String sessionId, CatalogManager catalogManager) throws CatalogException {
        if (path == null) {
            return null;
        }
        if (path.contains("/") || !path.contains(":")) {
            return path;
        }
        // Path contains :
        if (path.startsWith(":")) {
            return path.replace(":", "/");
        } else {
            // Get the user id
            String userId = catalogManager.getUserManager().getId(sessionId);

            // Get only the first part to check if it corresponds with user@project
            int position = path.indexOf(":");
            String project = path.substring(0, position);
            // Check if it corresponds with a project
            long id = catalogManager.getProjectManager().getId(userId, project);
            if (id <= 0) {
                // Was it a study user@study:filePath/...?
                id = catalogManager.getStudyManager().getId(userId, project);
                if (id <= 0) {
                    // Then it must be user@filePath/...
                    return path.replace(":", "/");
                } else {
                    // It must be user@study:filePath/...
                    return project + ":" + path.substring(position + 1).replace(":", "/");
                }
            } else {
                // Does it have the study as well? user@project:study:filePath/...
                int position2 = path.substring(position + 1).indexOf(":");
                if (position2 == -1) {
                    throw new CatalogException("No file was found in " + path);
                } else {
                    // We should have something like user@project:study
                    String study = project + path.substring(position).substring(0, position2 + 1);
                    // Check if the study exists
                    id = catalogManager.getStudyManager().getId(userId, study);
                    if (id <= 0) {
                        // Then it must be user@project:filePath/...
                        throw new CatalogException("Passing files with this structure user@project:filePath/... is not supported.");
                    } else {
                        // The structure seems to be user@project:study:filePath/...
                        return study + ":" + path.substring(position).substring(position2 + 2).replace(":", "/");
                    }
                }
            }
//            return path.substring(0, position + 1) + path.substring(position + 1).replace(":", "/");
        }
    }

    @Deprecated
    public static List<String> convertPathList(String path, String sessionId) throws CatalogException {
        if (path == null) {
            return Collections.emptyList();
        } else if (path.contains(",")) {
            String[] split = path.split(",");
            List<String> pathList = new ArrayList<>(split.length);
            for (String s : split) {
                pathList.add(convertPath(s, sessionId));
            }
            return pathList;
        } else {
            return Collections.singletonList(convertPath(path, sessionId));
        }
    }

}
