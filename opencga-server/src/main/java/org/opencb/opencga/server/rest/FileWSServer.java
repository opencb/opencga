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

package org.opencb.opencga.server.rest;

import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.analysis.variant.AbstractFileIndexer;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.analysis.storage.variant.VariantFetcher;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;


@Path("/{version}/files")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Files", position = 4, description = "Methods for working with 'files' endpoint")
public class FileWSServer extends OpenCGAWSServer {


    public FileWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException,
            ClassNotFoundException, IllegalAccessException, InstantiationException, VersionException {
        super(uriInfo, httpServletRequest);
//        String alignmentManagerName = properties.getProperty("STORAGE.ALIGNMENT-MANAGER", MONGODB_ALIGNMENT_MANAGER);
//        String alignmentManagerName = MONGODB_ALIGNMENT_MANAGER;
//        String variantManagerName = MONGODB_VARIANT_MANAGER;

//        if (variantStorageManager == null) {
//            variantStorageManager = (VariantStorageManager) Class.forName(variantManagerName).newInstance();
//        }
//        if(alignmentStorageManager == null) {
//            alignmentStorageManager = (AlignmentStorageManager) Class.forName(alignmentManagerName).newInstance();
////            try {
////                alignmentStorageManager = (AlignmentStorageManager) Class.forName(alignmentManagerName).newInstance();
////            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
////                e.printStackTrace();
////                logger.error(e.getMessage(), e);
////            }
//            //dbAdaptor = alignmentStorageManager.getDBAdaptor(null);
//        }
    }

    @Deprecated
    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create file with POST method", position = 1, response = File[].class, notes =
            "This method only creates the file entry in Catalog.<br>" +
                    "Will accept (but not yet): acl.<br>" +
                    "<ul>" +
                    "<il><b>id</b> parameter will be ignored.<br></il>" +
                    "<il><b>type</b> accepted values: [<b>'DIRECTORY', 'FILE', 'INDEX'</b>].<br></il>" +
                    "<il><b>format</b> accepted values: [<b>'PLAIN', 'GZIP', 'EXECUTABLE', 'IMAGE'</b>].<br></il>" +
                    "<il><b>bioformat</b> accepted values: [<b>'VARIANT', 'ALIGNMENT', 'SEQUENCE', 'NONE'</b>].<br></il>" +
                    "<il><b>status</b> accepted values (admin required): [<b>'INDEXING', 'STAGE', 'UPLOADED', 'READY', 'TRASHED', 'TRASHED'</b>].<br></il>" +
                    "<il><b>creatorId</b> should be the same as que sessionId user (unless you are admin) </il>" +
                    "<ul>")
    public Response createFilePOST(@ApiParam(value = "Study id", required = true) @QueryParam("studyId") String studyIdStr,
                                   @ApiParam(value = "Array of files", required = true) List<File> files) {
//        List<File> catalogFiles = new LinkedList<>();
        List<QueryResult<File>> queryResults = new LinkedList<>();
        long studyId;
        try {
            studyId = catalogManager.getStudyId(studyIdStr);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e);
        }
        for (File file : files) {
            try {
                QueryResult<File> fileQueryResult = catalogManager.createFile(studyId, file.getType(), file.getFormat(),
                        file.getBioformat(), file.getPath(), file.getCreationDate(),
                        file.getDescription(), new File.FileStatus(file.getStatus().getName()), file.getDiskUsage(), file.getExperimentId(),
                        file.getSampleIds(), file.getJobId(), file.getStats(), file.getAttributes(), true, queryOptions, sessionId);
//                file = fileQueryResult.getResult().get(0);
                System.out.println("fileQueryResult = " + fileQueryResult);
                queryResults.add(fileQueryResult);
            } catch (Exception e) {
                queryResults.add(new QueryResult<>("createFile", 0, 0, 0, "", e.getMessage(), Collections.<File>emptyList()));
//            return createErrorResponse(e.getMessage());
            }
        }
        return createOkResponse(queryResults);
    }

    @GET
    @Path("/create-folder")
    @ApiOperation(value = "Create a folder in the catalog environment", position = 2, response = File.class)
    public Response createFolder(@ApiParam(value = "Study id", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "CSV list of paths where the folders will be created", required = true) @QueryParam("folders") String folders,
                                 @ApiParam(value = "Create the parent directories if they do not exist", required = false)
                                 @QueryParam("parents") @DefaultValue("false") boolean parents) {
        try {
            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            List<String> folderList = convertPathList(folders);

            List<QueryResult> queryResultList = new ArrayList<>(folderList.size());
            for (String folder : folderList) {
                try {
                    java.nio.file.Path folderPath = Paths.get(convertPath(folder));
                    queryResultList.add(catalogManager.createFolder(studyId, folderPath, parents, queryOptions, sessionId));
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
    @Path("/{fileIds}/info")
    @ApiOperation(value = "File info", position = 3, response = File[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response info(@ApiParam(value="Comma separated list of file ids") @PathParam(value = "fileIds") String fileStr) {
        try {
            List<QueryResult<File>> queryResults = new LinkedList<>();
            List<String> strings = convertPathList(fileStr);
            List<Long> fileIds = catalogManager.getFileIds(StringUtils.join(strings.toArray(), ","), sessionId);
            for (Long fileId : fileIds) {
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
    @ApiOperation(value = "File uri", position = 3, notes = "Deprecated method. Use /info with include query options instead.")
    public Response getUri(@ApiParam(value = "fileId") @PathParam(value = "fileId") String fileIds) {
        try {
            List<QueryResult> results = new LinkedList<>();
            for (String fileId : fileIds.split(",")) {
                System.out.println("fileId = " + fileId);
                QueryResult<File> result = catalogManager.getFile(catalogManager.getFileId(fileId, sessionId), this.queryOptions, sessionId);
                URI fileUri = result.first().getUri();
                results.add(new QueryResult<>(fileId, 0, 1, 1, "", "", Collections.singletonList(fileUri)));
            }
            return createOkResponse(results);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @ApiOperation(httpMethod = "POST", position = 4, value = "Resource to upload a file by chunks", response = File.class)
    public Response upload(@FormDataParam("chunk_content") byte[] chunkBytes,
                                @FormDataParam("chunk_content") FormDataContentDisposition contentDisposition,
                                @FormDataParam("file") InputStream fileInputStream,
                                @FormDataParam("file") FormDataContentDisposition fileMetaData,

                                @DefaultValue("") @FormDataParam("chunk_id") String chunk_id,
                                @DefaultValue("false") @FormDataParam("last_chunk") String last_chunk,
                                @DefaultValue("") @FormDataParam("chunk_total") String chunk_total,
                                @DefaultValue("") @FormDataParam("chunk_size") String chunk_size,
                                @DefaultValue("") @FormDataParam("chunk_hash") String chunkHash,
                                @DefaultValue("false") @FormDataParam("resume_upload") String resume_upload,

                                @ApiParam(value = "filename", required = false) @FormDataParam("filename") String filename,
                                @ApiParam(value = "fileFormat", required = true) @DefaultValue("") @FormDataParam("fileFormat") String fileFormat,
                                @ApiParam(value = "bioformat", required = true) @DefaultValue("") @FormDataParam("bioformat") String bioformat,
//                                @ApiParam(value = "userId", required = true) @DefaultValue("") @FormDataParam("userId") String userId,
//                                @ApiParam(defaultValue = "projectId", required = true) @DefaultValue("") @FormDataParam("projectId") String projectId,
                                @ApiParam(value = "studyId", required = true) @FormDataParam("studyId") String studyIdStr,
                                @ApiParam(value = "Path within catalog where the file will be located (default: root folder)", required = false) @DefaultValue(".") @FormDataParam("relativeFilePath") String relativeFilePath,
                                @ApiParam(value = "description", required = false) @DefaultValue("") @FormDataParam("description") String description,
                                @ApiParam(value = "Create the parent directories if they do not exist", required = false) @DefaultValue("true") @FormDataParam("parents") boolean parents) {

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
            studyId = catalogManager.getStudyId(studyIdStr, sessionId);
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
                        QueryResult<File> queryResult = catalogManager.createFile(studyId, File.Format.valueOf(fileFormat.toUpperCase()), File.Bioformat.valueOf(bioformat.toUpperCase()), relativeFilePath, completedFilePath.toUri(), description, parents, sessionId);
                        File file = new FileMetadataReader(catalogManager).setMetadataInformation(queryResult.first(), null, new QueryOptions(queryOptions), sessionId, false);
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
                        catalogManager.createFolder(studyId, Paths.get(relativeFilePath), parents, null, sessionId);
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
    @Path("/{fileId}/download")
    @ApiOperation(value = "Download file", position = 5, response = QueryResponse.class)
    public Response download(@ApiParam(value = "File id") @PathParam("fileId") String fileIdStr) {
        try {
            DataInputStream stream;
            long fileId = catalogManager.getFileId(convertPath(fileIdStr), sessionId);
            QueryResult<File> queryResult = catalogManager.getFile(fileId, this.queryOptions, sessionId);
            File file = queryResult.getResult().get(0);
            stream = catalogManager.downloadFile(fileId, sessionId);
//             String content = org.apache.commons.io.IOUtils.toString(stream);
            return createOkResponse(stream, MediaType.APPLICATION_OCTET_STREAM_TYPE, file.getName());
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/content")
    @ApiOperation(value = "Show the content of a file (up to a limit)", position = 6, response = String.class)
    public Response content(@ApiParam(value = "File id") @PathParam("fileId") String fileIdStr,
                            @ApiParam(value = "start", required = false) @QueryParam("start") @DefaultValue("-1") int start,
                            @ApiParam(value = "limit", required = false) @QueryParam("limit") @DefaultValue("-1") int limit) {
        try {
            long fileId = catalogManager.getFileId(convertPath(fileIdStr), sessionId);
            DataInputStream stream = catalogManager.downloadFile(fileId, start, limit, sessionId);
//             String content = org.apache.commons.io.IOUtils.toString(stream);
            return createOkResponse(stream, MediaType.TEXT_PLAIN_TYPE);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/grep")
    @ApiOperation(value = "Searches for lines of the file containing a match of the pattern [TOCHECK]", position = 7, response = String.class)
    public Response downloadGrep(
            @ApiParam(value = "File id") @PathParam("fileId") String fileIdStr,
            @ApiParam(value = "Pattern", required = false) @QueryParam("pattern") @DefaultValue(".*") String pattern,
            @ApiParam(value = "Do a case insensitive search", required = false) @QueryParam("ignoreCase") @DefaultValue("false") Boolean ignoreCase,
            @ApiParam(value = "Return multiple matches", required = false) @QueryParam("multi") @DefaultValue("true") Boolean multi) {
        try {
            long fileId = catalogManager.getFileId(convertPath(fileIdStr), sessionId);
            DataInputStream stream = catalogManager.grepFile(fileId, pattern, ignoreCase, multi, sessionId);
//             String content = org.apache.commons.io.IOUtils.toString(stream);
            return createOkResponse(stream, MediaType.TEXT_PLAIN_TYPE);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{fileId}/set-header")
    @ApiOperation(value = "Set file header", position = 10, notes = "Deprecated method. Moved to update.")
    public Response setHeader(@PathParam(value = "fileId") @FormDataParam("fileId") String fileStr,
                              @ApiParam(value = "header", required = true) @DefaultValue("") @QueryParam("header") String header) {
        String content = "";
        DataInputStream stream;
        QueryResult<File> fileQueryResult;
        InputStream streamBody = null;
//        System.out.println("header: "+header);
        try {
            long fileId = catalogManager.getFileId(convertPath(fileStr), sessionId);
            /** Obtain file uri **/
            File file = catalogManager.getFile(fileId, sessionId).getResult().get(0);
            URI fileUri = catalogManager.getFileUri(file);
            System.out.println("getUri: " + fileUri.getPath());

            /** Set header **/
            stream = catalogManager.downloadFile(fileId, sessionId);
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
    @Path("/{folderId}/files")
    @ApiOperation(value = "File content", position = 11, notes = "Deprecated method. Moved to /list.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query")
    })
    public Response getAllFilesInFolder(@PathParam(value = "folderId") @FormDataParam("folderId") String folderIdStr) {
        QueryResult<File> results;
        try {
            long folderId = catalogManager.getFileId(convertPath(folderIdStr), sessionId);
            results = catalogManager.getAllFilesInFolder(folderId, queryOptions, sessionId);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return createOkResponse(results);
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Look for files using some filters", position = 12, response = File[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response search(@ApiParam(value = "Comma separated list of file ids", required = false) @DefaultValue("") @QueryParam("id") String id,
                           @ApiParam(value = "Study id", required = true) @QueryParam("studyId") String studyId,
                           // This can now be done using just the ids field
                           @Deprecated @ApiParam(value = "Comma separated list of file names", required = false) @DefaultValue("") @QueryParam("name") String name,
                           // This can now be done using just the ids field
                           @Deprecated @ApiParam(value = "Comma separated list of paths", required = false) @DefaultValue("") @QueryParam("path") String path,
                           @ApiParam(value = "Available types (FILE, DIRECTORY)", required = false) @DefaultValue("") @QueryParam("type") String type,
                           @ApiParam(value = "Comma separated Bioformat values. For existing Bioformats see files/help", required = false) @DefaultValue("") @QueryParam("bioformat") String bioformat,
                           @ApiParam(value = "Comma separated Format values. For existing Formats see files/help", required = false) @DefaultValue("") @QueryParam("format") String formats,
                           @ApiParam(value = "Status", required = false) @DefaultValue("") @QueryParam("status") String status,
                           @ApiParam(value = "Directory under which we want to look for files or folders", required = false) @DefaultValue("") @QueryParam("directory") String directory,
                           @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
                           @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss)", required = false) @DefaultValue("") @QueryParam("modificationDate") String modificationDate,
                           @ApiParam(value = "Description", required = false) @DefaultValue("") @QueryParam("description") String description,
                           @ApiParam(value = "Disk usage", required = false) @DefaultValue("") @QueryParam("diskUsage") Long diskUsage,
                           @ApiParam(value = "Comma separated list of sample ids", required = false) @DefaultValue("") @QueryParam("sampleIds") String sampleIds,
                           @ApiParam(value = "Job id that created the file(s) or folder(s)", required = false) @DefaultValue("") @QueryParam("jobId") String jobId,
                           @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                           @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)", required = false) @DefaultValue("") @QueryParam("nattributes") String nattributes) {
        try {
            long studyIdNum = catalogManager.getStudyId(studyId, sessionId);
            // TODO this must be changed: only one queryOptions need to be passed
            Query query = new Query();
            QueryOptions qOptions = new QueryOptions(this.queryOptions);
            parseQueryParams(params, FileDBAdaptor.QueryParams::getParam, query, qOptions);

            if (query.containsKey(FileDBAdaptor.QueryParams.NAME.key())
                    && (query.get(FileDBAdaptor.QueryParams.NAME.key()) == null
                    || query.getString(FileDBAdaptor.QueryParams.NAME.key()).isEmpty())) {
                query.remove(FileDBAdaptor.QueryParams.NAME.key());
                logger.debug("Name attribute empty, it's been removed");
            }

            if (!qOptions.containsKey(QueryOptions.LIMIT)) {
                qOptions.put(QueryOptions.LIMIT, 1000);
                logger.debug("Adding a limit of 1000");
            }
            logger.debug("query = " + query.toJson());
            QueryResult<File> result = catalogManager.searchFile(studyIdNum, query, qOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{folderId}/list")
    @ApiOperation(value = "List all the files inside the folder", position = 13, response = File[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response list(@ApiParam(value = "Folder id") @PathParam("folderId") String folderId) {
        try {
            long fileIdNum = catalogManager.getFileId(convertPath(folderId), sessionId);
            QueryResult result = catalogManager.getAllFilesInFolder(fileIdNum, this.queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/index")
    @ApiOperation(value = "Index variant files", position = 14, response = QueryResponse.class)
    public Response index(@ApiParam("Comma separated list of file ids (files or directories)") @PathParam(value = "fileId") String fileIdStr,
                          @ApiParam("Study id") @QueryParam("studyId") String studyId,
                          @ApiParam("Output directory id") @QueryParam("outDir") String outDirStr,
                          @ApiParam("Boolean indicating that only the transform step will be run") @DefaultValue("false") @QueryParam("transform") boolean transform,
                          @ApiParam("Boolean indicating that only the load step will be run") @DefaultValue("false") @QueryParam("load") boolean load,
                          @ApiParam("Comma separated list of fields to be include in the index") @QueryParam("includeExtraFields") String includeExtraFields,
                          @ApiParam("Type of aggregated VCF file: none, basic, EVS or ExAC") @DefaultValue("none") @QueryParam("aggregated") String aggregated,
                          @ApiParam("Calculate indexed variants statistics after the load step") @DefaultValue("false") @QueryParam("calculateStats") boolean calculateStats,
                          @ApiParam("Annotate indexed variants after the load step") @DefaultValue("false") @QueryParam("annotate") boolean annotate,
                          @ApiParam("Overwrite annotations already present in variants") @DefaultValue("false") @QueryParam("overwrite") boolean overwriteAnnotations) {

        Map<String, String> params = new LinkedHashMap<>();
        addParamIfNotNull(params, "studyId", studyId);
        addParamIfNotNull(params, "outdir", outDirStr);
        addParamIfTrue(params, "transform", transform);
        addParamIfTrue(params, "load", load);
        addParamIfNotNull(params, "include-extra-fields", includeExtraFields);
        addParamIfNotNull(params, "aggregated", aggregated);
        addParamIfTrue(params, "calculate-stats", calculateStats);
        addParamIfTrue(params, "annotate", annotate);
        addParamIfTrue(params, "overwrite-annotations", overwriteAnnotations);

        logger.info("ObjectMap: {}", params);

        try {
            List<String> fileIds = convertPathList(fileIdStr);
            QueryResult queryResult = catalogManager.getFileManager().index(StringUtils.join(fileIds, ","), "VCF", params, sessionId);
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
//            queryOptions.add(VariantStorageManager.Options.CALCULATE_STATS.key(), calculateStats);
//            queryOptions.add(VariantStorageManager.Options.ANNOTATE.key(), annotate);
//            QueryResult<Job> queryResult = analysisFileIndexer.index(fileId, outDirId, sessionId, new QueryOptions(queryOptions));
//            return createOkResponse(queryResult);
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
    }

    private void addParamIfNotNull(Map<String, String> params, String key, String value) {
        if (key != null && value != null) {
            params.put(key, value);
        }
    }

    private void addParamIfTrue(Map<String, String> params, String key, boolean value) {
        if (key != null && value) {
            params.put(key, Boolean.toString(value));
        }
    }

    @GET
    @Path("/{folderId}/tree-view")
    @ApiOperation(value = "Obtain a tree view of the files and folders within a folder", position = 15, response = FileTree[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "[TO BE IMPLEMENTED] Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
    })
    public Response treeView(@ApiParam(value = "Folder id or path") @PathParam("folderId") String folderId,
                             @ApiParam(value = "Maximum depth to get files from") @DefaultValue("5") @QueryParam("maxDepth") int maxDepth) {
//                             @ApiParam(value = "Available types (FILE, DIRECTORY)") @DefaultValue("") @QueryParam("type") String type,
//                             @ApiParam(value = "Comma separated Bioformat values. For existing Bioformats see files/help") @DefaultValue("") @QueryParam("bioformat") String bioformat,
//                             @ApiParam(value = "Comma separated Format values. For existing Formats see files/help") @DefaultValue("") @QueryParam("format") String formats,
//                             @ApiParam(value = "Creation date (Format: yyyyMMddHHmmss)") @DefaultValue("") @QueryParam("creationDate") String creationDate,
//                             @ApiParam(value = "Modification date (Format: yyyyMMddHHmmss)") @DefaultValue("") @QueryParam("modificationDate") String modificationDate,
//                             @ApiParam(value = "Description") @DefaultValue("") @QueryParam("description") String description,
//                             @ApiParam(value = "Disk usage") @DefaultValue("") @QueryParam("diskUsage") Long diskUsage,
//                             @ApiParam(value = "Comma separated list of sample ids") @DefaultValue("") @QueryParam("sampleIds") String sampleIds,
//                             @ApiParam(value = "Job id that created the file(s) or folder(s)") @DefaultValue("") @QueryParam("jobId") String jobId,
//                             @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @DefaultValue("") @QueryParam("attributes") String attributes,
//                             @ApiParam(value = "Numerical attributes (Format: sex=male,age>20 ...)") @DefaultValue("") @QueryParam("nattributes") String nattributes) {
        try {
            Query query = new Query();
            QueryOptions qOptions = new QueryOptions(this.queryOptions);
            parseQueryParams(params, FileDBAdaptor.QueryParams::getParam, query, qOptions);
            QueryResult result = catalogManager.getFileManager().getTree(convertPath(folderId), query, qOptions, maxDepth, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{fileId}/fetch")
    @ApiOperation(value = "File fetch", notes = "DEPRECATED. Use .../files/{fileId}/[variants|alignments] or .../studies/{studyId}/[variants|alignments] instead", position = 15)
    public Response fetch(@PathParam(value = "fileId") @DefaultValue("") String fileIds,
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
                fileIdNum = catalogManager.getFileId(fileId, sessionId);
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
                dataStore = AbstractFileIndexer.getDataStore(catalogManager, catalogManager.getStudyIdByFileId(file.getId()), file.getBioformat(), sessionId);
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
                        AlignmentStorageManager alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager(storageEngine);
                        dbAdaptor = alignmentStorageManager.getDBAdaptor(dbName);
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageManagerException e) {
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
                    Query query = VariantFetcher.getVariantQuery(queryOptions);
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
                        dbAdaptor = storageManagerFactory.getVariantStorageManager(storageEngine).getDBAdaptor(dbName);
//                        dbAdaptor = new CatalogVariantDBAdaptor(catalogManager, dbAdaptor);
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageManagerException e) {
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

    @GET
    @Path("/{fileId}/variants")
    @ApiOperation(value = "Fetch variants from a VCF/gVCF file", position = 15, response = QueryResponse.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getVariants(@ApiParam(value = "", required = true) @PathParam("fileId") String fileIdCsv,
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
                                @ApiParam(value = "Consequence type SO term list. e.g. SO:0000045,SO:0000046") @QueryParam("annot-ct") String annot_ct,
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
                                @ApiParam(value = "Returns the samples metadata group by studyId, instead of the variants", required = false) @QueryParam("samplesMetadata") boolean samplesMetadata,
                                @ApiParam(value = "Sort the results", required = false) @QueryParam("sort") boolean sort,
                                @ApiParam(value = "Group variants by: [ct, gene, ensemblGene]", required = false) @DefaultValue("") @QueryParam("groupBy") String groupBy,
                                @ApiParam(value = "Calculate histogram. Requires one region.", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                                @ApiParam(value = "Histogram interval size", required = false) @DefaultValue("2000") @QueryParam("interval") int interval,
                                @ApiParam(value = "Merge results", required = false) @DefaultValue("false") @QueryParam("merge") boolean merge) {

        List<QueryResult> results = new LinkedList<>();
        try {
            VariantFetcher variantFetcher = new VariantFetcher(catalogManager, storageManagerFactory);
            List<String> fileIds = convertPathList(fileIdCsv);
//            String[] splitFileId = fileIdCsv.split(",");
            for (String fileId : fileIds) {
                QueryResult result;
                result = variantFetcher.getVariantsPerFile(region, histogram, groupBy, interval, fileId, sessionId, queryOptions);
                results.add(result);
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return createOkResponse(results);
    }

    @GET
    @Path("/{fileId}/alignments")
    @ApiOperation(value = "Fetch alignments from a BAM file", position = 15, response = Alignment[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAlignments(@ApiParam(value = "fileId", required = true) @PathParam("fileId") String fileId) {
        return createOkResponse("PENDING");
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
    @Path("/{fileId}/update")
    @ApiOperation(value = "Update fields of a file", position = 16, response = File.class)
    public Response update(@ApiParam(value = "File id") @PathParam(value = "fileId") String fileIdStr,
                           @ApiParam(value = "File name", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "Format of the file (VCF, BCF, GVCF, SAM, BAM, BAI...UNKNOWN)", required = false) @DefaultValue("") @QueryParam("format") String format,
                           @ApiParam(value = "Bioformat of the file (VARIANT, ALIGNMENT, SEQUENCE, PEDIGREE...NONE)", required = false) @DefaultValue("") @QueryParam("bioformat") String bioformat,
                           @ApiParam(value = "Description of the file", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "Attributes", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                           @ApiParam(value = "Stats", required = false) @DefaultValue("") @QueryParam("stats") String stats,
                           @ApiParam(value = "Sample ids", required = false) @DefaultValue("") @QueryParam("sampleIds") String sampleIds,
                           @ApiParam(value = "Job id", required = false) @DefaultValue("") @QueryParam("jobId") String jobId,
                           @ApiParam(value = "Path", required = false) @DefaultValue("") @QueryParam("path") String path) {
        try {
            /*ObjectMap parameters = new ObjectMap();
            QueryOptions qOptions = new QueryOptions();
            parseQueryParams(params, CatalogFileDBAdaptor.QueryParams::getParam, parameters, qOptions);*/
            ObjectMap params = new ObjectMap();
            params.putIfNotEmpty("name", name);
            params.putIfNotEmpty("format", format);
            params.putIfNotEmpty("bioformat", bioformat);
            params.putIfNotEmpty("description", description);
            params.putIfNotEmpty("attributes", attributes);
            params.putIfNotEmpty("stats", stats);
            params.putIfNotEmpty("sampleIds", sampleIds);
            params.putIfNotEmpty("jobId", jobId);
            params.putIfNotEmpty("path", path);
            long fileId = catalogManager.getFileId(convertPath(fileIdStr), sessionId);
            QueryResult queryResult = catalogManager.getFileManager().update(fileId, params, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class UpdateFile {
        public String name;
        //        public File.Format format;
//        public File.Bioformat bioformat;
//        public String path;
//        public String ownerId;
//        public String creationDate;
//        public String modificationDate;
        public String description;
        //        public Long diskUsage;
//        public int experimentId;
        public List<Integer> sampleIds;
        public Integer jobId;
        //        public Map<String, Object> stats;
        public Map<String, Object> attributes;
    }

    @POST
    @Path("/{fileId}/update")
    @ApiOperation(value = "Modify file", position = 16, response = File.class)
    public Response updatePOST(@ApiParam(value = "File id") @PathParam(value = "fileId") String fileIdStr,
                               @ApiParam(name = "params", value = "Parameters to modify", required = true) UpdateFile params) {
        try {
            long fileId = catalogManager.getFileId(convertPath(fileIdStr), sessionId);
            QueryResult queryResult = catalogManager.modifyFile(fileId, new ObjectMap(jsonObjectMapper.writeValueAsString(params)), sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
//
//    @GET
//    @Path("/{fileIds}/share")
//    @ApiOperation(value = "Share files with other members", position = 17)
//    public Response share(@PathParam(value = "fileIds") String fileIds,
//                          @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
//                          @ApiParam(value = "Comma separated list of file permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
//                          @ApiParam(value = "Boolean indicating whether to allow the change of of permissions in case any member already had any", required = true) @DefaultValue("false") @QueryParam("override") boolean override) {
//        try {
//            return createOkResponse(catalogManager.shareFile(fileIds, members, Arrays.asList(permissions.split(",")), override, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @GET
//    @Path("/{fileIds}/unshare")
//    @ApiOperation(value = "Remove the permissions for the list of members", position = 18)
//    public Response unshare(@PathParam(value = "fileIds") String fileIds,
//                            @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members,
//                            @ApiParam(value = "Comma separated list of file permissions", required = false) @DefaultValue("") @QueryParam("permissions") String permissions) {
//        try {
//            return createOkResponse(catalogManager.unshareFile(fileIds, members, permissions, sessionId));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/link")
    @ApiOperation(value = "Link an external file into catalog.", hidden = true, position = 19, response = QueryResponse.class)
    public Response link(@ApiParam(value = "Uri of the file", required = true) @QueryParam("uri") String uriStr,
                         @ApiParam(value = "Study id", required = true) @QueryParam("studyId") String studyIdStr,
                         @ApiParam(value = "Path where the external file will be allocated in catalog", required = true) @QueryParam("path") String path,
                         @ApiParam(value = "Description") @QueryParam("description") String description,
                         @ApiParam(value = "Create the parent directories if they do not exist") @DefaultValue("false") @QueryParam("parents") boolean parents,
                         @ApiParam(value = "[TO HIDE] Size of the folder/file") @QueryParam("size") long size,
                         @ApiParam(value = "[TO HIDE] Checksum of something") @QueryParam("checksum") String checksum) {
        try {
            logger.debug("uri: {}", convertPathList(uriStr));
            logger.debug("studyId: {}", studyIdStr);
            logger.debug("path: {}", convertPath(path));

            path = convertPath(path);
            ObjectMap objectMap = new ObjectMap()
                    .append("parents", parents)
                    .append("description", description);
            List<String> uriList = convertPathList(uriStr);

            List<QueryResult<File>> queryResultList = new ArrayList<>();
            logger.info("path: {}", path);
            for (String uri : uriList) {
                logger.info("uri: {}", uri);
                try {
                    URI myUri = UriUtils.createUri(uri);
                    queryResultList.add(catalogManager.link(myUri, path, studyIdStr, objectMap, sessionId));
                } catch (URISyntaxException | CatalogException | IOException e) {
                    queryResultList.add(new QueryResult<>("Link file", -1, 0, 0, "", e.getMessage(), Collections.emptyList()));
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
    public Response link(@ApiParam(value = "File id", required = true) @QueryParam("fileId") String fileIdStr) throws CatalogException {
        try {
            QueryResult<File> queryResult = catalogManager.unlink(convertPath(fileIdStr), queryOptions, sessionId);
            return createOkResponse(new QueryResult<>("unlink", 0, 1, 1, null, null, queryResult.getResult()));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{fileId}/relink")
    @ApiOperation(value = "Change file location. Provided file must be either STAGE or be an external file.", position = 21)
    public Response relink(@ApiParam(value = "File ID") @PathParam("fileId") @DefaultValue("") String fileIdStr,
                           @ApiParam(value = "New URI" ,required = true) @QueryParam("uri") String uriStr,
                           @ApiParam(value = "Do calculate checksum for new files", required = false) @DefaultValue("false") @QueryParam("calculateChecksum") boolean calculateChecksum ) {
        try {
            URI uri = UriUtils.createUri(uriStr);
            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(uri);

            if (!ioManager.exists(uri)) {
                throw new CatalogIOException("File " + uri + " does not exist");
            }

            long fileId = catalogManager.getFileId(convertPath(fileIdStr), sessionId);
            File file = catalogManager.getFile(fileId, sessionId).first();

            new CatalogFileUtils(catalogManager).link(file, calculateChecksum, uri, false, true, sessionId);
            file = catalogManager.getFile(file.getId(), queryOptions, sessionId).first();
            file = FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, new QueryOptions(queryOptions), sessionId, false);

            return createOkResponse(new QueryResult<>("relink", 0, 1, 1, null, null, Collections.singletonList(file)));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/refresh")
    @ApiOperation(value = "Refresh metadata from the selected file or folder. Return updated files.", position = 22,
            response = QueryResponse.class)
    public Response refresh(@ApiParam(value = "File id") @PathParam(value = "fileId") String fileIdStr) {
        try {
            long fileId = catalogManager.getFileId(convertPath(fileIdStr), sessionId);
            File file = catalogManager.getFile(fileId, sessionId).first();

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
    @Path("/{fileId}/delete")
    @ApiOperation(value = "Delete file", position = 23, response = QueryResponse.class)
    public Response deleteGET(@ApiParam(value = "File id") @PathParam(value = "fileId") String fileIdStr,
                              @ApiParam(value = "Delete files and folders from disk (only applicable for linked files/folders)",
                                      required = false) @DefaultValue("false") @QueryParam("deleteExternal") boolean deleteExternal,
                              @ApiParam(value="Skip trash and delete the files/folders from disk directly (CANNOT BE RECOVERED)",
                                      required = false) @DefaultValue("false") @QueryParam("skipTrash") boolean skipTrash) {
        try {
            QueryOptions qOptions = new QueryOptions(queryOptions)
                    .append(FileManager.DELETE_EXTERNAL_FILES, deleteExternal)
                    .append(FileManager.SKIP_TRASH, skipTrash);
            List<QueryResult<File>> result = catalogManager.getFileManager().delete(convertPath(fileIdStr), qOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/groupBy")
    @ApiOperation(value = "Group files by several fields", position = 24, response = QueryResponse.class)
    public Response groupBy(@ApiParam(value = "Comma separated list of fields by which to group by.", required = true) @DefaultValue("") @QueryParam("fields") String fields,
                            @ApiParam(value = "studyId", required = true) @DefaultValue("") @QueryParam("studyId") String studyStr,
                            @ApiParam(value = "Comma separated list of ids.", required = false) @DefaultValue("") @QueryParam("id") String ids,
                            @ApiParam(value = "Comma separated list of names.", required = false) @DefaultValue("") @QueryParam("name") String names,
                            @ApiParam(value = "path", required = false) @DefaultValue("") @QueryParam("path") String path,
                            @ApiParam(value = "Comma separated Type values.", required = false) @DefaultValue("") @QueryParam("type") String type,
                            @ApiParam(value = "Comma separated Bioformat values.", required = false) @DefaultValue("") @QueryParam("bioformat") String bioformat,
                            @ApiParam(value = "Comma separated Format values.", required = false) @DefaultValue("") @QueryParam("format") String formats,
                            @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
                            @ApiParam(value = "directory", required = false) @DefaultValue("") @QueryParam("directory") String directory,
                            @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
                            @ApiParam(value = "modificationDate", required = false) @DefaultValue("") @QueryParam("modificationDate") String modificationDate,
                            @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description") String description,
                            @ApiParam(value = "diskUsage", required = false) @DefaultValue("") @QueryParam("diskUsage") Long diskUsage,
                            @ApiParam(value = "Comma separated sampleIds", required = false) @DefaultValue("") @QueryParam("sampleIds") String sampleIds,
                            @ApiParam(value = "jobId", required = false) @DefaultValue("") @QueryParam("jobId") String jobId,
                            @ApiParam(value = "attributes", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                            @ApiParam(value = "numerical attributes", required = false) @DefaultValue("") @QueryParam("nattributes") String nattributes) {
        try {
            Query query = new Query();
            QueryOptions qOptions = new QueryOptions();
            parseQueryParams(params, FileDBAdaptor.QueryParams::getParam, query, qOptions);

            logger.debug("query = " + query.toJson());
            logger.debug("queryOptions = " + qOptions.toJson());
            QueryResult result = catalogManager.fileGroupBy(query, qOptions, fields, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileIds}/acl")
    @ApiOperation(value = "Return the acl defined for the file or folder", position = 18, response = QueryResponse.class)
    public Response getAcls(@ApiParam(value = "Comma separated list of file ids", required = true) @PathParam("fileIds") String fileIdStr) {
        try {
            Object[] fileIds = convertPathList(fileIdStr).toArray();
            return createOkResponse(catalogManager.getAllFileAcls(StringUtils.join(fileIds, ","), sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{fileIds}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of users or groups", position = 19,
            response = QueryResponse.class)
    public Response createRole(@ApiParam(value = "Comma separated list of file ids", required = true) @PathParam("fileIds") String fileIdStr,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("members") String members) {
        try {
            Object[] fileIds = convertPathList(fileIdStr).toArray();
            return createOkResponse(catalogManager.createFileAcls(StringUtils.join(fileIds, ","), members, permissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/acl/{memberId}/info")
    @ApiOperation(value = "Return the permissions granted for the user or group", position = 20, response = QueryResponse.class)
    public Response getAcl(@ApiParam(value = "File id", required = true) @PathParam("fileId") String fileIdStr,
                           @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.getFileAcl(convertPath(fileIdStr), memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/acl/{memberId}/update")
    @ApiOperation(value = "Update the permissions granted for the user or group", position = 21, response = QueryResponse.class)
    public Response updateAcl(@ApiParam(value = "File id", required = true) @PathParam("fileId") String fileIdStr,
                              @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false)
                              @QueryParam("addPermissions") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false)
                              @QueryParam("removePermissions") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false)
                              @QueryParam("setPermissions") String setPermissions) {
        try {
            return createOkResponse(catalogManager.updateFileAcl(convertPath(fileIdStr), memberId, addPermissions, removePermissions, setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileIds}/acl/{memberId}/delete")
    @ApiOperation(value = "Remove all the permissions granted for the user or group", position = 22,
            response = QueryResponse.class)
    public Response deleteAcl(@ApiParam(value = "Comma separated list of file ids", required = true) @PathParam("fileIds") String fileIdsStr,
                              @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId) {
        try {
            Object[] fileIds = convertPathList(fileIdsStr).toArray();
            return createOkResponse(catalogManager.removeFileAcl(StringUtils.join(fileIds, ","), memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{folderId}/scan")
    @ApiOperation(value = "Scans a folder", position = 6)
    public Response scan(@PathParam(value = "folderId") @FormDataParam("folderId") String folderIdStr,
                         @ApiParam(value = "calculateChecksum") @QueryParam("calculateChecksum") @DefaultValue("false") boolean calculateChecksum) {
        try {
            long folderId = catalogManager.getFileId(convertPath(folderIdStr), sessionId);
            File directory = catalogManager.getFile(folderId, sessionId).first();
            List<File> scan = new FileScanner(catalogManager)
                    .scan(directory, null, FileScanner.FileScannerPolicy.REPLACE, calculateChecksum, false, sessionId);
            return createOkResponse(new QueryResult<>("Scan", 0, scan.size(), scan.size(), "", "", scan));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private String convertPath(String path) {
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
            // The path will probably contain the study as well and those : should remain
            int position = path.indexOf(":");
            return path.substring(0, position + 1) + path.substring(position + 1).replace(":", "/");
        }
    }

    private List<String> convertPathList(String path) {
        if (path == null) {
            return Collections.emptyList();
        } else if (path.contains(",")) {
            String[] split = path.split(",");
            List<String> pathList = new ArrayList<>(split.length);
            for (String s : split) {
                pathList.add(convertPath(s));
            }
            return pathList;
        } else {
            return Collections.singletonList(convertPath(path));
        }
    }

}
