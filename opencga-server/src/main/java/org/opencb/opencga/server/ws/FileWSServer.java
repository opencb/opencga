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

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.biodata.models.core.Region;
import org.opencb.datastore.core.*;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.analysis.files.FileScanner;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.server.utils.VariantFetcher;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;


@Path("/{version}/files")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Files", position = 4, description = "Methods for working with 'files' endpoint")
public class FileWSServer extends OpenCGAWSServer {


    public FileWSServer(@PathParam("version") String version, @Context UriInfo uriInfo,
                        @Context HttpServletRequest httpServletRequest)
            throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, VersionException {
        super(version, uriInfo, httpServletRequest);
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

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create file with POST method", position = 1, response = QueryResult.class, notes =
            "This method only creates the file entry in Catalog.<br>" +
                    "Will accept (but not yet): acl.<br>" +
                    "<ul>" +
                    "<il><b>id</b> parameter will be ignored.<br></il>" +
                    "<il><b>type</b> accepted values: [<b>'FOLDER', 'FILE', 'INDEX'</b>].<br></il>" +
                    "<il><b>format</b> accepted values: [<b>'PLAIN', 'GZIP', 'EXECUTABLE', 'IMAGE'</b>].<br></il>" +
                    "<il><b>bioformat</b> accepted values: [<b>'VARIANT', 'ALIGNMENT', 'SEQUENCE', 'NONE'</b>].<br></il>" +
                    "<il><b>status</b> accepted values (admin required): [<b>'INDEXING', 'STAGE', 'UPLOADED', 'READY', 'TRASHED', 'DELETED'</b>].<br></il>" +
                    "<il><b>creatorId</b> should be the same as que sessionId user (unless you are admin) </il>" +
                    "<ul>")
    public Response createFilePOST(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                   @ApiParam(value = "files", required = true) List<File> files) {
//        List<File> catalogFiles = new LinkedList<>();
        List<QueryResult<File>> queryResults = new LinkedList<>();
        int studyId;
        try {
            studyId = catalogManager.getStudyId(studyIdStr);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e);
        }
        for (File file : files) {
            try {
                QueryResult<File> fileQueryResult = catalogManager.createFile(studyId, file.getType(), file.getFormat(),
                        file.getBioformat(), file.getPath(), file.getOwnerId(), file.getCreationDate(),
                        file.getDescription(), file.getStatus(), file.getDiskUsage(), file.getExperimentId(),
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
    @ApiOperation(value = "Create folder", position = 2)
    public Response createFolder(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") String studyIdStr,
                                 @ApiParam(value = "Path of the folder to be created", required = true) @QueryParam("folder") String folder,
                                 @ApiParam(value = "parents", required = false) @QueryParam("parents") @DefaultValue("true") boolean parents) {
        try {
            java.nio.file.Path folderPath = Paths.get(folder);
            int studyId = catalogManager.getStudyId(studyIdStr);
            QueryResult queryResult = catalogManager.createFolder(studyId, folderPath, parents, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/info")
    @ApiOperation(value = "File info", position = 3)
    public Response info(@PathParam(value = "fileId") @DefaultValue("") @FormDataParam("fileId") String fileId) {
        try {
            String[] fieldIdArray = fileId.split(",");
            List<QueryResult> results = new LinkedList<>();
            for (String id : fieldIdArray) {
                results.add(catalogManager.getFile(catalogManager.getFileId(id), this.queryOptions, sessionId));
            }
            return createOkResponse(results);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/uri")
    @ApiOperation(value = "File uri", position = 3)
    public Response getUri(@PathParam(value = "fileId") @DefaultValue("") @FormDataParam("fileId") String fileIds) {
        try {
            List<QueryResult> results = new LinkedList<>();
            for (String fileId : fileIds.split(",")) {
                QueryResult<File> result = catalogManager.getFile(catalogManager.getFileId(fileId), this.queryOptions, sessionId);
                URI fileUri = catalogManager.getFileUri(result.first());
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
    @ApiOperation(httpMethod = "POST", position = 4, value = "Resource to upload a file by chunks", response = QueryResponse.class)
    public Response chunkUpload(@FormDataParam("chunk_content") byte[] chunkBytes,
                                @FormDataParam("chunk_content") FormDataContentDisposition contentDisposition,
                                @DefaultValue("") @FormDataParam("chunk_id") String chunk_id,
                                @DefaultValue("false") @FormDataParam("last_chunk") String last_chunk,
                                @DefaultValue("") @FormDataParam("chunk_total") String chunk_total,
                                @DefaultValue("") @FormDataParam("chunk_size") String chunk_size,
                                @DefaultValue("") @FormDataParam("chunk_hash") String chunkHash,
                                @DefaultValue("false") @FormDataParam("resume_upload") String resume_upload,

                                @ApiParam(value = "filename", required = true) @DefaultValue("") @FormDataParam("filename") String filename,
                                @ApiParam(value = "fileFormat", required = true) @DefaultValue("") @FormDataParam("fileFormat") String fileFormat,
                                @ApiParam(value = "bioFormat", required = true) @DefaultValue("") @FormDataParam("bioFormat") String bioFormat,
                                @ApiParam(value = "userId", required = true) @DefaultValue("") @FormDataParam("userId") String userId,
//                                @ApiParam(defaultValue = "projectId", required = true) @DefaultValue("") @FormDataParam("projectId") String projectId,
                                @ApiParam(value = "studyId", required = true) @FormDataParam("studyId") String studyIdStr,
                                @ApiParam(value = "relativeFilePath", required = true) @DefaultValue("") @FormDataParam("relativeFilePath") String relativeFilePath,
                                @ApiParam(value = "description", required = true) @DefaultValue("") @FormDataParam("description") String description,
                                @ApiParam(value = "parents", required = true) @DefaultValue("true") @FormDataParam("parents") boolean parents) {

        long t = System.currentTimeMillis();

        java.nio.file.Path filePath = null;
        final int studyId;
        try {
            studyId = catalogManager.getStudyId(studyIdStr);
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
                            File.Bioformat.valueOf(bioFormat.toUpperCase()), relativeFilePath, completedFilePath.toUri(),
                            description, parents, sessionId
                    );
                    File file = new FileMetadataReader(catalogManager).setMetadataInformation(queryResult.first(), null,
                            queryOptions, sessionId, false);
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
    }

    @GET
    @Path("/{fileId}/download")
    @ApiOperation(value = "File download", position = 5)
    public Response download(@PathParam(value = "fileId") @FormDataParam("fileId") String fileIdStr) {
        try {
            DataInputStream stream;
            int fileId = catalogManager.getFileId(fileIdStr);
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
    @ApiOperation(value = "File content", position = 6)
    public Response content(@PathParam(value = "fileId") @FormDataParam("fileId") String fileIdStr,
                            @ApiParam(value = "start", required = false) @QueryParam("start") @DefaultValue("-1") int start,
                            @ApiParam(value = "limit", required = false) @QueryParam("limit") @DefaultValue("-1") int limit) {
        try {
            int fileId = catalogManager.getFileId(fileIdStr);
            DataInputStream stream = catalogManager.downloadFile(fileId, start, limit, sessionId);
//             String content = org.apache.commons.io.IOUtils.toString(stream);
            return createOkResponse(stream, MediaType.TEXT_PLAIN_TYPE);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/content-grep")
    @ApiOperation(value = "File content", position = 7)
    public Response downloadGrep(
            @PathParam(value = "fileId") @FormDataParam("fileId") String fileIdStr,
            @ApiParam(value = "pattern", required = false) @QueryParam("pattern") @DefaultValue(".*") String pattern,
            @ApiParam(value = "ignoreCase", required = false) @QueryParam("ignoreCase") @DefaultValue("false") Boolean ignoreCase,
            @ApiParam(value = "multi", required = false) @QueryParam("multi") @DefaultValue("true") Boolean multi) {
        try {
            int fileId = catalogManager.getFileId(fileIdStr);
            DataInputStream stream = catalogManager.grepFile(fileId, pattern, ignoreCase, multi, sessionId);
//             String content = org.apache.commons.io.IOUtils.toString(stream);
            return createOkResponse(stream, MediaType.TEXT_PLAIN_TYPE);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/content-example")
    @ApiOperation(value = "File content", position = 8)
    public Response downloadExample(@ApiParam(value = "toolName", required = true) @DefaultValue("") @QueryParam("toolName") String toolName,
                                    @ApiParam(value = "fileName", required = true) @DefaultValue("") @QueryParam("fileName") String fileName) {
        /** I think this next two lines should be parametrized either in analysis.properties or the manifest.json of each tool **/
        String analysisPath = Config.getOpenCGAHome() + "/" + Config.getAnalysisProperties().getProperty("OPENCGA.ANALYSIS.BINARIES.PATH");
        String fileExamplesToolPath = analysisPath + "/" + toolName + "/examples/" + fileName;
        try {
            InputStream stream = new FileInputStream(fileExamplesToolPath);
            return createOkResponse(stream, MediaType.APPLICATION_OCTET_STREAM_TYPE, fileName);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/download-example")
    @ApiOperation(value = "File download", position = 9)
    public Response downloadExampleFile(@ApiParam(value = "toolName", required = true) @DefaultValue("") @QueryParam("toolName") String toolName,
                                        @ApiParam(value = "fileName", required = true) @DefaultValue("") @QueryParam("fileName") String fileName) {
        try {
            String analysisPath = Config.getGcsaHome() + "/" + Config.getAnalysisProperties().getProperty("OPENCGA.ANALYSIS.BINARIES.PATH");
            String fileExamplesToolPath = analysisPath + "/" + toolName + "/examples/" + fileName;
            InputStream istream = new FileInputStream(fileExamplesToolPath);
            DataInputStream stream = new DataInputStream(istream);
            return createOkResponse(stream, MediaType.APPLICATION_OCTET_STREAM_TYPE, fileName);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/set-header")
    @ApiOperation(value = "Set file header", position = 10)
    public Response setHeader(@PathParam(value = "fileId") @FormDataParam("fileId") int fileId,
                              @ApiParam(value = "header", required = true) @DefaultValue("") @QueryParam("header") String header) {
        String content = "";
        DataInputStream stream;
        QueryResult<File> fileQueryResult;
        InputStream streamBody = null;
//        System.out.println("header: "+header);
        try {
            /** Obtain file uri **/
            File file = catalogManager.getFile(catalogManager.getFileId(String.valueOf(fileId)), sessionId).getResult().get(0);
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

    @GET
    @Path("/{folderId}/files")
    @ApiOperation(value = "File content", position = 11)
    public Response getAllFilesInFolder(@PathParam(value = "folderId") @FormDataParam("folderId") String folderIdStr) {
        QueryResult<File> results;
        try {
            int folderId = catalogManager.getFileId(folderIdStr);
            results = catalogManager.getAllFilesInFolder(folderId, queryOptions, sessionId);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return createOkResponse(results);
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "File info", position = 12)
    public Response search(@ApiParam(value = "id", required = false) @DefaultValue("") @QueryParam("id") String id,
                           @ApiParam(value = "studyId", required = true) @DefaultValue("") @QueryParam("studyId") String studyId,
                           @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
                           @ApiParam(value = "type", required = false) @DefaultValue("") @QueryParam("type") File.Type type,
                           @ApiParam(value = "path", required = false) @DefaultValue("") @QueryParam("path") String path,
                           @ApiParam(value = "bioformat", required = false) @DefaultValue("") @QueryParam("bioformat") File.Bioformat bioformat,
                           @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") File.Status status,
                           @ApiParam(value = "directory", required = false) @DefaultValue("") @QueryParam("directory") String directory,
                           @ApiParam(value = "ownerId", required = false) @DefaultValue("") @QueryParam("ownerId") String ownerId,
                           @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
                           @ApiParam(value = "modificationDate", required = false) @DefaultValue("") @QueryParam("modificationDate") String modificationDate,
                           @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description") String description,
                           @ApiParam(value = "diskUsage", required = false) @DefaultValue("") @QueryParam("diskUsage") Long diskUsage,
                           @ApiParam(value = "Comma separated sampleIds", required = false) @DefaultValue("") @QueryParam("sampleIds") String sampleIds,
                           @ApiParam(value = "jobId", required = false) @DefaultValue("") @QueryParam("jobId") String jobId,
                           @ApiParam(value = "attributes", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                           @ApiParam(value = "numerical attributes", required = false) @DefaultValue("") @QueryParam("nattributes") String nattributes) {
        try {
            int studyIdNum = catalogManager.getStudyId(studyId);

            // TODO this must be changed: only one queryOptions need to be passed
            QueryOptions query = new QueryOptions();
            for (String param : params.keySet()) {
                try {
                    CatalogFileDBAdaptor.FileFilterOption.valueOf(param.split("\\.")[0]);
                    query.put(param, params.getFirst(param));
                } catch (IllegalArgumentException ignore) {
                }
            }

            if (query.containsKey("name") && (query.get("name") == null || query.getString("name").isEmpty())) {
                query.remove("name");
                System.out.println("Name attribute empty, it;s been removed");
            }

            if (!this.queryOptions.containsKey("limit")) {
                this.queryOptions.put("limit", 1000);
                System.out.println("Adding a limit of 1000");
            }
            System.out.println("query = " + query.toJson());
            QueryResult<File> result = catalogManager.searchFile(studyIdNum, query, this.queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/list")
    @ApiOperation(value = "List folder", position = 13)
    public Response list(@PathParam(value = "fileId") @DefaultValue("") @FormDataParam("fileId") String fileId) {
        try {
            int fileIdNum = catalogManager.getFileId(fileId);
            QueryResult result = catalogManager.getAllFilesInFolder(fileIdNum, this.queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/index")
    @ApiOperation(value = "File index", position = 14)
    public Response index(@ApiParam("fileId") @PathParam(value = "fileId") @DefaultValue("") String fileIdStr,
                          @ApiParam("Output directory id") @DefaultValue("-1") @QueryParam("outdir") String outDirStr,
                          @ApiParam("Annotate variants") @DefaultValue("true") @QueryParam("annotate") boolean annotate,
                          @ApiParam("Calculate stats") @DefaultValue("true") @QueryParam("calculateStats") boolean calculateStats) {
        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);

        try {
            int outDirId = catalogManager.getFileId(outDirStr);
            int fileId = catalogManager.getFileId(fileIdStr);
            if(outDirId < 0) {
                outDirId = catalogManager.getFileParent(fileId, null, sessionId).first().getId();
            }
            queryOptions.add(VariantStorageManager.Options.CALCULATE_STATS.key(), calculateStats);
            queryOptions.add(VariantStorageManager.Options.ANNOTATE.key(), annotate);
            QueryResult<Job> queryResult = analysisFileIndexer.index(fileId, outDirId, sessionId, queryOptions);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/fetch")
    @ApiOperation(value = "File fetch", notes = "DEPRECATED. Use .../files/{fileId}/[variants|alignments] or .../studies/{studyId}/[variants|alignments] instead", position = 15)
    @Deprecated
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
            int fileIdNum;
            File file;
            URI fileUri;

            try {
                fileIdNum = catalogManager.getFileId(fileId);
                QueryResult<File> queryResult = catalogManager.getFile(fileIdNum, sessionId);
                file = queryResult.getResult().get(0);
                fileUri = catalogManager.getFileUri(file);
            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse(e);
            }

//            if (!file.getType().equals(File.Type.INDEX)) {
            if (file.getIndex() == null || file.getIndex().getStatus() != Index.Status.READY) {
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
                dataStore = AnalysisFileIndexer.getDataStore(catalogManager, catalogManager.getStudyIdByFileId(file.getId()), file.getBioformat(), sessionId);
            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse(e);
            }
            String storageEngine = dataStore.getStorageEngine();
            String dbName = dataStore.getDbName();
            QueryResult result;
            switch (file.getBioformat()) {
                case ALIGNMENT: {
                    //TODO: getChunkSize from file.index.attributes?  use to be 200
                    int chunkSize = indexAttributes.getInt("coverageChunkSize", 200);
                    QueryOptions queryOptions = new QueryOptions();
                    queryOptions.put(AlignmentDBAdaptor.QO_FILE_ID, Integer.toString(fileIdNum));
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
                    QueryResult alignmentsByRegion;
                    if (histogram) {
                        if (regions.size() != 1) {
                            return createErrorResponse("", "Histogram fetch only accepts one region.");
                        }
                        alignmentsByRegion = dbAdaptor.getAllIntervalFrequencies(regions.get(0), queryOptions);
                    } else {
                        alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(regions, queryOptions);
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
                    QueryResult queryResult;
                    if (histogram) {
                        queryOptions.put("interval", interval);
                        queryResult = dbAdaptor.get(query, queryOptions);
//                    } else if (variantSource) {
//                        queryOptions.put("fileId", Integer.toString(fileIdNum));
//                        queryResult = dbAdaptor.getVariantSourceDBAdaptor().getAllSources(queryOptions);
                    } else if (!groupBy.isEmpty()) {
                        queryResult = dbAdaptor.groupBy(query, groupBy, queryOptions);
                    } else {
                        //With merge = true, will return only one result.
//                        queryOptions.put("merge", true);
//                        queryResult = dbAdaptor.getAllVariantsByRegionList(regions, queryOptions).get(0);
                        queryResult = dbAdaptor.get(query, queryOptions);
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

            result.setId(Integer.toString(fileIdNum));
            System.out.println("result = " + result);
            results.add(result);
        }
        System.out.println("results = " + results);
        return createOkResponse(results);
    }

    @GET
    @Path("/{fileId}/variants")
    @ApiOperation(value = "Fetch variants from a VCF/gVCF file", position = 15)
    public Response getVariants(@ApiParam(value = "", required = true) @PathParam("fileId") String fileIdCsv,
                                @ApiParam(value = "CSV list of variant ids") @QueryParam("ids") String ids,
                                @ApiParam(value = "CSV list of regions: {chr}:{start}-{end}") @QueryParam("region") String region,
                                @ApiParam(value = "CSV list of chromosomes") @QueryParam("chromosome") String chromosome,
                                @ApiParam(value = "CSV list of genes") @QueryParam("gene") String gene,
                                @ApiParam(value = "Variant type: [SNV, MNV, INDEL, SV, CNV]") @QueryParam("type") String type,
                                @ApiParam(value = "Filter by reference") @QueryParam("reference") String reference,
                                @ApiParam(value = "Filter by alternate") @QueryParam("alternate") String alternate,
//                                @ApiParam(value = "") @QueryParam("studies") String studies,
                                @ApiParam(value = "CSV list of studies to be returned") @QueryParam("returnedStudies") String returnedStudies,
                                @ApiParam(value = "CSV list of samples to be returned") @QueryParam("returnedSamples") String returnedSamples,
                                @ApiParam(value = "CSV list of files to be returned.") @QueryParam("returnedFiles") String returnedFiles,
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
                                @ApiParam(value = "Polyphen value: [<|>|<=|>=]{number}") @QueryParam("polyphen") String polyphen,
                                @ApiParam(value = "Sift value: [<|>|<=|>=]{number}") @QueryParam("sift") String sift,
//                                @ApiParam(value = "") @QueryParam("protein_substitution") String protein_substitution,
                                @ApiParam(value = "Conservation score: {conservation_score}[<|>|<=|>=]{number}") @QueryParam("conservation") String conservation,
                                @ApiParam(value = "Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("annot-population-maf") String annotPopulationMaf,
                                @ApiParam(value = "Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("alternate_frequency") String alternate_frequency,
                                @ApiParam(value = "Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("reference_frequency") String reference_frequency,
                                @ApiParam(value = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]") @QueryParam("unknownGenotype") String unknownGenotype,
                                @ApiParam(value = "Limit the number of returned variants. Max value: " + VariantFetcher.LIMIT_MAX) @DefaultValue(""+VariantFetcher.LIMIT_DEFAULT) @QueryParam("limit") int limit,
                                @ApiParam(value = "Skip some number of variants.") @QueryParam("skip") int skip,
                                @ApiParam(value = "Group variants by: [ct, gene, ensemblGene]", required = false) @DefaultValue("") @QueryParam("groupBy") String groupBy,
                                @ApiParam(value = "Count results", required = false) @QueryParam("count") boolean count,
                                @ApiParam(value = "Calculate histogram. Requires one region.", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                                @ApiParam(value = "Histogram interval size", required = false) @DefaultValue("2000") @QueryParam("interval") int interval,
                                @ApiParam(value = "Merge results", required = false) @DefaultValue("false") @QueryParam("merge") boolean merge) {

        List<QueryResult> results = new LinkedList<>();
        try {
            VariantFetcher variantFetcher = new VariantFetcher(catalogManager, storageManagerFactory);
            String[] splitFileId = fileIdCsv.split(",");
            for (String fileId : splitFileId) {
                QueryResult result;
                result = variantFetcher.variantsFile(region, histogram, groupBy, interval, fileId, sessionId, queryOptions);
                results.add(result);
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        return createOkResponse(results);
    }

    @GET
    @Path("/{fileId}/alignments")
    @ApiOperation(value = "Fetch alignments from a BAM file", position = 15)
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
    @ApiOperation(value = "Modify file", position = 16)
    public Response update(@PathParam(value = "fileId") String fileIdStr) {
        try {
            ObjectMap parameters = new ObjectMap();
            for (String param : params.keySet()) {
                if (param.equalsIgnoreCase("sid"))
                    continue;
                String value = params.get(param).get(0);
                parameters.put(param, value);
            }
            int fileId = catalogManager.getFileId(fileIdStr);
            QueryResult queryResult = catalogManager.modifyFile(fileId, parameters, sessionId);
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
        public String ownerId;
        public String creationDate;
        public String modificationDate;
        public String description;
        public Long diskUsage;
//        public int experimentId;
        public List<Integer> sampleIds;
        public Integer jobId;
        public Map<String, Object> stats;
        public Map<String, Object> attributes;
    }

    @POST
    @Path("/{fileId}/update")
    @ApiOperation(value = "Modify file", position = 16)
    public Response updatePOST(@PathParam(value = "fileId") String fileIdStr,
                               @ApiParam(name = "params", value = "Parameters to modify", required = true) UpdateFile params) {
        try {
            int fileId = catalogManager.getFileId(fileIdStr);
            QueryResult queryResult = catalogManager.modifyFile(fileId, new ObjectMap(jsonObjectMapper.writeValueAsString(params)), sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/share")
    @ApiOperation(value = "Share file with other user", position = 16)
    public Response share(@PathParam(value = "fileId") String fileIdStr,
                          @ApiParam(value = "User you want to share the file with. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @DefaultValue("") @QueryParam("userId") String userId,
                          @ApiParam(value = "Remove the previous AclEntry", required = false) @DefaultValue("false") @QueryParam("unshare") boolean unshare,
                          @ApiParam(value = "Read permission", required = false) @DefaultValue("false") @QueryParam("read") boolean read,
                          @ApiParam(value = "Write permission", required = false) @DefaultValue("false") @QueryParam("write") boolean write,
                          @ApiParam(value = "Delete permission", required = false) @DefaultValue("false") @QueryParam("delete") boolean delete
                          /*@ApiParam(value = "Execute permission", required = false) @DefaultValue("false") @QueryParam("execute") boolean execute*/) {
        try {
            int fileId = catalogManager.getFileId(fileIdStr);
            QueryResult queryResult;
            if (unshare) {
                queryResult = catalogManager.unshareFile(fileId, userId, sessionId);
            } else {
                queryResult = catalogManager.shareFile(fileId, new AclEntry(userId, read, write, false, delete), sessionId);
            }
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/link")
    @ApiOperation(value = "Link an external file into catalog.", position = 17)
    public Response link(@ApiParam(required = true) @QueryParam("uri") String uriStr,
                         @ApiParam(required = true) @QueryParam("studyId") String studyIdStr,
                         @ApiParam(required = true) @QueryParam("path") String path,
                         @ApiParam(required = false) @DefaultValue("") @QueryParam("description") String description,
                         @ApiParam(required = false) @DefaultValue("false") @QueryParam("parents") boolean parents,
                         @ApiParam(required = false) @DefaultValue("false") @QueryParam("calculateChecksum") boolean calculateChecksum ) {
        try {
            URI uri = UriUtils.createUri(uriStr);
            File file;
            CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);
            int studyId = catalogManager.getStudyId(studyIdStr);
            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(uri);
            if (!ioManager.exists(uri)) {
                throw new CatalogIOException("File " + uri + " does not exist");
            }
            if (ioManager.isDirectory(uri)) {
                uri = UriUtils.createDirectoryUri(uriStr);
                file = catalogFileUtils.linkFolder(studyId, path, parents, description, calculateChecksum, uri, false, false, sessionId);
                new FileScanner(catalogManager).scan(file, null, FileScanner.FileScannerPolicy.REPLACE, calculateChecksum, false, sessionId);
            } else {
                final String filePath;
                if (path.endsWith("/")) {
                    filePath = path + Paths.get(uri.getPath()).getFileName().toString();
                } else {
                    int folders = catalogManager.getAllFiles(studyId, new QueryOptions(CatalogFileDBAdaptor.FileFilterOption.path.toString(), path + "/"), sessionId).getNumResults();
                    if (folders != 0) {
                        filePath = path + "/" + Paths.get(uri.getPath()).getFileName().toString();
                    } else {
                        filePath = path;
                    }
                }
                file = catalogManager.createFile(studyId, null, null,
                        filePath, description, parents, -1, sessionId).first();
                file = catalogFileUtils.link(file, calculateChecksum, uri, false, false, sessionId);
                file = FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, queryOptions, sessionId, false);
            }
            return createOkResponse(new QueryResult<>("link", 0, 1, 1, null, null, Collections.singletonList(file)));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{fileId}/relink")
    @ApiOperation(value = "Change file location. Provided file must be either STAGE or be an external file.", position = 17)
    public Response relink(@ApiParam(value = "File ID") @PathParam("fileId") @DefaultValue("") String fileIdStr,
                           @ApiParam(value = "new URI" ,required = true) @QueryParam("uri") String uriStr,
                           @ApiParam(value = "Do calculate checksum for new files", required = false) @DefaultValue("false") @QueryParam("calculateChecksum") boolean calculateChecksum ) {
        try {
            URI uri = UriUtils.createUri(uriStr);
            CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().get(uri);

            if (!ioManager.exists(uri)) {
                throw new CatalogIOException("File " + uri + " does not exist");
            }

            int fileId = catalogManager.getFileId(fileIdStr);
            File file = catalogManager.getFile(fileId, sessionId).first();

            new CatalogFileUtils(catalogManager).link(file, calculateChecksum, uri, false, true, sessionId);
            file = catalogManager.getFile(file.getId(), queryOptions, sessionId).first();
            file = FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, queryOptions, sessionId, false);

            return createOkResponse(new QueryResult<>("relink", 0, 1, 1, null, null, Collections.singletonList(file)));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{fileId}/refresh")
    @ApiOperation(value = "Refresh metadata from the selected file or folder. Return updated files.", position = 17)
    public Response refresh(@PathParam(value = "fileId") @DefaultValue("") String fileIdStr) {
        try {
            int fileId = catalogManager.getFileId(fileIdStr);
            File file = catalogManager.getFile(fileId, sessionId).first();

            List<File> files;
            CatalogFileUtils catalogFileUtils = new CatalogFileUtils(catalogManager);
            FileMetadataReader fileMetadataReader = FileMetadataReader.get(catalogManager);
            if (file.getType() == File.Type.FILE) {
                File file1 = catalogFileUtils.checkFile(file, false, sessionId);
                file1 = fileMetadataReader.setMetadataInformation(file1, null, queryOptions, sessionId, false);
                if (file == file1) {    //If the file is the same, it was not modified. Only return modified files.
                    files = Collections.emptyList();
                } else {
                    files = Collections.singletonList(file);
                }
            } else {
                List<File> result = catalogManager.getAllFilesInFolder(file.getId(), null, sessionId).getResult();
                files = new ArrayList<>(result.size());
                for (File f : result) {
                    File file1 = fileMetadataReader.setMetadataInformation(f, null, queryOptions, sessionId, false);
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
    @ApiOperation(value = "Delete file", position = 17)
    public Response deleteGET(@PathParam(value = "fileId") @DefaultValue("") String fileIdStr) {
        try {
            int fileId = catalogManager.getFileId(fileIdStr);
            QueryResult result = catalogManager.deleteFile(fileId, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
