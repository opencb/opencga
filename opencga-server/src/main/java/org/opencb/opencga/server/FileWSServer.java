package org.opencb.opencga.server;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.biodata.models.feature.Region;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisFileIndexer;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Index;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.SgeManager;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentQueryBuilder;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

@Path("/files")
@Api(value = "files", description = "files")
public class FileWSServer extends OpenCGAWSServer {



    public FileWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest)
            throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
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
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/upload")
    @Produces("application/json")
    @ApiOperation(httpMethod = "POST", value = "Resource to upload a file by chunks", response = QueryResponse.class, nickname="chunkUpload")
    public Response chunkUpload(@FormDataParam("chunk_content") byte[] chunkBytes,
                                @FormDataParam("chunk_content") FormDataContentDisposition contentDisposition,
                                @DefaultValue("") @FormDataParam("chunk_id") String chunk_id,
                                @DefaultValue("") @FormDataParam("last_chunk") String last_chunk,
                                @DefaultValue("") @FormDataParam("chunk_total") String chunk_total,
                                @DefaultValue("") @FormDataParam("chunk_size") String chunk_size,
                                @DefaultValue("") @FormDataParam("chunk_hash") String chunkHash,
                                @DefaultValue("false") @FormDataParam("resume_upload") String resume_upload,


                                @ApiParam(value = "filename", required = true) @DefaultValue("") @FormDataParam("filename") String filename,
                                @ApiParam(value = "fileFormat", required = true) @DefaultValue("") @FormDataParam("fileFormat") String fileFormat,
                                @ApiParam(value = "bioFormat", required = true) @DefaultValue("") @FormDataParam("bioFormat") String bioFormat,
                                @ApiParam(value = "userId", required = true) @DefaultValue("") @FormDataParam("userId") String userId,
                                @ApiParam(value = "projectId", required = true) @DefaultValue("") @FormDataParam("projectId") String projectId,
                                @ApiParam(value = "studyId", required = true) @FormDataParam("studyId") int studyId,
                                @ApiParam(value = "relativeFilePath", required = true) @DefaultValue("") @FormDataParam("relativeFilePath") String relativeFilePath,
                                @ApiParam(value = "description", required = true) @DefaultValue("") @FormDataParam("description") String description,
                                @ApiParam(value = "parents", required = true) @DefaultValue("true") @FormDataParam("parents") boolean parents) {

        long t = System.currentTimeMillis();

        java.nio.file.Path filePath = null;
        try {
            filePath = Paths.get(catalogManager.getFileUri(userId, projectId, String.valueOf(studyId), relativeFilePath));
            System.out.println(filePath);
        } catch (CatalogIOManagerException e) {
            System.out.println("catalogManager.getFilePath");
            e.printStackTrace();
        }

        java.nio.file.Path completedFilePath = filePath.getParent().resolve("_" + relativeFilePath);
        java.nio.file.Path folderPath = filePath.getParent().resolve("__" + relativeFilePath);


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

            // WRITE CHUNK FILE
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
            }

            if (lastChunk) {
                logger.info("lastChunk is true...");
                Files.createFile(completedFilePath);
                List<java.nio.file.Path> chunks = getSortedChunkList(folderPath);
                logger.info("----ordered chunks length: " + chunks.size());
                for (java.nio.file.Path partPath : chunks) {
                    logger.info(partPath.getFileName().toString());
                    Files.write(completedFilePath, Files.readAllBytes(partPath), StandardOpenOption.APPEND);
                }
                IOUtils.deleteDirectory(folderPath);
                try {

                    QueryResult queryResult = catalogManager.uploadFile(studyId, fileFormat, bioFormat, relativeFilePath, description, parents, Files.newInputStream(completedFilePath), sessionId);
                    IOUtils.deleteDirectory(completedFilePath);

                    return createOkResponse(queryResult);
                } catch (Exception e) {
                    logger.error(e.toString());
                    return createErrorResponse(e.getMessage());
                }
            }

        } catch (IOException e) {

            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        logger.info("chunk saved ms :" + (System.currentTimeMillis() - t));
        return createOkResponse("ok");
    }

    @GET
    @Path("/create-folder")
    @Produces("application/json")
    @ApiOperation(value = "Create folder")
    public Response createFolder(@ApiParam(value = "studyId", required = true) @QueryParam("studyId") int studyId,
                                 @ApiParam(value = "folder", required = true) @QueryParam("folder") String folder
    ) {

        java.nio.file.Path folderPath = Paths.get(folder);
        boolean parents = true;

        QueryResult queryResult;
        try {
            queryResult = catalogManager.createFolder(studyId, folderPath, parents, sessionId);
            return createOkResponse(queryResult);
        } catch (CatalogManagerException | CatalogIOManagerException  e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{fileId}/info")
    @Produces("application/json")
    @ApiOperation(value = "File info")
    public Response info(@PathParam(value = "fileId") @DefaultValue("") @FormDataParam("fileId") String fileId
    ) {
        String[] splitedFileId = fileId.split(",");
        try {
            List<QueryResult> results = new LinkedList<>();
            for (String id : splitedFileId) {
                results.add(catalogManager.getFile(catalogManager.getFileId(id), sessionId));
            }
            return createOkResponse(results);
        } catch (CatalogManagerException | CatalogIOManagerException | IOException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/search")
    @Produces("application/json")
    @ApiOperation(value = "File info")
    public Response search(@ApiParam(value = "name", required = false)       @DefaultValue("") @QueryParam("name") String name,
                           @ApiParam(value = "studyId", required = true)     @DefaultValue("") @QueryParam("studyId") String studyId,
                           @ApiParam(value = "type", required = false)       @DefaultValue("") @QueryParam("type") String type,
                           @ApiParam(value = "bioformat", required = false)  @DefaultValue("") @QueryParam("bioformat") String bioformat,
                           @ApiParam(value = "maxSize", required = false)    @DefaultValue("") @QueryParam("maxSize") String maxSize,
                           @ApiParam(value = "minSize", required = false)    @DefaultValue("") @QueryParam("minSize") String minSize,
                           @ApiParam(value = "startDate", required = false)  @DefaultValue("") @QueryParam("startDate") String startDate,
                           @ApiParam(value = "endDate", required = false)    @DefaultValue("") @QueryParam("endDate") String endDate,
                           @ApiParam(value = "like", required = false)       @DefaultValue("") @QueryParam("like") String like,
                           @ApiParam(value = "startsWith", required = false) @DefaultValue("") @QueryParam("startsWith") String startsWith,
                           @ApiParam(value = "directory", required = false)  @DefaultValue("") @QueryParam("directory") String directory,
                           @ApiParam(value = "indexJobId", required = false) @DefaultValue("") @QueryParam("indexJobId") String indexJobId

    ) {
        try {
            int studyIdNum = catalogManager.getStudyId(studyId);
            QueryOptions queryOptions = new QueryOptions();
            if( !name.isEmpty() )       { queryOptions.put("name", name); }
            if( !type.isEmpty() )       { queryOptions.put("type", type); }
            if( !bioformat.isEmpty() )  { queryOptions.put("bioformat", bioformat); }
            if( !maxSize.isEmpty() )    { queryOptions.put("maxSize", maxSize); }
            if( !minSize.isEmpty() )    { queryOptions.put("minSize", minSize); }
            if( !startDate.isEmpty() )  { queryOptions.put("startDate", startDate); }
            if( !endDate.isEmpty() )    { queryOptions.put("endDate", endDate); }
            if( !like.isEmpty() )       { queryOptions.put("like", like); }
            if( !startsWith.isEmpty() ) { queryOptions.put("startsWith", startsWith); }
            if( !directory.isEmpty() )  { queryOptions.put("directory", directory); }
            if( !indexJobId.isEmpty() ) { queryOptions.put("indexJobId", indexJobId); }


            QueryResult<File> result = catalogManager.searchFile(studyIdNum, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{fileId}/list")
    @Produces("application/json")
    @ApiOperation(value = "List folder")
    public Response list(@PathParam(value = "fileId") @DefaultValue("") @FormDataParam("fileId") String fileId
    ) {
        try {
            int fileIdNum = catalogManager.getFileId(fileId);
            QueryResult result = catalogManager.getAllFilesInFolder(fileIdNum, sessionId);
            return createOkResponse(result);
        } catch (CatalogManagerException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/{fileId}/index")
    @Produces("application/json")
    @ApiOperation(value = "File index")
    public Response index(@PathParam(value = "fileId") @DefaultValue("") @FormDataParam("fileId") String fileIdStr,
                          @ApiParam(value = "outdir", required = true) @DefaultValue("-1") @QueryParam("outdir") String outDirStr,
                          @ApiParam(value = "backend", required = false) @DefaultValue("") @QueryParam("backend") String backend
                          ) {
        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager, properties);
        Index index = null;
        if(backend.isEmpty()) {
            backend = "mongo"; //TODO: Get default backend from properties.
        }
        try {
            int outDirId = catalogManager.getFileId(outDirStr);
            int fileId   = catalogManager.getFileId(fileIdStr);
            index = analysisFileIndexer.index(fileId, outDirId, backend, sessionId, queryOptions);
        } catch (CatalogManagerException | CatalogIOManagerException | IOException e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
        return createOkResponse(index);
    }


    @GET
    @Path("/index-status")
    @Produces("application/json")
    @ApiOperation(value = "File index status")
    public Response indexStatus(@ApiParam(value = "jobId", required = true) @DefaultValue("") @QueryParam("jobId") String jobId
    ) {
        String status;
        try {
            status = SgeManager.status(jobId);
        } catch (Exception e) {
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
        return createOkResponse(status);
    }


    @GET
    @Path("/{fileId}/fetch")
    @Produces("application/json")
    @ApiOperation(value = "File fetch")
    public Response fetch(@PathParam(value = "fileId") @DefaultValue("") @FormDataParam("fileId") String fileIds,
                          @ApiParam(value = "region", required = true) @DefaultValue("") @QueryParam("region") String region,
                          @ApiParam(value = "backend", required = false) @DefaultValue("") @QueryParam("backend") String backend,
                          @ApiParam(value = "view_as_pairs", required = false) @DefaultValue("false") @QueryParam("view_as_pairs") boolean view_as_pairs,
                          @ApiParam(value = "include_coverage", required = false) @DefaultValue("true") @QueryParam("include_coverage") boolean include_coverage,
                          @ApiParam(value = "process_differences", required = false) @DefaultValue("true") @QueryParam("process_differences") boolean process_differences,
                          @ApiParam(value = "histogram", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                          @ApiParam(value = "interval", required = false) @DefaultValue("2000") @QueryParam("interval") int interval
    ) {
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
            } catch (CatalogManagerException | CatalogIOManagerException | IOException e) {
                e.printStackTrace();
                return createErrorResponse(e.getMessage());
            }

            if(backend.isEmpty()) {
                backend = "mongo";  //TODO: Get default backend from properties.
            }

            List<Index> indices = file.getIndices();
            Index index = null;
            for (Index i : indices) {
                if (i.getBackend().equals(backend)) {
                    index = i;
                }
            }
            if (index == null || !index.getState().equals(Index.INDEXED)) {
                return createErrorResponse("File {id:" + file.getId() + " name:'" + file.getName() + "'} " +
                        " is not indexed in the selected backend.");
            }
            ObjectMap indexAttributes = new ObjectMap(index.getAttributes());
            QueryResult result;
            switch (file.getBioformat()) {
                case "bam": {
                    //TODO: getChunkSize from file.index.attributes?  use to be 200
                    int chunkSize = indexAttributes.getInt("coverageChunkSize", 200);
                    QueryOptions queryOptions = new QueryOptions();
                    queryOptions.put(AlignmentQueryBuilder.QO_FILE_ID, Integer.toString(fileIdNum));
                    queryOptions.put(AlignmentQueryBuilder.QO_BAM_PATH, fileUri.getPath());     //TODO: Make uri-compatible
                    queryOptions.put(AlignmentQueryBuilder.QO_VIEW_AS_PAIRS, view_as_pairs);
                    queryOptions.put(AlignmentQueryBuilder.QO_INCLUDE_COVERAGE, include_coverage);
                    queryOptions.put(AlignmentQueryBuilder.QO_PROCESS_DIFFERENCES, process_differences);
                    queryOptions.put(AlignmentQueryBuilder.QO_INTERVAL_SIZE, interval);
                    queryOptions.put(AlignmentQueryBuilder.QO_HISTOGRAM, histogram);
                    queryOptions.put(AlignmentQueryBuilder.QO_COVERAGE_CHUNK_SIZE, chunkSize);

                    if(indexAttributes.containsKey("baiFileId")) {
                        File baiFile = null;
                        try {
                            baiFile = catalogManager.getFile(indexAttributes.getInt("baiFileId"), sessionId).getResult().get(0);
                            URI baiUri = catalogManager.getFileUri(baiFile);
                            queryOptions.put(AlignmentQueryBuilder.QO_BAI_PATH, baiUri.getPath());  //TODO: Make uri-compatible
                        } catch (IOException | CatalogManagerException | CatalogIOManagerException e) {
                            e.printStackTrace();
                            logger.error("Can't obtain bai file for file " + fileIdNum, e);
                        }
                    }

                    AlignmentQueryBuilder dbAdaptor;
                    try {
                        AlignmentStorageManager alignmentStorageManager = StorageManagerFactory.getAlignmentStorageManager(backend);
                        dbAdaptor = alignmentStorageManager.getDBAdaptor(index.getDbName());
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                        return createErrorResponse(e.getMessage());
                    }
                    QueryResult alignmentsByRegion;
                    if (histogram) {
                        if(regions.size() != 1) {
                            return createErrorResponse("Histogram fetch only accepts one region.");
                        }
                        alignmentsByRegion = dbAdaptor.getAllIntervalFrequencies(regions.get(0), queryOptions);
                    } else {
                        alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(regions, queryOptions);
                    }
                    result = alignmentsByRegion;
                    break;
                }

                case "vcf": {
                    QueryOptions queryOptions = new QueryOptions();
                    queryOptions.put("interval", interval);
                    queryOptions.put("merge", true);
                    queryOptions.put("files", Arrays.asList(Integer.toString(fileIdNum)));
//                    queryOptions.put("exclude", Arrays.asList(exclude.split(",")));
//                    queryOptions.put("include", Arrays.asList(include.split(",")));

                    //java.nio.file.Path configPath = Paths.get(Config.getGcsaHome(), "config", "application.properties");
                    VariantDBAdaptor dbAdaptor;
                    try {
                        dbAdaptor = StorageManagerFactory.getVariantStorageManager(backend).getDBAdaptor(null);
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                        return createErrorResponse(e.getMessage());
                    }
                    QueryResult variantsByRegion;
                    if (histogram) {
                        variantsByRegion = dbAdaptor.getVariantsHistogramByRegion(regions.get(0), queryOptions);
                    } else {
                        //With merge = true, will return only one result.
                        variantsByRegion = dbAdaptor.getAllVariantsByRegionList(regions, queryOptions).get(0);
                    }
                    result = variantsByRegion;
                    break;

                }
                default:
                    return createErrorResponse("Unknown bioformat '" + file.getBioformat() + '\'');
            }

            result.setId(Integer.toString(fileIdNum));
            System.out.println("result = " + result);
            results.add(result);
        }
        System.out.println("results = " + results);
        return createOkResponse(results);
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
                logger.info(id_o1 + "");
                logger.info(id_o2 + "");
                return id_o1 - id_o2;
            }
        });
        return files;
    }
}
