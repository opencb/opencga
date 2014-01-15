package org.opencb.opencga.server.ws;

import com.google.common.base.Splitter;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.opencga.account.beans.ObjectItem;
import org.opencb.opencga.account.db.AccountManagementException;
import org.opencb.opencga.lib.common.IOUtils;
import org.opencb.opencga.lib.common.StringUtils;
import org.opencb.opencga.lib.common.TimeUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.opencb.commons.containers.QueryResult;

@Path("/account/{accountId}/storage/{bucketId}/{objectId}")
public class StorageWSServer extends GenericWSServer {
    private String accountId;
    private String bucketId;
    private java.nio.file.Path objectId;

    public StorageWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                           @DefaultValue("") @PathParam("accountId") String accountId,
                           @DefaultValue("") @PathParam("bucketId") String bucketId,
                           @DefaultValue("") @PathParam("objectId") String objectId) throws IOException, AccountManagementException {
        super(uriInfo, httpServletRequest);
        this.accountId = accountId;
        this.bucketId = bucketId;
        this.objectId = StringUtils.parseObjectId(objectId);
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadObject(@FormDataParam("file") InputStream fileIs,
                                 @FormDataParam("file") FormDataContentDisposition fileInfo,
                                 @DefaultValue("undefined") @FormDataParam("name") String name, @FormDataParam("fileFormat") String fileFormat,
                                 @DefaultValue("r") @QueryParam("filetype") String filetype,
                                 @DefaultValue("-") @FormDataParam("responsible") String responsible,
                                 @DefaultValue("-") @FormDataParam("organization") String organization,
                                 @DefaultValue("-") @FormDataParam("date") String date,
                                 @DefaultValue("-") @FormDataParam("description") String description,
                                 @DefaultValue("-1") @FormDataParam("jobid") String jobid,
                                 @DefaultValue("false") @QueryParam("parents") boolean parents) {

        System.out.println(bucketId);
        System.out.println(objectId);
        System.out.println(parents);

        ObjectItem objectItem = new ObjectItem(null, null, null);// TODO PAKO
        // COMPROBAR
        // CONSTRUCTOR
        objectItem.setFileFormat(fileFormat);
        objectItem.setFileType(filetype);
        objectItem.setResponsible(responsible);
        objectItem.setOrganization(organization);
        objectItem.setDate(TimeUtils.getTime());
        objectItem.setDescription(description);

        try {
            String res = cloudSessionManager.createObjectToBucket(accountId, bucketId, objectId, objectItem, fileIs,
                    parents, sessionId);
            return createOkResponse(res);
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
    }

    /**
     * Object methods
     * ***************************
     */

    @GET
    @Path("/create_directory")
    public Response createDirectory(@DefaultValue("false") @QueryParam("parents") boolean parents) {
        ObjectItem objectItem = new ObjectItem(null, null, null);
        objectItem.setFileType("dir");
        objectItem.setDate(TimeUtils.getTime());
        try {
            String res = cloudSessionManager.createFolderToBucket(accountId, bucketId, objectId, objectItem, parents,
                    sessionId);
            return createOkResponse(res);
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/delete")
    public Response deleteData() {
        try {
            cloudSessionManager.deleteDataFromBucket(accountId, bucketId, objectId, sessionId);
            return createOkResponse("OK");
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
    }

    @GET
    @Path("/fetch")
    @Produces(MediaType.APPLICATION_JSON)
    public Response fetchData(@DefaultValue("") @PathParam("objectId") String objectIdFromURL,
                            @DefaultValue("") @QueryParam("region") String regionStr) {
        try {
            // Validate user and object permissions, and retrieve paths
            java.nio.file.Path objectPath = cloudSessionManager.getObjectPath(accountId, bucketId, objectId);
            ObjectItem objectItem = cloudSessionManager.getObjectFromBucket(accountId, bucketId, objectId, sessionId);
            
            List<String> regions = Splitter.on(',').splitToList(regionStr);
            
            switch (objectItem.getFileFormat().toLowerCase()) {
                case "bam":
                    List<QueryResult> bamResults = new ArrayList<>();
                    for (String region : regions) {
                        bamResults.add(cloudSessionManager.fetchAlignmentData(objectPath, region, params));
                    }
                    return createOkResponse(bamResults);
                case "vcf":
//                    List<String> vcfResults = new ArrayList<>();
                    List<QueryResult> vcfResults = new ArrayList<>();
                    for (String region : regions) {
                        vcfResults.add(cloudSessionManager.fetchVariationData(objectPath, region, params));
                    }
//                    return createOkResponse(vcfResults.toString());
                    return createOkResponse(vcfResults);
            }
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
        
        return createErrorResponse("Data not found with given arguments");
    }

    /**
     * ***************
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/chunk_upload")
    public Response chunkUpload(@FormDataParam("chunk_content") byte[] chunkBytes,
                                @FormDataParam("chunk_content") FormDataContentDisposition contentDisposition,
                                @DefaultValue("") @FormDataParam("chunk_id") String chunk_id,
                                @DefaultValue("") @FormDataParam("filename") String filename,
                                @DefaultValue("") @FormDataParam("fileFormat") String fileFormat,
                                @DefaultValue("") @FormDataParam("object_id") String objectIdFromURL,
                                @DefaultValue("") @FormDataParam("bucket_id") String bucketId,
                                @DefaultValue("") @FormDataParam("last_chunk") String last_chunk,
                                @DefaultValue("") @FormDataParam("chunk_total") String chunk_total,
                                @DefaultValue("") @FormDataParam("chunk_size") String chunk_size,
                                @DefaultValue("") @FormDataParam("chunk_hash") String chunkHash,
                                @DefaultValue("false") @FormDataParam("resume_upload") String resume_upload) {

        long t = System.currentTimeMillis();

        java.nio.file.Path filePath = cloudSessionManager.getObjectPath(accountId, bucketId, objectId);
        java.nio.file.Path completedFilePath = filePath.getParent().resolve("_" + objectId);
        java.nio.file.Path folderPath = filePath.getParent().resolve("__" + objectId);

        logger.info(objectIdFromURL + "");
        logger.info(folderPath + "");
        logger.info(filePath + "");
        boolean resume = Boolean.parseBoolean(resume_upload);

        try {
            logger.info("---resume is: " + resume);
            if (resume) {
                logger.info("Resume ms :" + (System.currentTimeMillis() - t));
                return createOkResponse(getResumeFileJSON(folderPath).toString());
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
                    ObjectItem objectItem = new ObjectItem(null, null, null);// TODO PAKO
                    // COMPROBAR
                    // CONSTRUCTOR
                    objectItem.setFileFormat(fileFormat);
                    objectItem.setFileType("r");
                    objectItem.setResponsible("");
                    objectItem.setOrganization("");
                    objectItem.setDate(TimeUtils.getTime());
                    objectItem.setDescription("");

                    String res = cloudSessionManager.createObjectToBucket(accountId, bucketId, objectId, objectItem, Files.newInputStream(completedFilePath),
                            false, sessionId);
                    IOUtils.deleteDirectory(completedFilePath);
                    return createOkResponse(res);
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

    private StringBuilder getResumeFileJSON(java.nio.file.Path folderPath) throws IOException {
        StringBuilder sb = new StringBuilder();

        if (!Files.exists(folderPath)) {
            sb.append("{}");
            return sb;
        }

        String c = "\"";
        DirectoryStream<java.nio.file.Path> folderStream = Files.newDirectoryStream(folderPath, "*_partial");
        sb.append("{");
        for (java.nio.file.Path partPath : folderStream) {
            String[] nameSplit = partPath.getFileName().toString().split("_");
            sb.append(c + nameSplit[0] + c + ":{");
            sb.append(c + "size" + c + ":" + nameSplit[1]);
            // sb.append(c + "hash" + c + ":" + c + nameSplit[2] + c);
            sb.append("},");
        }
        // Remove last comma
        if (sb.length() > 1) {
            sb.replace(sb.length() - 1, sb.length(), "");
        }
        sb.append("}");
        return sb;
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


    /**
     * Index methods
     * ***************************
     */

    @GET
    @Path("/index")
    public Response index(@DefaultValue("false") @QueryParam("force") boolean force) throws Exception {
        try {
            String res = cloudSessionManager.indexFileObject(accountId, bucketId, objectId, force, sessionId);
            return createOkResponse(res);
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse("job id not found.");
        }

    }

    @GET
    @Path("/index_status")
    public Response indexStatus(@DefaultValue("") @QueryParam("indexerid") String indexerId) throws Exception {
        try {
            String res = cloudSessionManager.indexFileObjectStatus(accountId, bucketId, objectId, sessionId, indexerId);
            return createOkResponse(res);
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse("indexer id not found.");
        }

    }
}