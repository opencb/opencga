package org.opencb.opencga.server;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.IOUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Path("/files")
@Api(value = "files", description = "files")
public class FileWSServer extends OpenCGAWSServer {

    public FileWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        super(version, uriInfo, httpServletRequest);
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/upload")
    @Produces("application/json")
    @ApiOperation(value = "Upload a file to an study")
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
                                @ApiParam(value = "parents", required = true) @DefaultValue("true") @FormDataParam("parents") boolean parents,


    ) {

        long t = System.currentTimeMillis();

        java.nio.file.Path filePath = null;
        try {
            filePath = catalogManager.getFilePath(userId, projectId, String.valueOf(studyId), relativeFilePath);
        } catch (CatalogIOManagerException e) {
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
//                    ObjectItem objectItem = new ObjectItem(null, null, null);// TODO PAKO
//                    // COMPROBAR
//                    // CONSTRUCTOR
//                    objectItem.setFileFormat(fileFormat);
//                    objectItem.setFileType("r");
//                    objectItem.setResponsible("");
//                    objectItem.setOrganization("");
//                    objectItem.setDate(TimeUtils.getTime());
//                    objectItem.setDescription("");


                    QueryResult queryResult = catalogManager.uploadFile(
                            studyId, fileFormat, bioFormat, relativeFilePath, description, parents, Files.newInputStream(completedFilePath), sessionId
                    );


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
}
