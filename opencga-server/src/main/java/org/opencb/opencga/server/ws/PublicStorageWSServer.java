package org.opencb.opencga.server.ws;

import com.google.common.base.Splitter;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.account.db.AccountManagementException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.StringUtils;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
@Path("/public/storage/{bucketId}/{objectId}")
@Produces(MediaType.APPLICATION_JSON)
public class PublicStorageWSServer extends GenericWSServer {

    private String bucketId;
    private java.nio.file.Path objectId;
    
    public PublicStorageWSServer(UriInfo uriInfo, HttpServletRequest httpServletRequest) throws IOException {
        super(uriInfo, httpServletRequest);
    }
    

    public PublicStorageWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                @DefaultValue("") @PathParam("bucketId") String bucketId,
                                @DefaultValue("") @PathParam("objectId") String objectId) 
            throws IOException, AccountManagementException {
        super(uriInfo, httpServletRequest);
        this.bucketId = bucketId;
        this.objectId = StringUtils.parseObjectId(objectId);
    }
    
    
    // TODO for now, only region filter allowed
    @GET
    @Path("/fetch")
    public Response fetchData(@DefaultValue("") @PathParam("objectId") String objectIdFromURL,
                           @DefaultValue("") @QueryParam("region") String regionStr) {
        try {
            Properties storageProperties = Config.getStorageProperties(Config.getGcsaHome());
            if (storageProperties == null) {
                return createErrorResponse("\"storage.properties\" file not found!");
            }
            
            // 1: Check if the bucket is in the list of allowed folders
            if (!isBucketAllowed(storageProperties)) {
                throw new IllegalArgumentException("Reading the files inside the requested folder is forbidden");
            }
            
            String bucketPathName = storageProperties.getProperty("OPENCGA.LOCAL.FOLDERS." + bucketId, "");
            java.nio.file.Path bucketPath = Paths.get(bucketPathName);
            java.nio.file.Path objectPath = Paths.get(bucketPathName, objectId.toString());
            System.out.println("* Bucket path = " + bucketPath.toString());
            System.out.println("* Object path = " + objectPath.toString());
            
            // 2: Check if the path structure is valid (stays inside the working folder)
            if (!isValidPathStructure(bucketPath, objectPath)) {
                throw new IllegalArgumentException("The specified file path is not valid");
            }
            
            // 3: Check if the path corresponding to alias folder + filename exists
            if (!storageProperties.containsKey("OPENCGA.LOCAL.FOLDERS." + bucketId)) {
                throw new IllegalStateException("There is no path defined for the requested folder ID");
            }
            FileUtils.checkFile(objectPath); // Check file exists and is readable
            
            // 4: Check if the file format is allowed for that alias
            if (!isFileFormatAllowed(Files.getFileExtension(objectPath.toString()), storageProperties)) {
                throw new IllegalArgumentException("Reading this file format inside the requested folder is forbidden");
            }
            
            // 5: Launch queries depending of file format
            List<String> regions = Splitter.on(',').splitToList(regionStr);
            
            switch (Files.getFileExtension(objectPath.toString())) {
                case "bam":
                    List<QueryResult> bamResults = new ArrayList<>();
                    for (String region : regions) {
                        bamResults.add(cloudSessionManager.fetchAlignmentData(objectPath, region, params));
                    }
                    return createOkResponse(bamResults);
                case "vcf":
                    List<String> vcfResults = new ArrayList<>();
                    for (String region : regions) {
                        vcfResults.add(cloudSessionManager.fetchVariationData(objectPath, region, params));
                    }
                    return createOkResponse(vcfResults.toString());
            }
            
        } catch (Exception e) {
            logger.error(e.toString());
            return createErrorResponse(e.getMessage());
        }
        
        return createErrorResponse("Data not found with given arguments");
    }


    private boolean isBucketAllowed(Properties storageProperties) throws IllegalStateException {
        if (!storageProperties.containsKey("OPENCGA.LOCAL.FOLDERS.ALLOWED")) {
            throw new IllegalStateException("List of folders with read-access not available. Please check your \"storage.properties\" file.");
        }
        String[] buckets = storageProperties.getProperty("OPENCGA.LOCAL.FOLDERS.ALLOWED", "").split(",");
        boolean bucketAllowed = false;
        for (String bucket : buckets) {
            if (bucket.equalsIgnoreCase(bucketId)) {
                bucketAllowed = true;
                break;
            }
        }
        
        return bucketAllowed;
    }

    private boolean isValidPathStructure(java.nio.file.Path bucketPath, java.nio.file.Path filePath) throws IOException {
        File parent = bucketPath.toFile().getCanonicalFile();
        File child = filePath.toFile().getCanonicalFile();
        
        while (child != null) {
            if (child.equals(parent)) {
                return true;
            }
            child = child.getParentFile();
        }
        
        return false;
    }
    
    private boolean isFileFormatAllowed(String fileFormat, Properties storageProperties) {
        String[] fileFormatsAllowed = new String[0]; // No formats allowed if none specified
        if (storageProperties.containsKey("OPENCGA.LOCAL.EXTENSIONS." + bucketId)) {
            fileFormatsAllowed = storageProperties.getProperty("OPENCGA.LOCAL.EXTENSIONS." + bucketId, "").split(",");
        } else if (storageProperties.containsKey("OPENCGA.LOCAL.EXTENSIONS.ALLOWED")) {
            fileFormatsAllowed = storageProperties.getProperty("OPENCGA.LOCAL.EXTENSIONS.ALLOWED", "").split(",");
        }
        
        logger.debug("Bucket extensions: " + storageProperties.getProperty("OPENCGA.LOCAL.EXTENSIONS." + bucketId));
        logger.debug("Global extensions: " + storageProperties.getProperty("OPENCGA.LOCAL.EXTENSIONS.ALLOWED"));
        
        boolean formatAllowed = false;
        for (String format : fileFormatsAllowed) {
            if (format.equalsIgnoreCase(fileFormat)) {
                formatAllowed = true;
                break;
            }
        }
        return formatAllowed;
    }

}
