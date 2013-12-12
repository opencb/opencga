package org.opencb.opencga.server.ws;

import com.google.common.base.Splitter;
import com.google.common.io.Files;
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
            // CSM:     
            //   * validate user and object permissions
            //   * retrieve paths (how? they are not in a bucket)
            //cloudSessionManager.getObjectPath(accountId, accountId, objectId)
            Properties storageProperties = Config.getStorageProperties(Config.getGcsaHome());
            if (!storageProperties.containsKey("OPENCGA.LOCAL.FOLDERS." + bucketId)) {
                throw new IllegalStateException("There is no path defined for the requested bucket ID");
            }
            
            String bucketPathName = storageProperties.getProperty("OPENCGA.LOCAL.FOLDERS." + bucketId, "");
            System.out.println("* Bucket path name = " + bucketPathName);
            java.nio.file.Path objectPath = Paths.get(bucketPathName + "/" + objectId);
            System.out.println("* Object path name = " + bucketPathName);
            FileUtils.checkFile(objectPath); // Check file exists and is readable
            
            // Storage recibe los paths (URI) 
            List<String> regions = Splitter.on(',').splitToList(regionStr);
            List<String> results = new ArrayList<>();
            
            for (String region : regions) {
                results.add(cloudSessionManager.fetchData(objectPath, Files.getFileExtension(objectPath.toString()), region, params));
            }
            return createOkResponse(results.toString());
        } catch (Exception e) {
            logger.error(e.toString());
            e.printStackTrace();
            return createErrorResponse(e.getMessage());
        }
    }

}
