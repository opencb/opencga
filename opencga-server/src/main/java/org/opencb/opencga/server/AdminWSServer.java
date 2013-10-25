package org.opencb.opencga.server;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.opencga.account.beans.Bucket;
import org.opencb.opencga.account.beans.Project;
import org.opencb.opencga.account.db.AccountManagementException;
import org.opencb.opencga.account.io.IOManagementException;

@Path("/account/{accountId}/admin")
public class AdminWSServer extends GenericWSServer {
    private String accountId;

    public AdminWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                         @DefaultValue("") @PathParam("accountId") String accountId) throws IOException, AccountManagementException {
        super(uriInfo, httpServletRequest);
        this.accountId = accountId;
    }

    /**
     * Bucket methods
     * ***************************
     */
    @GET
    @Path("/bucket/list")
    public Response getBucketsList() throws JsonProcessingException {
        try {
            String res = cloudSessionManager.getBucketsList(accountId, sessionId);
            return createOkResponse(res);
        } catch (AccountManagementException e) {
            logger.error(e.toString());
            return createErrorResponse("could not get buckets");
        }
    }

    @GET
    @Path("/bucket/{bucketId}/create")
    public Response createBucket(@DefaultValue("") @PathParam("bucketId") String bucketId,
                                 @DefaultValue("") @QueryParam("description") String description) throws JsonProcessingException {
        Bucket bucket = new Bucket(bucketId);
        bucket.setOwnerId(accountId);
        bucket.setDescripcion(description);
        try {
            cloudSessionManager.createBucket(accountId, bucket, sessionId);
            return createOkResponse("OK");
        } catch (AccountManagementException | IOManagementException e ) {
            logger.error(e.toString());
            return createErrorResponse("could not create bucket");
        }
    }

    @GET
    @Path("/bucket/{bucketId}/refresh")
    public Response refreshBucket(@DefaultValue("") @PathParam("bucketId") String bucketId) {
        try {
            cloudSessionManager.refreshBucket(accountId, bucketId, sessionId);
            return createOkResponse("Ok");
        } catch (AccountManagementException | IOException e) {
            logger.error(e.toString());
            return createErrorResponse("could not refresh bucket");
        }
    }

    // TODO
    @GET
    @Path("/bucket/{bucketId}/rename/{newName}")
    public Response renameBucket(@DefaultValue("") @PathParam("bucketId") String bucketId, @DefaultValue("") @PathParam("newName") String newName) {
        try {
            cloudSessionManager.renameBucket(accountId, bucketId, newName, sessionId);
            return createOkResponse("OK");
        } catch (AccountManagementException | IOManagementException e) {
            logger.error(e.toString());
            return createErrorResponse("could not rename bucket");
        }
    }

    // TODO
    // @GET
    // @Path("/{bucketname}/delete")
    // public Response deleteBucket(@DefaultValue("") @PathParam("bucketname")
    // String bucketId) {
    // try {
    // cloudSessionManager.deleteBucket(accountId, bucketId, sessionId);
    // return createOkResponse("OK");
    // } catch (AccountManagementException | IOManagementException e) {
    // logger.error(e.toString());
    // return createErrorResponse("could not delete the bucket");
    // }
    // }

    // TODO
    // @GET
    // @Path("/{bucketname}/share/{accountList}")
    // public Response shareBucket(@DefaultValue("") @PathParam("bucketname")
    // String bucketId,
    // @DefaultValue("") @PathParam("accountList") String accountList) {
    // try {
    // cloudSessionManager.shareBucket(accountId, bucketId,
    // StringUtils.toList(accountList, ","), sessionId);
    // return createOkResponse("OK");
    // } catch (AccountManagementException | IOManagementException e) {
    // logger.error(e.toString());
    // return createErrorResponse("could not share the bucket");
    // }
    // }

    /**
     * Project methods
     * ***************************
     */
    @GET
    @Path("/project/list")
    public Response getProjectsList() {
        try {
            String res = cloudSessionManager.getProjectsList(accountId, sessionId);
            return createOkResponse(res);
        } catch (AccountManagementException e) {
            logger.error(e.toString());
            return createErrorResponse("could not get projects list");
        }
    }

    @GET
    @Path("/project/{projectId}/create")
    public Response createProject(@DefaultValue("") @PathParam("projectId") String projectId,
                                  @DefaultValue("") @QueryParam("description") String description) throws JsonProcessingException {
        Project project = new Project(projectId);
        project.setOwnerId(accountId);
        try {
            cloudSessionManager.createProject(accountId, project, sessionId);
            return createOkResponse("OK");
        } catch (AccountManagementException | IOManagementException e) {
            logger.error(e.toString());
            return createErrorResponse("could not create project");
        }
    }


    /**
     * Profile methods
     * ***************************
     */
    @GET
    @Path("/profile/change_password")
    public Response changePassword(@DefaultValue("") @QueryParam("old_password") String old_password,
                                   @DefaultValue("") @QueryParam("new_password1") String new_password1,
                                   @DefaultValue("") @QueryParam("new_password2") String new_password2) {
        try {
            cloudSessionManager.changePassword(accountId, old_password, new_password1, new_password2, sessionId);
            return createOkResponse("OK");
        } catch (AccountManagementException e) {
            logger.error(e.toString());
            return createErrorResponse("could not change password");
        }
    }

    @GET
    @Path("/profile/reset_password")
    public Response resetPassword(@DefaultValue("") @QueryParam("email") String email) {
        try {
            cloudSessionManager.resetPassword(accountId, email);
            return createOkResponse("OK");
        } catch (AccountManagementException e) {
            logger.error(e.toString());
            return createErrorResponse("could not reset password");
        }
    }

    @GET
    @Path("/profile/change_email")
    public Response changeEmail(@DefaultValue("") @QueryParam("new_email") String new_email) {
        try {
            cloudSessionManager.changeEmail(accountId, new_email, sessionId);
            return createOkResponse("OK");
        } catch (AccountManagementException e) {
            logger.error(e.toString());
            return createErrorResponse("could not change email");
        }
    }
}