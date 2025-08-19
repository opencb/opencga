package org.opencb.opencga.server.rest;

import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.notification.Notification;
import org.opencb.opencga.core.models.notification.NotificationCreateParams;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;

@Path("/{apiVersion}/notifications")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Notifications", description = "Methods for working with 'notifications' endpoint")
public class NotificationWSServer extends OpenCGAWSServer {

    public NotificationWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @POST
    @Path("/create")
    @ApiOperation(value = "Create a new notification", response = Notification.class, notes = "Create a notification.")
    public Response create(
            @ApiParam(value = "JSON containing the notification information", required = true) NotificationCreateParams params) {
        return run(() -> catalogManager.getNotificationManager().create(params, queryOptions, token));
    }

    @POST
    @Path("/{notification}/visit")
    @ApiOperation(value = "Mark a notification as visited", response = Notification.class)
    public Response visit(
            @ApiParam(value = ParamConstants.NOTIFICATION_DESCRIPTION, required = true) @PathParam(ParamConstants.NOTIFICATION) String uuid,
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId) {
        return run(() -> catalogManager.getNotificationManager().visit(organizationId, uuid, token));
    }

    @GET
    @Path("/{notification}/info")
    @ApiOperation(value = "Return the notification information", response = Notification.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
    })
    public Response info(
            @ApiParam(value = ParamConstants.NOTIFICATION_DESCRIPTION, required = true) @PathParam(ParamConstants.NOTIFICATION) String uuid,
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId) {
        return run(() -> catalogManager.getNotificationManager().get(organizationId, uuid, queryOptions, token));
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Notification search method", response = Notification.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType =
                    "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType =
                    "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.NOTIFICATION_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.NOTIFICATION_TYPE_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_TYPE_PARAM) String type,
            @ApiParam(value = ParamConstants.NOTIFICATION_SCOPE_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_SCOPE_PARAM) String scope,
            @ApiParam(value = ParamConstants.NOTIFICATION_FQN_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_FQN_PARAM) String fqn,
            @ApiParam(value = ParamConstants.NOTIFICATION_SENDER_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_SENDER_PARAM) String sender,
            @ApiParam(value = ParamConstants.NOTIFICATION_TARGET_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_TARGET_PARAM) String target,
            @ApiParam(value = ParamConstants.NOTIFICATION_RECEIVER_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_RECEIVER_PARAM) String receiver,
            @ApiParam(value = ParamConstants.NOTIFICATION_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.NOTIFICATION_VISITED_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_VISITED_PARAM) Boolean visited,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate) {
        return run(() -> {
            query.remove(ParamConstants.ORGANIZATION);
            return catalogManager.getNotificationManager().search(organizationId, query, queryOptions, token);
        });
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Fetch catalog notification stats", response = FacetField.class)
    public Response getAggregationStats(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.NOTIFICATION_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.NOTIFICATION_TYPE_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_TYPE_PARAM) String type,
            @ApiParam(value = ParamConstants.NOTIFICATION_SCOPE_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_SCOPE_PARAM) String scope,
            @ApiParam(value = ParamConstants.NOTIFICATION_FQN_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_FQN_PARAM) String fqn,
            @ApiParam(value = ParamConstants.NOTIFICATION_SENDER_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_SENDER_PARAM) String sender,
            @ApiParam(value = ParamConstants.NOTIFICATION_TARGET_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_TARGET_PARAM) String target,
            @ApiParam(value = ParamConstants.NOTIFICATION_RECEIVER_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_RECEIVER_PARAM) String receiver,
            @ApiParam(value = ParamConstants.NOTIFICATION_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERNAL_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.NOTIFICATION_VISITED_DESCRIPTION) @QueryParam(ParamConstants.NOTIFICATION_VISITED_PARAM) Boolean visited,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,

            // Facet field
            @ApiParam(value = ParamConstants.FACET_DESCRIPTION) @QueryParam(ParamConstants.FACET_PARAM) String facet) {
        return run(() -> {
            query.remove(ParamConstants.ORGANIZATION);
            query.remove(ParamConstants.FACET_PARAM);
            return catalogManager.getSampleManager().facet(organizationId, query, facet, token);
        });
    }

}
