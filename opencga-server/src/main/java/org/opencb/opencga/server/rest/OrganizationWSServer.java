/*
 * Copyright 2015-2020 OpenCB
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

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.NoteDBAdaptor;
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.managers.EventManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.event.CatalogEvent;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.models.notes.NoteCreateParams;
import org.opencb.opencga.core.models.notes.NoteUpdateParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.OrganizationConfiguration;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.models.user.OrganizationUserUpdateParams;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.models.user.UserStatusUpdateParams;
import org.opencb.opencga.core.tools.annotations.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Path("/{apiVersion}/organizations")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Organizations", description = "Methods for working with 'organizations' endpoint")
public class OrganizationWSServer extends OpenCGAWSServer {

    public OrganizationWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    @GET
    @Path("/{organization}/info")
    @ApiOperation(value = "Return the organization information", response = Organization.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
    })
    public Response getInfo(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @PathParam(ParamConstants.ORGANIZATION) String organizationId) {
        return run(() -> catalogManager.getOrganizationManager().get(organizationId, queryOptions, token));
    }

    @POST
    @Path("/{organization}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some organization attributes", response = Organization.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, dataType = "string", paramType = "query")
    })
    public Response update(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @PathParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "Action to be performed if the array of admins is being updated.", allowableValues = "ADD,REMOVE", defaultValue = "ADD")
            @QueryParam("adminsAction") ParamUtils.AddRemoveAction adminsAction,
            @ApiParam(value = "JSON containing the params to be updated.", required = true) OrganizationUpdateParams parameters) {
        return run(() -> {
            if (adminsAction != null) {
                Map<String, Object> actionMap = new HashMap<>();
                actionMap.put(OrganizationDBAdaptor.QueryParams.ADMINS.key(), adminsAction);
                queryOptions.put(Constants.ACTIONS, actionMap);
            }
            return catalogManager.getOrganizationManager().update(organizationId, parameters, queryOptions, token);
        });
    }

    @POST
    @Path("/{organization}/configuration/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the Organization configuration attributes", response = OrganizationConfiguration.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, dataType = "string", paramType = "query")
    })
    public Response updateConfiguration(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @PathParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "Action to be performed if the array of authenticationOrigins is being updated.",
                    allowableValues = "ADD,REMOVE,SET,REPLACE", defaultValue = "ADD") @QueryParam("authenticationOriginsAction") ParamUtils.UpdateAction authOriginsAction,
            @ApiParam(value = "JSON containing the params to be updated.", required = true) OrganizationConfiguration parameters) {
        return run(() -> {
            if (authOriginsAction != null) {
                Map<String, Object> actionMap = new HashMap<>();
                actionMap.put(OrganizationDBAdaptor.AUTH_ORIGINS_FIELD, authOriginsAction);
                queryOptions.put(Constants.ACTIONS, actionMap);
            }
            return catalogManager.getOrganizationManager().updateConfiguration(organizationId, parameters, queryOptions, token);
        });
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new organization", response = Organization.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, dataType = "string", paramType = "query")
    })
    public Response create(
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing the organization to be created.", required = true) OrganizationCreateParams parameters) {
        return run(() -> catalogManager.getOrganizationManager().create(parameters, queryOptions, token));
    }

    @GET
    @Path("/notes/search")
    @ApiOperation(value = "Search for notes of scope ORGANIZATION", response = Note.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
    })
    public Response noteSearch(
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = FieldConstants.NOTES_ID_DESCRIPTION) @QueryParam(FieldConstants.NOTES_ID_PARAM) String noteId,
            @ApiParam(value = FieldConstants.NOTES_TYPE_DESCRIPTION) @QueryParam(FieldConstants.NOTES_TYPE_PARAM) String type,
            @ApiParam(value = FieldConstants.NOTES_SCOPE_DESCRIPTION) @QueryParam(FieldConstants.NOTES_SCOPE_PARAM) String scope,
            @ApiParam(value = FieldConstants.NOTES_VISIBILITY_DESCRIPTION) @QueryParam(FieldConstants.NOTES_VISIBILITY_PARAM) String visibility,
            @ApiParam(value = FieldConstants.GENERIC_UUID_DESCRIPTION) @QueryParam("uuid") String uuid,
            @ApiParam(value = FieldConstants.NOTES_USER_ID_DESCRIPTION) @QueryParam(FieldConstants.NOTES_USER_ID_PARAM) String userId,
            @ApiParam(value = FieldConstants.NOTES_TAGS_DESCRIPTION) @QueryParam(FieldConstants.NOTES_TAGS_PARAM) String tags,
            @ApiParam(value = FieldConstants.GENERIC_VERSION_DESCRIPTION) @QueryParam("version") String version
    ) {
        return run(() -> catalogManager.getNotesManager().searchOrganizationNote(query, queryOptions, token));
    }

    @POST
    @Path("/notes/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new note", response = Note.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, dataType = "string", paramType = "query")
    })
    public Response createNote(
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing the Note to be added.", required = true) NoteCreateParams parameters) {
        return run(() -> catalogManager.getNotesManager().createOrganizationNote(parameters, queryOptions, token));
    }

    @POST
    @Path("/notes/{id}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update a note", response = Note.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, dataType = "string", paramType = "query")
    })
    public Response updateNote(
            @ApiParam(value = FieldConstants.NOTES_ID_DESCRIPTION) @PathParam(FieldConstants.NOTES_ID_PARAM) String noteId,
            @ApiParam(value = "Action to be performed if the array of tags is being updated.", allowableValues = "ADD,REMOVE,SET", defaultValue = "ADD")
            @QueryParam("tagsAction") ParamUtils.BasicUpdateAction tagsAction,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing the Note fields to be updated.", required = true) NoteUpdateParams parameters) {
        return run(() -> {
            if (tagsAction != null) {
                Map<String, Object> actionMap = new HashMap<>();
                actionMap.put(NoteDBAdaptor.QueryParams.TAGS.key(), tagsAction);
                queryOptions.put(Constants.ACTIONS, actionMap);
            }
            return catalogManager.getNotesManager().updateOrganizationNote(noteId, parameters, queryOptions, token);
        });
    }

    @DELETE
    @Path("/notes/{id}/delete")
    @ApiOperation(value = "Delete note", response = Note.class)
    public Response deleteNote(
            @ApiParam(value = FieldConstants.NOTES_ID_DESCRIPTION) @PathParam(FieldConstants.NOTES_ID_PARAM) String noteId,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult) {
        return run(() -> catalogManager.getNotesManager().deleteOrganizationNote(noteId, queryOptions, token));
    }

    @GET
    @Path("/{organization}/events/query")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Search events", response = CatalogEvent.class)
    public Response searchEvents(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @PathParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = FieldConstants.EVENT_SUCCESSFUL_DESCRIPTION) @QueryParam(FieldConstants.EVENT_SUCCESSFUL_PARAM) Boolean successful) {
        return run(() -> EventManager.getInstance().search(organizationId, query, token));
    }

    @POST
    @Path("/{organization}/events/{eventId}/archive")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Archive an event", response = CatalogEvent.class)
    public Response archiveEvent(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @PathParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = FieldConstants.EVENT_ID_DESCRIPTION) @PathParam(FieldConstants.EVENT_ID_PARAM) String eventId) {
        return run(() -> EventManager.getInstance().archiveEvent(organizationId, eventId, token));
    }

    @POST
    @Path("/{organization}/events/{eventId}/retry")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Retry unsuccessful event", response = CatalogEvent.class)
    public Response retryEvent(
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @PathParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = FieldConstants.EVENT_ID_DESCRIPTION) @PathParam(FieldConstants.EVENT_ID_PARAM) String eventId) {
        return run(() -> EventManager.getInstance().retryEvent(organizationId, eventId, token));
    }

    @POST
    @Path("/user/{user}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the user information", response = User.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, dataType = "string", paramType = "query")
    })
    public Response updateUserInformation(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing the User fields to be updated.", required = true) OrganizationUserUpdateParams parameters) {
        return run(() -> catalogManager.getOrganizationManager().updateUser(organizationId, userId, parameters, queryOptions, token));
    }

    @POST
    @Path("/user/{user}/status/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update the user status", response = User.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, dataType = "string", paramType = "query")
    })
    public Response updateUserStatus(
            @ApiParam(value = ParamConstants.USER_DESCRIPTION, required = true) @PathParam("user") String userId,
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION) @QueryParam(ParamConstants.ORGANIZATION) String organizationId,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing the User fields to be updated.", required = true) UserStatusUpdateParams parameters) {
        return run(() -> catalogManager.getUserManager().changeStatus(organizationId, userId, parameters.getStatus(), queryOptions, token));
    }


}