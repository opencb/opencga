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
import org.opencb.opencga.catalog.db.api.OrganizationDBAdaptor;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.models.notes.NoteCreateParams;
import org.opencb.opencga.core.models.notes.NoteUpdateParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.response.OpenCGAResult;
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
            @ApiParam(value = ParamConstants.ORGANIZATION_DESCRIPTION, required = true) @PathParam(ParamConstants.ORGANIZATION) String organizationId
    ) {
        try {
            OpenCGAResult<Organization> result = catalogManager.getOrganizationManager().get(organizationId, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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
        try {
            if (adminsAction == null) {
                adminsAction = ParamUtils.AddRemoveAction.ADD;
            }
            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(OrganizationDBAdaptor.QueryParams.ADMINS.key(), adminsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);
            OpenCGAResult<Organization> result = catalogManager.getOrganizationManager().update(organizationId, parameters, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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
        try {
            OpenCGAResult<Organization> result = catalogManager.getOrganizationManager().create(parameters, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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
            @ApiParam(value = FieldConstants.GENERIC_UUID_DESCRIPTION) @QueryParam("uuid") String uuid,
            @ApiParam(value = FieldConstants.NOTES_USER_ID_DESCRIPTION) @QueryParam(FieldConstants.NOTES_USER_ID_PARAM) String userId,
            @ApiParam(value = FieldConstants.NOTES_TAGS_DESCRIPTION) @QueryParam(FieldConstants.NOTES_TAGS_PARAM) String tags,
            @ApiParam(value = FieldConstants.NOTES_VISIBILITY_DESCRIPTION) @QueryParam(FieldConstants.NOTES_VISIBILITY_PARAM) String visibility,
            @ApiParam(value = FieldConstants.GENERIC_VERSION_DESCRIPTION) @QueryParam("version") String version
    ) {
        try {
            OpenCGAResult<Note> result = catalogManager.getNotesManager().search(Note.Scope.ORGANIZATION, query, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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
        try {
            OpenCGAResult<Note> result = catalogManager.getNotesManager().createOrganizationNote(parameters, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
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
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(value = "JSON containing the Note fields to be updated.", required = true) NoteUpdateParams parameters) {
        try {
            OpenCGAResult<Note> result = catalogManager.getNotesManager().update(Note.Scope.ORGANIZATION, noteId, parameters, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/notes/{id}/delete")
    @ApiOperation(value = "Delete note", response = Note.class)
    public Response deleteNote(
            @ApiParam(value = FieldConstants.NOTES_ID_DESCRIPTION) @PathParam(FieldConstants.NOTES_ID_PARAM) String noteId,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult) {
        try {
            OpenCGAResult<Note> result = catalogManager.getNotesManager().deleteOrganizationNote(noteId, queryOptions, token);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}