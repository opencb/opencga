/*
* Copyright 2015-2024 OpenCB
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

package org.opencb.opencga.client.rest.clients;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.*;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.models.notes.NoteCreateParams;
import org.opencb.opencga.core.models.notes.NoteUpdateParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.organizations.OrganizationCreateParams;
import org.opencb.opencga.core.models.organizations.OrganizationUpdateParams;
import org.opencb.opencga.core.response.RestResponse;


/*
* WARNING: AUTOGENERATED CODE
*
* This code was generated by a tool.
* Autogenerated on: 2024-04-29
*
* Manual changes to this file may cause unexpected behavior in your application.
* Manual changes to this file will be overwritten if the code is regenerated.
*/


/**
 * This class contains methods for the Organization webservices.
 *    Client version: 3.1.0-SNAPSHOT
 *    PATH: organizations
 */
public class OrganizationClient extends AbstractParentClient {

    public OrganizationClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Create a new organization.
     * @param data JSON containing the organization to be created.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       includeResult: Flag indicating to include the created or updated document result in the response.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Organization> create(OrganizationCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("organizations", null, null, null, "create", params, POST, Organization.class);
    }

    /**
     * Create a new note.
     * @param data JSON containing the Note to be added.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       includeResult: Flag indicating to include the created or updated document result in the response.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Note> createNotes(NoteCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("organizations", null, "notes", null, "create", params, POST, Note.class);
    }

    /**
     * Search for notes of scope ORGANIZATION.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       creationDate: Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
     *       modificationDate: Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
     *       id: Note unique identifier.
     *       scope: Scope of the Note.
     *       visibility: Visibility of the Note.
     *       uuid: Unique 32-character identifier assigned automatically by OpenCGA.
     *       userId: User that wrote that Note.
     *       tags: Note tags.
     *       version: Autoincremental version assigned to the registered entry. By default, updates does not create new versions. To enable
     *            versioning, users must set the `incVersion` flag from the /update web service when updating the document.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Note> searchNotes(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("organizations", null, "notes", null, "search", params, GET, Note.class);
    }

    /**
     * Delete note.
     * @param id Note unique identifier.
     * @param params Map containing any of the following optional parameters.
     *       includeResult: Flag indicating to include the created or updated document result in the response.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Note> deleteNotes(String id, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("organizations", null, "notes", id, "delete", params, DELETE, Note.class);
    }

    /**
     * Update a note.
     * @param id Note unique identifier.
     * @param data JSON containing the Note fields to be updated.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       includeResult: Flag indicating to include the created or updated document result in the response.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Note> updateNotes(String id, NoteUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("organizations", null, "notes", id, "update", params, POST, Note.class);
    }

    /**
     * Return the organization information.
     * @param organization Organization id.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Organization> info(String organization, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("organizations", organization, null, null, "info", params, GET, Organization.class);
    }

    /**
     * Update some organization attributes.
     * @param organization Organization id.
     * @param data JSON containing the params to be updated.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       includeResult: Flag indicating to include the created or updated document result in the response.
     *       adminsAction: Action to be performed if the array of admins is being updated.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Organization> update(String organization, OrganizationUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("organizations", organization, null, null, "update", params, POST, Organization.class);
    }
}