/**
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
 * WARNING: AUTOGENERATED CODE
 * 
 * This code was generated by a tool.
 * 
 * Manual changes to this file may cause unexpected behavior in your application.
 * Manual changes to this file will be overwritten if the code is regenerated. 
 *
**/

import OpenCGAParentClass from "./../opencga-parent-class.js";


/**
 * This class contains the methods for the "Organization" resource
 */

export default class Organization extends OpenCGAParentClass {

    constructor(config) {
        super(config);
    }

    /** Create a new organization
    * @param {Object} data - JSON containing the organization to be created.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    create(data, params) {
        return this._post("organizations", null, null, null, "create", data, params);
    }

    /** Create a new note
    * @param {Object} data - JSON containing the Note to be added.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    createNotes(data, params) {
        return this._post("organizations", null, "notes", null, "create", data, params);
    }

    /** Search for notes of scope ORGANIZATION
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {String} [params.creationDate] - Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
    * @param {String} [params.modificationDate] - Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
    * @param {String} [params.id] - Note unique identifier.
    * @param {String} [params.type] - Note type.
    * @param {String} [params.scope] - Scope of the Note.
    * @param {String} [params.visibility] - Visibility of the Note.
    * @param {String} [params.uuid] - Unique 32-character identifier assigned automatically by OpenCGA.
    * @param {String} [params.userId] - User that wrote that Note.
    * @param {String} [params.tags] - Note tags.
    * @param {String} [params.version] - Autoincremental version assigned to the registered entry. By default, updates does not create new
    *     versions. To enable versioning, users must set the `incVersion` flag from the /update web service when updating the document.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    searchNotes(params) {
        return this._get("organizations", null, "notes", null, "search", params);
    }

    /** Delete note
    * @param {String} id - Note unique identifier.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    deleteNotes(id, params) {
        return this._delete("organizations", null, "notes", id, "delete", params);
    }

    /** Update a note
    * @param {String} id - Note unique identifier.
    * @param {Object} data - JSON containing the Note fields to be updated.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {"ADD SET REMOVE"} [params.tagsAction = "ADD"] - Action to be performed if the array of tags is being updated. The default
    *     value is ADD.
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    updateNotes(id, data, params) {
        return this._post("organizations", null, "notes", id, "update", data, params);
    }

    /** Update the user status
    * @param {String} user - User ID.
    * @param {Object} data - JSON containing the User fields to be updated.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {String} [params.organization] - Organization id.
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    userUpdateStatus(user, data, params) {
        return this._post("organizations/user", user, "status", null, "update", data, params);
    }

    /** Update the user information
    * @param {String} user - User ID.
    * @param {Object} data - JSON containing the User fields to be updated.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {String} [params.organization] - Organization id.
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    updateUser(user, data, params) {
        return this._post("organizations", null, "user", user, "update", data, params);
    }

    /** Update the Organization configuration attributes
    * @param {String} organization - Organization id.
    * @param {Object} data - JSON containing the params to be updated.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @param {"ADD SET REMOVE REPLACE"} [params.authenticationOriginsAction = "ADD"] - Action to be performed if the array of
    *     authenticationOrigins is being updated. The default value is ADD.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    updateConfiguration(organization, data, params) {
        return this._post("organizations", organization, "configuration", null, "update", data, params);
    }

    /** Return the organization information
    * @param {String} organization - Organization id.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    info(organization, params) {
        return this._get("organizations", organization, null, null, "info", params);
    }

    /** Update some organization attributes
    * @param {String} organization - Organization id.
    * @param {Object} data - JSON containing the params to be updated.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @param {"ADD REMOVE"} [params.adminsAction = "ADD"] - Action to be performed if the array of admins is being updated. The default
    *     value is ADD.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    update(organization, data, params) {
        return this._post("organizations", organization, null, null, "update", data, params);
    }

}