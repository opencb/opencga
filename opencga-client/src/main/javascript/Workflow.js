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
 * This class contains the methods for the "Workflow" resource
 */

export default class Workflow extends OpenCGAParentClass {

    constructor(config) {
        super(config);
    }

    /** Update the set of permissions granted for the member
    * @param {String} members - Comma separated list of user or group ids.
    * @param {Object} data - JSON containing the parameters to update the permissions.
    * @param {"SET ADD REMOVE RESET"} action = "ADD" - Action to be performed [ADD, SET, REMOVE or RESET]. The default value is ADD.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    updateAcl(members, action, data, params) {
        return this._post("workflows", null, "acl", members, "update", data, {action, ...params});
    }

    /** Create a workflow
    * @param {Object} data - JSON containing workflow information.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    create(data, params) {
        return this._post("workflows", null, null, null, "create", data, params);
    }

    /** Workflow distinct method
    * @param {String} field - Comma separated list of fields for which to obtain the distinct values.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @param {String} [params.id] - Comma separated list of workflow IDs up to a maximum of 100. Also admits basic regular expressions using
    *     the operator '~', i.e. '~{perl-regex}' e.g. '~value' for case sensitive, '~/value/i' for case insensitive search.
    * @param {String} [params.name] - Comma separated list of workflow names up to a maximum of 100. Also admits basic regular expressions
    *     using the operator '~', i.e. '~{perl-regex}' e.g. '~value' for case sensitive, '~/value/i' for case insensitive search.
    * @param {String} [params.uuid] - Comma separated list of workflow UUIDs up to a maximum of 100.
    * @param {String} [params.tags] - Comma separated list of tags.
    * @param {Boolean} [params.draft] - Boolean field indicating whether the workflow is a draft or not.
    * @param {String} [params.internal.registrationUserId] - UserId that created the workflow.
    * @param {String} [params.manager.id] - Id of the workflow system (Allowed values: NEXTFLOW).
    * @param {String} [params.type] - Workflow type. Allowed types: [CLINICAL_INTERPRETATION, SECONDARY_ANALYSIS, RESEARCH or OTHER].
    * @param {String} [params.creationDate] - Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
    * @param {String} [params.modificationDate] - Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
    * @param {String} [params.acl] - Filter entries for which a user has the provided permissions. Format: acl={user}:{permissions}.
    *     Example: acl=john:WRITE,WRITE_ANNOTATIONS will return all entries for which user john has both WRITE and WRITE_ANNOTATIONS
    *     permissions. Only study owners or administrators can query by this field. .
    * @param {String} [params.release] - Release when it was created.
    * @param {Number} [params.snapshot] - Snapshot value (Latest version of the entry in the specified release).
    * @param {Boolean} [params.deleted = "false"] - Boolean to retrieve deleted entries. The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    distinct(field, params) {
        return this._get("workflows", null, null, null, "distinct", {field, ...params});
    }

    /** Execute a Nextflow analysis.
    * @param {Object} data - Repository parameters.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    importWorkflow(data, params) {
        return this._post("workflows", null, null, null, "import", data, params);
    }

    /** Execute a Nextflow analysis.
    * @param {Object} data - NextFlow run parameters.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @param {String} [params.jobId] - Job ID. It must be a unique string within the study. An ID will be autogenerated automatically if not
    *     provided.
    * @param {String} [params.jobDescription] - Job description.
    * @param {String} [params.jobDependsOn] - Comma separated list of existing job IDs the job will depend on.
    * @param {String} [params.jobTags] - Job tags.
    * @param {String} [params.jobScheduledStartTime] - Time when the job is scheduled to start.
    * @param {String} [params.jobPriority] - Priority of the job.
    * @param {Boolean} [params.jobDryRun] - Flag indicating that the job will be executed in dry-run mode. In this mode, OpenCGA will
    *     validate that all parameters and prerequisites are correctly set for successful execution, but the job will not actually run.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    run(data, params) {
        return this._post("workflows", null, null, null, "run", data, params);
    }

    /** Workflow search method
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {Number} [params.limit] - Number of results to be returned.
    * @param {Number} [params.skip] - Number of results to skip.
    * @param {Boolean} [params.count = "false"] - Get the total number of results matching the query. Deactivated by default. The default
    *     value is false.
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @param {String} [params.id] - Comma separated list of workflow IDs up to a maximum of 100. Also admits basic regular expressions using
    *     the operator '~', i.e. '~{perl-regex}' e.g. '~value' for case sensitive, '~/value/i' for case insensitive search.
    * @param {String} [params.name] - Comma separated list of workflow names up to a maximum of 100. Also admits basic regular expressions
    *     using the operator '~', i.e. '~{perl-regex}' e.g. '~value' for case sensitive, '~/value/i' for case insensitive search.
    * @param {String} [params.uuid] - Comma separated list of workflow UUIDs up to a maximum of 100.
    * @param {String} [params.tags] - Comma separated list of tags.
    * @param {Boolean} [params.draft] - Boolean field indicating whether the workflow is a draft or not.
    * @param {String} [params.internal.registrationUserId] - UserId that created the workflow.
    * @param {String} [params.manager.id] - Id of the workflow system (Allowed values: NEXTFLOW).
    * @param {String} [params.type] - Workflow type. Allowed types: [CLINICAL_INTERPRETATION, SECONDARY_ANALYSIS, RESEARCH or OTHER].
    * @param {String} [params.creationDate] - Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
    * @param {String} [params.modificationDate] - Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805.
    * @param {String} [params.acl] - Filter entries for which a user has the provided permissions. Format: acl={user}:{permissions}.
    *     Example: acl=john:WRITE,WRITE_ANNOTATIONS will return all entries for which user john has both WRITE and WRITE_ANNOTATIONS
    *     permissions. Only study owners or administrators can query by this field. .
    * @param {String} [params.release] - Release when it was created.
    * @param {Number} [params.snapshot] - Snapshot value (Latest version of the entry in the specified release).
    * @param {Boolean} [params.deleted = "false"] - Boolean to retrieve deleted entries. The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    search(params) {
        return this._get("workflows", null, null, null, "search", params);
    }

    /** Update some workflow attributes
    * @param {String} workflowId - Comma separated list workflow IDs or UUIDs up to a maximum of 100.
    * @param {Object} [data] - body.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @param {Boolean} [params.includeResult = "false"] - Flag indicating to include the created or updated document result in the response.
    *     The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    update(workflowId, data, params) {
        return this._post("workflows", workflowId, null, null, "update", data, params);
    }

    /** Returns the acl of the workflows. If member is provided, it will only return the acl for the member.
    * @param {String} workflows - Comma separated list workflow IDs or UUIDs up to a maximum of 100.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @param {String} [params.member] - User or group id.
    * @param {Boolean} [params.silent = "false"] - Boolean to retrieve all possible entries that are queried for, false to raise an
    *     exception whenever one of the entries looked for cannot be shown for whichever reason. The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    acl(workflows, params) {
        return this._get("workflows", workflows, null, null, "acl", params);
    }

    /** Delete workflows
    * @param {String} workflows - Comma separated list workflow IDs or UUIDs up to a maximum of 100.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    delete(workflows, params) {
        return this._delete("workflows", workflows, null, null, "delete", params);
    }

    /** Get workflow information
    * @param {String} workflows - Comma separated list sample IDs or UUIDs up to a maximum of 100.
    * @param {Object} [params] - The Object containing the following optional parameters:
    * @param {String} [params.include] - Fields included in the response, whole JSON path must be provided.
    * @param {String} [params.exclude] - Fields excluded in the response, whole JSON path must be provided.
    * @param {String} [params.study] - Study [[organization@]project:]study where study and project can be either the ID or UUID.
    * @param {String} [params.version] - Comma separated list of workflow versions. 'all' to get all the workflow versions. Not supported if
    *     multiple workflow ids are provided.
    * @param {Boolean} [params.deleted = "false"] - Boolean to retrieve deleted entries. The default value is false.
    * @returns {Promise} Promise object in the form of RestResponse instance.
    */
    info(workflows, params) {
        return this._get("workflows", workflows, null, null, "info", params);
    }

}