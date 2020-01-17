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

package org.opencb.opencga.client.rest;

import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.GroupCreateParams;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.PermissionRule;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclUpdateParams;
import org.opencb.opencga.core.models.study.StudyCreateParams;
import org.opencb.opencga.core.models.study.StudyUpdateParams;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.models.study.VariableSetCreateParams;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Study webservices.
 *    Client version: 2.0.0
 *    PATH: studies
 */
public class StudyClient extends AbstractParentClient {

    public StudyClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Fetch study information.
     * @param studies Comma separated list of Studies [[user@]project:]study where study and project can be either the ID or UUID up to a
     *     maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Study> info(String studies, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("studies", studies, null, null, "info", params, GET, Study.class);
    }

    /**
     * Update some study attributes.
     * @param study Study [[user@]project:]study where study and project can be either the ID or UUID.
     * @param data JSON containing the params to be updated.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Study> update(String study, StudyUpdateParams data) throws ClientException {
        ObjectMap params = new ObjectMap();
        params.put("body", data);
        return execute("studies", study, null, null, "update", params, POST, Study.class);
    }

    /**
     * Return the acl of the study. If member is provided, it will only return the acl for the member.
     * @param studies Comma separated list of Studies [[user@]project:]study where study and project can be either the ID or UUID up to a
     *     maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> acl(String studies, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("studies", studies, null, null, "acl", params, GET, ObjectMap.class);
    }

    /**
     * Update the set of permissions granted for the member.
     * @param members Comma separated list of user or group ids.
     * @param data JSON containing the parameters to modify ACLs. 'template' could be either 'admin', 'analyst' or 'view_only'.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> updateAcl(String members, StudyAclUpdateParams data) throws ClientException {
        ObjectMap params = new ObjectMap();
        params.put("body", data);
        return execute("studies", members, null, null, "update", params, POST, ObjectMap.class);
    }

    /**
     * Fetch catalog study stats.
     * @param studies Comma separated list of studies [[user@]project:]study up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<FacetField> aggregationStats(String studies, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("studies", studies, null, null, "aggregationStats", params, GET, FacetField.class);
    }

    /**
     * Create a new study.
     * @param data study.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Study> create(StudyCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("studies", null, null, null, "create", params, POST, Study.class);
    }

    /**
     * Return the groups present in the study.
     * @param study Study [[user@]project:]study where study and project can be either the ID or UUID.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Group> groups(String study, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("studies", study, null, null, "groups", params, GET, Group.class);
    }

    /**
     * Add or remove a group.
     * @param study Study [[user@]project:]study where study and project can be either the ID or UUID.
     * @param data JSON containing the parameters.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Group> updateGroups(String study, GroupCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("studies", study, "groups", null, "update", params, POST, Group.class);
    }

    /**
     * Add, set or remove users from an existing group.
     * @param study Study [[user@]project:]study where study and project can be either the ID or UUID.
     * @param group Group name.
     * @param data JSON containing the parameters.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Group> updateUsers(String study, String group, GroupUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("studies", study, "groups", group, "users/update", params, POST, Group.class);
    }

    /**
     * Fetch permission rules.
     * @param study Study [[user@]project:]study where study and project can be either the ID or UUID.
     * @param entity Entity where the permission rules should be applied to.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<PermissionRule> permissionRules(String study, String entity) throws ClientException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull("entity", entity);
        return execute("studies", study, null, null, "permissionRules", params, GET, PermissionRule.class);
    }

    /**
     * Add or remove a permission rule.
     * @param study Study [[user@]project:]study where study and project can be either the ID or UUID.
     * @param entity Entity where the permission rules should be applied to.
     * @param data JSON containing the permission rule to be created or removed.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<PermissionRule> updatePermissionRules(String study, String entity, PermissionRule data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("entity", entity);
        params.put("body", data);
        return execute("studies", study, "permissionRules", null, "update", params, POST, PermissionRule.class);
    }

    /**
     * Fetch variableSets from a study.
     * @param study Study [[user@]project:]study where study and project can be either the ID or UUID.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<VariableSet> variableSets(String study, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("studies", study, null, null, "variableSets", params, GET, VariableSet.class);
    }

    /**
     * Add or remove a variableSet.
     * @param study Study [[user@]project:]study where study and project can be either the ID or UUID.
     * @param data JSON containing the VariableSet to be created or removed.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<VariableSet> updateVariableSets(String study, VariableSetCreateParams data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("studies", study, "variableSets", null, "update", params, POST, VariableSet.class);
    }

    /**
     * Search studies.
     * @param project Project [user@]project where project can be either the ID or the alias.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Study> search(String project, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("project", project);
        return execute("studies", null, null, null, "search", params, GET, Study.class);
    }

    /**
     * Add or remove variables to a VariableSet.
     * @param study Study [[user@]project:]study where study and project can be either the ID or UUID.
     * @param variableSet VariableSet id of the VariableSet to be updated.
     * @param data JSON containing the variable to be added or removed. For removing, only the variable id will be needed.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<VariableSet> updateVariables(String study, String variableSet, Variable data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("studies", study, "variableSets", variableSet, "variables/update", params, POST, VariableSet.class);
    }
}
