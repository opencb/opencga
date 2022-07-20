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

package org.opencb.opencga.core.models.study;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class GroupCreateParams {

    @JsonProperty(required = true)
    @DataField(description = ParamConstants.GROUP_CREATE_PARAMS_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.GROUP_CREATE_PARAMS_USERS_DESCRIPTION)
    private List<String> users;

    public GroupCreateParams() {
    }

    public GroupCreateParams(String id, List<String> users) {
        this.id = id;
        this.users = users;
    }

    public static GroupCreateParams of(Group group) {
        return new GroupCreateParams(group.getId(), group.getUserIds());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", users='").append(users).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public GroupCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getUsers() {
        return users;
    }

    public GroupCreateParams setUsers(List<String> users) {
        this.users = users;
        return this;
    }
}
