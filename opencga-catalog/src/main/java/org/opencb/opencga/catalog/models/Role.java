/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.catalog.models;

import java.util.List;

/**
 * This class models a role and its users in one Study. At this moment one user can only be belong to one role.
 *
 * Created by pfurio on 15/04/16.
 */
@Deprecated
public class Role {

    private String id;

    /**
     * A list of users or group of users belonging to this role. Group ids must be preceded by '@'.
     */
    private List<String> users;
    private StudyPermissions permissions;

    public Role() {
    }

    public Role(String id, List<String> users, StudyPermissions permissions) {
        this.id = id;
        this.users = users;
        this.permissions = permissions;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Role{");
        sb.append("id='").append(id).append('\'');
        sb.append(", users=").append(users);
        sb.append(", permissions=").append(permissions);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Role setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getUsers() {
        return users;
    }

    public Role setUsers(List<String> users) {
        this.users = users;
        return this;
    }

    public StudyPermissions getPermissions() {
        return permissions;
    }

    public Role setPermissions(StudyPermissions permissions) {
        this.permissions = permissions;
        return this;
    }

}
