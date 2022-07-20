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

import java.util.List;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class GroupUpdateParams {

    @DataField(description = ParamConstants.GROUP_UPDATE_PARAMS_USERS_DESCRIPTION)
    private List<String> users;

    public GroupUpdateParams() {
    }

    public GroupUpdateParams(List<String> users) {
        this.users = users;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupUpdateParams{");
        sb.append("users='").append(users).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public List<String> getUsers() {
        return users;
    }

    public GroupUpdateParams setUsers(List<String> users) {
        this.users = users;
        return this;
    }
}
