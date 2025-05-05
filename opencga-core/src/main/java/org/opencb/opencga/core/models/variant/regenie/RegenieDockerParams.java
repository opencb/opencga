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

package org.opencb.opencga.core.models.variant.regenie;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class RegenieDockerParams extends ToolParams {

    @DataField(id = "name", description = FieldConstants.REGENIE_WALKER_DOCKER_NAME_DESCRIPTION)
    private String name;

    @DataField(id = "tag", description = FieldConstants.REGENIE_WALKER_DOCKER_TAG_DESCRIPTION)
    private String tag;

    @DataField(id = "username", description = FieldConstants.REGENIE_WALKER_DOCKER_USERNAME_DESCRIPTION)
    private String username;

    @DataField(id = "password", description = FieldConstants.REGENIE_WALKER_DOCKER_PASSWORD_DESCRIPTION)
    private String password;

    public RegenieDockerParams() {
    }

    public RegenieDockerParams(String name, String tag, String username, String password) {
        this.name = name;
        this.tag = tag;
        this.username = username;
        this.password = password;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieDockerParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", tag='").append(tag).append('\'');
        sb.append(", username='").append(username).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public RegenieDockerParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getTag() {
        return tag;
    }

    public RegenieDockerParams setTag(String tag) {
        this.tag = tag;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public RegenieDockerParams setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public RegenieDockerParams setPassword(String password) {
        this.password = password;
        return this;
    }
}
