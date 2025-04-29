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

    @DataField(id = "organisation", description = FieldConstants.REGENIE_WALKER_DOCKER_ORGANISATION_DESCRIPTION)
    private String organisation;

    @DataField(id = "username", description = FieldConstants.REGENIE_WALKER_DOCKER_USERNAME_DESCRIPTION)
    private String username;

    @DataField(id = "token", description = FieldConstants.REGENIE_WALKER_DOCKER_TOKEN_DESCRIPTION)
    private String token;

    public RegenieDockerParams() {
    }

    public RegenieDockerParams(String organisation, String username, String token) {
        this.organisation = organisation;
        this.username = username;
        this.token = token;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RegenieDockerParams{");
        sb.append("organisation='").append(organisation).append('\'');
        sb.append(", username='").append(username).append('\'');
        sb.append(", token='").append(token).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getOrganisation() {
        return organisation;
    }

    public RegenieDockerParams setOrganisation(String organisation) {
        this.organisation = organisation;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public RegenieDockerParams setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getToken() {
        return token;
    }

    public RegenieDockerParams setToken(String token) {
        this.token = token;
        return this;
    }
}
