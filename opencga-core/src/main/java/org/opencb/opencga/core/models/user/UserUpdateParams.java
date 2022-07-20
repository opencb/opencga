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

package org.opencb.opencga.core.models.user;

import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class UserUpdateParams {

    @DataField(description = ParamConstants.USER_UPDATE_PARAMS_NAME_DESCRIPTION)
    private String name;
    @DataField(description = ParamConstants.USER_UPDATE_PARAMS_EMAIL_DESCRIPTION)
    private String email;
    @DataField(description = ParamConstants.USER_UPDATE_PARAMS_ORGANIZATION_DESCRIPTION)
    private String organization;
    @DataField(description = ParamConstants.GENERIC_ATTRIBUTES_DESCRIPTION)
    private Map<String, Object> attributes;

    public UserUpdateParams() {
    }

    public UserUpdateParams(String name, String email, String organization, Map<String, Object> attributes) {
        this.name = name;
        this.email = email;
        this.organization = organization;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public UserUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public UserUpdateParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public UserUpdateParams setOrganization(String organization) {
        this.organization = organization;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public UserUpdateParams setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
