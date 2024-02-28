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

import com.fasterxml.jackson.annotation.JsonProperty;

public class PasswordChangeParams {

    @JsonProperty(required = true)
    private String organizationId;
    @JsonProperty(required = true)
    private String user;
    @JsonProperty(required = false)
    private String password;
    @JsonProperty(required = false)
    private String newPassword;
    @JsonProperty(required = false)
    private String reset;

    public PasswordChangeParams() {
    }

    @Deprecated
    public PasswordChangeParams(String user, String oldPassword, String newPassword) {
        this.user = user;
        this.password = oldPassword;
        this.newPassword = newPassword;
    }

    public PasswordChangeParams(String organizationId, String user, String password, String newPassword) {
        this.organizationId = organizationId;
        this.user = user;
        this.password = password;
        this.newPassword = newPassword;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PasswordChangeParams{");
        sb.append("organizationId='").append(organizationId).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", newPassword='").append(newPassword).append('\'');
        sb.append(", reset='").append(reset).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public PasswordChangeParams setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
        return this;
    }

    public String getUser() {
        return user;
    }

    public PasswordChangeParams setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public PasswordChangeParams setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getNewPassword() {
        return newPassword;
    }

    public PasswordChangeParams setNewPassword(String newPassword) {
        this.newPassword = newPassword;
        return this;
    }

    public String getReset() {
        return reset;
    }

    public PasswordChangeParams setReset(String reset) {
        this.reset = reset;
        return this;
    }
}
