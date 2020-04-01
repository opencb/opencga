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
    private String password;
    @Deprecated
    private String npassword;
    @JsonProperty(required = true)
    private String newPassword;

    public PasswordChangeParams() {
    }

    public PasswordChangeParams(String oldPassword, String newPassword) {
        this.password = oldPassword;
        this.newPassword = newPassword;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PasswordChangeParams{");
        sb.append("password='").append(password).append('\'');
        sb.append(", npassword='").append(npassword).append('\'');
        sb.append(", newPassword='").append(newPassword).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getPassword() {
        return password;
    }

    public PasswordChangeParams setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getNpassword() {
        return npassword;
    }

    public PasswordChangeParams setNpassword(String npassword) {
        this.npassword = npassword;
        return this;
    }

    public String getNewPassword() {
        return newPassword;
    }

    public PasswordChangeParams setNewPassword(String newPassword) {
        this.newPassword = newPassword;
        return this;
    }
}
