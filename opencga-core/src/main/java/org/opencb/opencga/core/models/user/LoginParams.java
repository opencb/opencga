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

public class LoginParams {

    private String organizationId;
    private String user;
    private String password;
    private String refreshToken;

    public LoginParams() {
    }

    public LoginParams(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public LoginParams(String organizationId, String user, String password) {
        this.organizationId = organizationId;
        this.user = user;
        this.password = password;
    }

    public LoginParams(String organizationId, String user, String password, String refreshToken) {
        this.organizationId = organizationId;
        this.user = user;
        this.password = password;
        this.refreshToken = refreshToken;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LoginParams{");
        sb.append("organizationId='").append(organizationId).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", refreshToken='").append(refreshToken).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getOrganizationId() {
        return organizationId;
    }

    public LoginParams setOrganizationId(String organizationId) {
        this.organizationId = organizationId;
        return this;
    }

    public String getUser() {
        return user;
    }

    public LoginParams setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public LoginParams setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public LoginParams setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
        return this;
    }
}
