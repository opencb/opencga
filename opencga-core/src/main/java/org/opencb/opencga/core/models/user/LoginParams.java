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

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class LoginParams {

    @DataField(description = ParamConstants.LOGIN_PARAMS_USER_DESCRIPTION)
    private String user;
    @DataField(description = ParamConstants.LOGIN_PARAMS_PASSWORD_DESCRIPTION)
    private String password;
    @DataField(description = ParamConstants.LOGIN_PARAMS_REFRESH_TOKEN_DESCRIPTION)
    private String refreshToken;

    public LoginParams() {
    }

    public LoginParams(String user, String password) {
        this.user = user;
        this.password = password;
    }

    public LoginParams(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public LoginParams(String user, String password, String refreshToken) {
        this.user = user;
        this.password = password;
        this.refreshToken = refreshToken;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LoginParams{");
        sb.append("user='").append(user).append('\'');
        sb.append(", password='").append("********").append('\'');
        sb.append(", refreshToken='").append("********").append('\'');
        sb.append('}');
        return sb.toString();
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
