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

package org.opencb.opencga.core.models.admin;

public class InstallationParams {

    private String secretKey;
    private String password;
    private String email;

    public InstallationParams() {
    }

    public InstallationParams(String secretKey, String password, String email) {
        this.secretKey = secretKey;
        this.password = password;
        this.email = email;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InstallationParams{");
        sb.append("secretKey='").append(secretKey).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSecretKey() {
        return secretKey;
    }

    public InstallationParams setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public InstallationParams setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public InstallationParams setEmail(String email) {
        this.email = email;
        return this;
    }

}
