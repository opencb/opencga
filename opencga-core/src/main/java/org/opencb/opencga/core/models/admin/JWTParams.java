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

public class JWTParams {

    private String secretKey;

    public JWTParams() {
    }

    public JWTParams(String secretKey) {
        this.secretKey = secretKey;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JWTParams{");
        sb.append("secretKey='").append(secretKey).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSecretKey() {
        return secretKey;
    }

    public JWTParams setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }
}
