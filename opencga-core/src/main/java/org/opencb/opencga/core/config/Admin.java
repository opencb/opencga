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

package org.opencb.opencga.core.config;

/**
 * Created by imedina on 18/04/16.
 */
public class Admin {

    private String secretKey;
    private String algorithm;

    public Admin() {
    }

    public String getSecretKey() {
        return secretKey;
    }

    public Admin setSecretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public Admin setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Admin{");
        sb.append(", algorithm='").append(algorithm).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
