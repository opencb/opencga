/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.client.config;

/**
 * Created by imedina on 04/05/16.
 */
public class RestConfig {

    private String host;
    private int batchQuerySize;
    private int timeout;
    private int defaultLimit;

    public RestConfig() {
    }

    public RestConfig(String host, int batchQuerySize, int timeout, int defaultLimit) {
        this.host = host;
        this.batchQuerySize = batchQuerySize;
        this.timeout = timeout;
        this.defaultLimit = defaultLimit;
    }

    public String getHost() {
        return host;
    }

    public RestConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public int getBatchQuerySize() {
        return batchQuerySize;
    }

    public RestConfig setBatchQuerySize(int batchQuerySize) {
        this.batchQuerySize = batchQuerySize;
        return this;
    }

    public int getTimeout() {
        return timeout;
    }

    public RestConfig setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public int getDefaultLimit() {
        return defaultLimit;
    }

    public RestConfig setDefaultLimit(int defaultLimit) {
        this.defaultLimit = defaultLimit;
        return this;
    }
}
