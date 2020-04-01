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
 * Created by imedina on 22/05/16.
 */
public class RestServerConfiguration extends AbstractServerConfiguration {

    private int defaultLimit;
    private int maxLimit;


    public RestServerConfiguration() {
    }

    public RestServerConfiguration(int port) {
        this(port, 2000, 5000);
    }

    public RestServerConfiguration(int port, int defaultLimit, int maxLimit) {
        super(port);
        this.defaultLimit = defaultLimit;
        this.maxLimit = maxLimit;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RestServerConfiguration{");
        sb.append("defaultLimit=").append(defaultLimit);
        sb.append(", maxLimit=").append(maxLimit);
        sb.append('}');
        return sb.toString();
    }

    public int getDefaultLimit() {
        return defaultLimit;
    }

    public RestServerConfiguration setDefaultLimit(int defaultLimit) {
        this.defaultLimit = defaultLimit;
        return this;
    }

    public int getMaxLimit() {
        return maxLimit;
    }

    public RestServerConfiguration setMaxLimit(int maxLimit) {
        this.maxLimit = maxLimit;
        return this;
    }
}
