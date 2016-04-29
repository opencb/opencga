/*
 * Copyright 2015 OpenCB
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

import java.util.Map;

/**
 * Created by imedina on 25/04/16.
 */
public class ServerConfiguration {

    private int rest;
    private int grpc;
    private Map<String, String> options;

    public ServerConfiguration(int rest, int grpc) {
        this.rest = rest;
        this.grpc = grpc;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ServerConfiguration{");
        sb.append("rest=").append(rest);
        sb.append(", grpc=").append(grpc);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public int getRest() {
        return rest;
    }

    public ServerConfiguration setRest(int rest) {
        this.rest = rest;
        return this;
    }

    public int getGrpc() {
        return grpc;
    }

    public ServerConfiguration setGrpc(int grpc) {
        this.grpc = grpc;
        return this;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public ServerConfiguration setOptions(Map<String, String> options) {
        this.options = options;
        return this;
    }
}
