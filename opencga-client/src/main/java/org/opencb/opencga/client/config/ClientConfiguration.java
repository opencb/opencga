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

package org.opencb.opencga.client.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by imedina on 04/05/16.
 */
public class ClientConfiguration {

    private String version;
    private int sessionDuration;

    private RestConfig rest;
    private GrpcConfig grpc;


    public ClientConfiguration() {
    }

    public ClientConfiguration(RestConfig rest, GrpcConfig grpc) {
        this.rest = rest;
        this.grpc = grpc;
    }

    public static ClientConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static ClientConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        ClientConfiguration clientConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                clientConfiguration = objectMapper.readValue(configurationInputStream, ClientConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                clientConfiguration = objectMapper.readValue(configurationInputStream, ClientConfiguration.class);
                break;
        }

        return clientConfiguration;
    }

    public void serialize(OutputStream configurationOutputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOutputStream, this);
    }

    public int getSessionDuration() {
        return sessionDuration;
    }

    public void setSessionDuration(int sessionDuration) {
        this.sessionDuration = sessionDuration;
    }

    public RestConfig getRest() {
        return rest;
    }

    public ClientConfiguration setRest(RestConfig rest) {
        this.rest = rest;
        return this;
    }

    public GrpcConfig getGrpc() {
        return grpc;
    }

    public ClientConfiguration setGrpc(GrpcConfig grpc) {
        this.grpc = grpc;
        return this;
    }
}
