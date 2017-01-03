/*
 * Copyright 2015-2016 OpenCB
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by imedina on 25/04/16.
 */
@Deprecated
public class GeneralConfiguration {

    private String logLevel;
    private String logFile;

    private RestServerConfiguration rest;
    private GrpcServerConfiguration grpc;

    protected static Logger logger = LoggerFactory.getLogger(GeneralConfiguration.class);

    /**
     * This method loads the configuration from the InputStream.
     * @throws IOException If any IO problem occurs
     */
    public static GeneralConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static GeneralConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        GeneralConfiguration storageGeneralConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                storageGeneralConfiguration = objectMapper.readValue(configurationInputStream, GeneralConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                storageGeneralConfiguration = objectMapper.readValue(configurationInputStream, GeneralConfiguration.class);
                break;
        }
        return storageGeneralConfiguration;
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Configuration{");
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append(", rest=").append(rest);
        sb.append(", grpc=").append(grpc);
        sb.append('}');
        return sb.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public GeneralConfiguration setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public String getLogFile() {
        return logFile;
    }

    public GeneralConfiguration setLogFile(String logFile) {
        this.logFile = logFile;
        return this;
    }

    public RestServerConfiguration getRest() {
        return rest;
    }

    public GeneralConfiguration setRest(RestServerConfiguration rest) {
        this.rest = rest;
        return this;
    }

    public GrpcServerConfiguration getGrpc() {
        return grpc;
    }

    public GeneralConfiguration setGrpc(GrpcServerConfiguration grpc) {
        this.grpc = grpc;
        return this;
    }
}
