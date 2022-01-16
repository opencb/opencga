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

package org.opencb.opencga.client.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Map;

/**
 * Created by imedina on 04/05/16.
 */
public final class ClientConfiguration {

    private String logLevel;
    private RestConfig rest;
    private GrpcConfig grpc;

    private static Logger logger;
    private static final String DEFAULT_CONFIGURATION_FORMAT = "YAML";

    public ClientConfiguration() {
        logger = LoggerFactory.getLogger(ClientConfiguration.class);
    }

    public static ClientConfiguration load(Path clientConfigurationPath) throws IOException {
        logger.debug("Loading client configuration file from '{}'", clientConfigurationPath.toString());
        InputStream inputStream = FileUtils.newInputStream(clientConfigurationPath);
        return load(inputStream, DEFAULT_CONFIGURATION_FORMAT);
    }

    public static ClientConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, DEFAULT_CONFIGURATION_FORMAT);
    }

    public static ClientConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        ClientConfiguration clientConfiguration;
        ObjectMapper objectMapper;
        switch (format.toUpperCase()) {
            case "JSON":
                objectMapper = new ObjectMapper();
                clientConfiguration = objectMapper.readValue(configurationInputStream, ClientConfiguration.class);
                break;
            case "YML":
            case "YAML":
                objectMapper = new ObjectMapper(new YAMLFactory());
                clientConfiguration = objectMapper.readValue(configurationInputStream, ClientConfiguration.class);
                break;
            default:
                logger.warn("Not valid client configuration format '{}'", format);
                logger.warn("Creating an empty client configuration object, information can be set by env variables");
                clientConfiguration = new ClientConfiguration();
                break;
        }

        // Multiple hosts can exist, we must check and set a valid defaultHostIndex
        if (clientConfiguration.getRest().getHosts() != null && clientConfiguration.getRest().getHosts().size() > 0) {
            // If hosts are defined then defaultHostIndex must be a value between 0 and the number of hosts
            int defaultHostIndex = clientConfiguration.getRest().getDefaultHostIndex();
            if (defaultHostIndex < 0 || defaultHostIndex >= clientConfiguration.getRest().getHosts().size()) {
                logger.warn("Setting defaultHostIndex to first host");
                clientConfiguration.getRest().setDefaultHostIndex(0);
            }
        } else {
            // If no hosts exist then defaultHostIndex is set to -1
            logger.warn("No hosts found, setting defaultHostIndex to -1");
            clientConfiguration.getRest().setDefaultHostIndex(-1);
        }

        // Overwrite client configuration file with environment variables
        parseEnvironmentVariables(clientConfiguration);

        return clientConfiguration;
    }

    private static void parseEnvironmentVariables(ClientConfiguration configuration) {
        Map<String, String> envVariables = System.getenv();
        for (String variable : envVariables.keySet()) {
            if (variable.startsWith("OPENCGA_")) {
                logger.debug("Setting environment variable '{}'", variable);
                switch (variable.toUpperCase()) {
                    case "OPENCGA_CLIENT_REST_URL":
                    case "OPENCGA_CLIENT_REST_HOST":
                        if (configuration.getRest().getHosts().size() == 0) {
                            configuration.getRest().getHosts().add(new HostConfig("default", variable));
                        } else {
                            int defaultHostIndex = configuration.getRest().getDefaultHostIndex();
                            configuration.getRest().getHosts().get(defaultHostIndex).setUrl(envVariables.get(variable));
                        }
                        break;
                    case "TLS_ALLOW_INVALID_CERTIFICATES":
                        configuration.getRest()
                                .setTlsAllowInvalidCertificates("TRUE".equalsIgnoreCase(envVariables.get(variable)));
                        break;
                    case "OPENCGA_CLIENT_GRPC_HOST":
                        configuration.getGrpc().setHost(envVariables.get(variable));
                        break;
                    default:
                        break;
                }
            }
        }
    }

    public void serialize(OutputStream configurationOutputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOutputStream, this);
    }

    public HostConfig getCurrentHost() throws ClientException {
        if (rest.getHosts() == null
                || rest.getDefaultHostIndex() < 0 || rest.getDefaultHostIndex() >= rest.getHosts().size()) {
            throw new ClientException("Hosts not found");
        }
        logger.debug("Default host index: {}, value: {}", rest.getDefaultHostIndex(),
                rest.getHosts().get(rest.getDefaultHostIndex()));
        return rest.getHosts().get(rest.getDefaultHostIndex());
    }

    public HostConfig getHostByName(String name) throws ClientException {
        if (rest.getHosts() == null
                || rest.getDefaultHostIndex() < 0 || rest.getDefaultHostIndex() >= rest.getHosts().size()) {
            throw new ClientException("Hosts not found");
        }
        for (HostConfig hostConfig : rest.getHosts()) {
            if (hostConfig.getName().equalsIgnoreCase(name)) {
                return hostConfig;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientConfiguration{");
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", rest=").append(rest);
        sb.append(", grpc=").append(grpc);
        sb.append('}');
        return sb.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public ClientConfiguration setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
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
