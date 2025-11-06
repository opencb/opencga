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

package org.opencb.opencga.core.config.client;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by imedina on 04/05/16.
 */
public final class ClientConfiguration {

    private static final String DEFAULT_CONFIGURATION_FORMAT = "YAML";
    private static Logger logger;
    @Deprecated
    private String logLevel;
    private RestConfig rest;
    private GrpcConfig grpc;
    private Map<String, Object> attributes;

    public ClientConfiguration() {
        logger = LoggerFactory.getLogger(ClientConfiguration.class);
    }

    public ClientConfiguration(String opencgaUrl) {
        logger = LoggerFactory.getLogger(ClientConfiguration.class);
        HostConfig hostConfig = new HostConfig("opencga", opencgaUrl);
        this.rest = new RestConfig(Collections.singletonList(hostConfig), false, null);
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
        switch (format.toUpperCase()) {
            case "JSON":
                clientConfiguration = readClientConfiguration(new JsonFactory(), configurationInputStream);
                break;
            case "YML":
            case "YAML":
                clientConfiguration = readClientConfiguration(new YAMLFactory(), configurationInputStream);
                break;
            default:
                logger.warn("Not valid client configuration format '{}'", format);
                logger.warn("Creating an empty client configuration object, information can be set by env variables");
                clientConfiguration = new ClientConfiguration();
                break;
        }

        // Overwrite client configuration file with environment variables
        parseEnvironmentVariables(clientConfiguration);

        return clientConfiguration;
    }

    private static ClientConfiguration readClientConfiguration(JsonFactory jf, InputStream configurationInputStream)
            throws IOException {
        ObjectMapper objectMapper = new ObjectMapper(jf);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.readValue(configurationInputStream, ClientConfiguration.class);
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
        if (rest.getHosts() == null) {
            throw new ClientException("Hosts not found");
        }
        if (rest.getDefaultHostIndex() < 0 && !rest.getDefaultHost().isEmpty()) {
            int index = 0;
            for (HostConfig host : rest.getHosts()) {
                if (host.getName().equalsIgnoreCase(rest.getDefaultHost())) {
                    rest.setDefaultHostIndex(index);
                    logger.debug("Setting default host index to {}", rest.getDefaultHostIndex());
                    return host;
                }
                index++;
            }
            throw new ClientException("Default host '" + rest.getDefaultHost() + "' not found in the list of hosts: "
                    + rest.getHosts().stream().map(HostConfig::getName).collect(Collectors.toList()));
        }
        if (rest.getDefaultHostIndex() < 0 || rest.getDefaultHostIndex() >= rest.getHosts().size()) {
            throw new ClientException("Default host index is invalid: " + rest.getDefaultHostIndex());
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

    public void setDefaultIndexByName(String hostname) throws ClientException {
        if (CollectionUtils.isEmpty(rest.getHosts())) {
            throw new ClientException("Host name not found");
        }
        boolean found = false;
        for (int i = 0; i < rest.getHosts().size(); i++) {
            if (rest.getHosts().get(i).getName().equalsIgnoreCase(hostname)) {
                rest.setDefaultHostIndex(i);
                logger.debug("Setting default host index to {}", i);
                found = true;
            }
        }
        if (!found) {
            // Check if the name is a valid host URL
            if (hostname.startsWith("http://") || hostname.startsWith("https://")) {
                rest.getHosts().add(new HostConfig(hostname, hostname));
                rest.setDefaultHostIndex(rest.getHosts().size() - 1);
            } else {
                throw new ClientException("Invalid host name '" + hostname
                        + "' not found in the list of hosts: " + getRest().getHosts()
                        .stream()
                        .map(HostConfig::getName)
                        .collect(Collectors.toList()));
            }
        }
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

    @Deprecated
    public String getLogLevel() {
        return logLevel;
    }

    @Deprecated
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ClientConfiguration setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
