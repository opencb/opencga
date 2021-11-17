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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by imedina on 04/05/16.
 */
public final class ClientConfiguration {

    private String logLevel;

    private String version;
    private int cliSessionDuration;

    private RestConfig rest;
    private GrpcConfig grpc;

    private VariantClientConfiguration variant;

    private static Logger logger;
    private static final String DEFAULT_CONFIGURATION_FORMAT = "yaml";
    private static ClientConfiguration instance;
    private static Logger privateLogger = LoggerFactory.getLogger(ClientConfiguration.class);

    public static ClientConfiguration getInstance() {
        if (instance == null) {
            instance = new ClientConfiguration();
        }
        return instance;
    }

    private ClientConfiguration() {
        logger = LoggerFactory.getLogger(ClientConfiguration.class);
    }

    public static ClientConfiguration load(Path configurationPath) throws IOException {
        InputStream inputStream = FileUtils.newInputStream(configurationPath);
        return load(inputStream, DEFAULT_CONFIGURATION_FORMAT);
    }

    public static ClientConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, DEFAULT_CONFIGURATION_FORMAT);
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
        overwriteEnvironmentVariables(clientConfiguration);
        return clientConfiguration;
    }

    public void serialize(OutputStream configurationOutputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOutputStream, this);
    }

    private static void overwriteEnvironmentVariables(ClientConfiguration configuration) {
        Map<String, String> envVariables = System.getenv();
        for (String variable : envVariables.keySet()) {
            if (variable.startsWith("OPENCGA_")) {
                logger.debug("Overwriting environment parameter '{}'", variable);
                switch (variable) {
                    case "OPENCGA_CLIENT_REST_HOST":
                        configuration.getRest().setUrl(envVariables.get(variable));
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

    /**
     * This method attempts to first data configuration from CLI parameter, if not present then uses the configuration from installation
     * directory, if not exists then loads JAR client-configuration.yml.
     */
    public static void loadClientConfiguration() {
        // We load configuration file either from app home folder or from the JAR

        try {
            String conf = System.getProperty("app.home", System.getenv("OPENCGA_HOME")) + "/conf";
            Path path = Paths.get(conf).resolve("client-configuration.yml");
            if (Files.exists(path)) {
                privateLogger.debug("Loading configuration from '{}'", path.toAbsolutePath());
                instance = ClientConfiguration.load(new FileInputStream(path.toFile()));
            } else {
                privateLogger.debug("Loading configuration from JAR file");
                instance = ClientConfiguration
                        .load(ClientConfiguration.class.getClassLoader().getResourceAsStream("client-configuration.yml"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientConfiguration{");
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", sessionDuration=").append(cliSessionDuration);
        sb.append(", rest=").append(rest);
        sb.append(", grpc=").append(grpc);
        sb.append(", variant=").append(variant);
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

    @Deprecated
    public String getLogFile() {
        return null;
    }

    @Deprecated
    public ClientConfiguration setLogFile(String none) {
        if (none != null) {
            logger.warn("Deprecated option 'client-configuration.yml#logFile'");
        }
        return this;
    }

    public String getVersion() {
        return version;
    }

    public ClientConfiguration setVersion(String version) {
        this.version = version;
        return this;
    }

    public int getCliSessionDuration() {
        return cliSessionDuration;
    }

    public ClientConfiguration setCliSessionDuration(int cliSessionDuration) {
        this.cliSessionDuration = cliSessionDuration;
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

    public VariantClientConfiguration getVariant() {
        return variant;
    }

    public ClientConfiguration setVariant(VariantClientConfiguration variant) {
        this.variant = variant;
        return this;
    }
}
