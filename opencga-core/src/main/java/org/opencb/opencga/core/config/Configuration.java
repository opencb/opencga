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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 25/04/16.
 */
public class Configuration {

    private String logLevel;
    private String logFile;

    ServerConfiguration server;

    protected static Logger logger = LoggerFactory.getLogger(Configuration.class);

    /**
     * This method attempts to find and load the configuration from installation directory,
     * if not exists then loads JAR storage-configuration.yml
     * @throws IOException
     */
    @Deprecated
    public static Configuration load() throws IOException {
        String appHome = System.getProperty("app.home", System.getenv("OPENCGA_HOME"));
        Path path = Paths.get(appHome + "/conf/configuration.yml");
        if (appHome != null && Files.exists(path)) {
            logger.debug("Loading configuration from '{}'", appHome + "/conf/configuration.yml");
            return Configuration.load(new FileInputStream(new File(appHome + "/conf/configuration.yml")));
        } else {
            logger.debug("Loading configuration from '{}'",
                    Configuration.class.getClassLoader().getResourceAsStream("configuration.yml").toString());
            return Configuration.load(Configuration.class.getClassLoader().getResourceAsStream("configuration.yml"));
        }
    }

    public static Configuration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static Configuration load(InputStream configurationInputStream, String format) throws IOException {
        Configuration storageConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                storageConfiguration = objectMapper.readValue(configurationInputStream, Configuration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                storageConfiguration = objectMapper.readValue(configurationInputStream, Configuration.class);
                break;
        }
        return storageConfiguration;
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }



    public String getLogLevel() {
        return logLevel;
    }

    public Configuration setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public String getLogFile() {
        return logFile;
    }

    public Configuration setLogFile(String logFile) {
        this.logFile = logFile;
        return this;
    }

    public ServerConfiguration getServer() {
        return server;
    }

    public Configuration setServer(ServerConfiguration server) {
        this.server = server;
        return this;
    }
}
