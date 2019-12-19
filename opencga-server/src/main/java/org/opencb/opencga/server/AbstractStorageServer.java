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

package org.opencb.opencga.server;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 02/01/16.
 */
public abstract class AbstractStorageServer {

    @Deprecated
    protected Path configDir;
    protected Path opencgaHome;
    protected int port;

    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;

    /**
     * This is the default StorageEngine to use when it is not provided by the client.
     */
    @Deprecated
    protected String defaultStorageEngine;

    protected Logger logger;


    @Deprecated
    public AbstractStorageServer() {
        initDefaultConfigurationFiles();
    }

    @Deprecated
    public AbstractStorageServer(int port, String defaultStorageEngine) {
        initDefaultConfigurationFiles();

        this.port = port;
        if (StringUtils.isNotEmpty(defaultStorageEngine)) {
            this.defaultStorageEngine = defaultStorageEngine;
        } else {
            this.defaultStorageEngine = storageConfiguration.getVariant().getDefaultEngine();
        }
    }

    public AbstractStorageServer(Path opencgaHome) {
        this(opencgaHome, 0);
    }

    public AbstractStorageServer(Path opencgaHome, int port) {
        this.opencgaHome = opencgaHome;
        this.port = port;

        init();
    }

    private void init() {
        logger = LoggerFactory.getLogger(this.getClass());

        initConfigurationFiles(opencgaHome);

        if (this.port == 0) {
            this.port = configuration.getServer().getRest().getPort();
        }
    }

    private void initConfigurationFiles(Path opencgaHome) {
        try {
            if (opencgaHome != null && Files.exists(opencgaHome) && Files.isDirectory(opencgaHome)
                    && Files.exists(opencgaHome.resolve("conf"))) {
                logger.info("Loading configuration files from '{}'", opencgaHome.toString());
//                generalConfiguration = GeneralConfiguration.load(GeneralConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml"));
                configuration = Configuration
                        .load(new FileInputStream(new File(opencgaHome.resolve("conf").toFile().getAbsolutePath() + "/configuration.yml")));
                storageConfiguration = StorageConfiguration
                        .load(new FileInputStream(new File(opencgaHome.resolve("conf").toFile().getAbsolutePath() + "/storage-configuration.yml")));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    private void initDefaultConfigurationFiles() {
        try {
            if (System.getenv("OPENCGA_HOME") != null) {
                initConfigurationFiles(Paths.get(System.getenv("OPENCGA_HOME") + "/conf"));
            } else {
                logger.info("Loading configuration files from inside JAR file");
//                generalConfiguration = GeneralConfiguration.load(GeneralConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml"));
                configuration = Configuration
                        .load(Configuration.class.getClassLoader().getResourceAsStream("configuration.yml"));
                storageConfiguration = StorageConfiguration
                        .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public abstract void start() throws Exception;

    public abstract void stop() throws Exception;

    public abstract void blockUntilShutdown() throws Exception;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageServer{");
        sb.append("port=").append(port);
        sb.append(", defaultStorageEngine='").append(defaultStorageEngine).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDefaultStorageEngine() {
        return defaultStorageEngine;
    }

    public void setDefaultStorageEngine(String defaultStorageEngine) {
        this.defaultStorageEngine = defaultStorageEngine;
    }

}
