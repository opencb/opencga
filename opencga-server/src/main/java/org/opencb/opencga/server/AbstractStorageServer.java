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

package org.opencb.opencga.server;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
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

    protected int port;

    protected Path configDir;

    protected Configuration configuration;
    protected CatalogConfiguration catalogConfiguration;
    protected StorageConfiguration storageConfiguration;

    /**
     * This is the default StorageEngine to use when it is not provided by the client.
     */
    protected String defaultStorageEngine;

    protected static Logger logger = LoggerFactory.getLogger("org.opencb.opencga.server.AbstractStorageServer");

//    static {
//        logger = LoggerFactory.getLogger("org.opencb.opencga.server.AbstractStorageServer");
//        logger.info("Static block, loading CatalogConfiguration and StorageConfiguration");
//        try {
//            if (System.getenv("OPENCGA_HOME") != null) {
//                logger.info("Loading catalog and storage configuration from OPENCGA_HOME at '{}'",
//                        System.getenv("OPENCGA_HOME") + "/conf/storage-configuration.yml");
//                catalogConfiguration = CatalogConfiguration
//                        .load(new FileInputStream(new File(System.getenv("OPENCGA_HOME") + "/conf/catalog-configuration.yml")));
//                storageConfiguration = StorageConfiguration
//                        .load(new FileInputStream(new File(System.getenv("OPENCGA_HOME") + "/conf/storage-configuration.yml")));
//            } else {
//                logger.info("Loading catalog and storage configuration from JAR at '{}'",
//                        StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml").toString());
//                catalogConfiguration = CatalogConfiguration
//                        .load(CatalogConfiguration.class.getClassLoader().getResourceAsStream("catalog-configuration.yml"));
//                storageConfiguration = StorageConfiguration
//                        .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public AbstractStorageServer() {
        initDefaultConfigurationFiles();
    }

    public AbstractStorageServer(int port, String defaultStorageEngine) {
        initDefaultConfigurationFiles();

        this.port = port;
        if (StringUtils.isNotEmpty(defaultStorageEngine)) {
            this.defaultStorageEngine = defaultStorageEngine;
        } else {
            this.defaultStorageEngine = storageConfiguration.getDefaultStorageEngineId();
        }
    }

    public AbstractStorageServer(Path configDir) {
        this.configDir = configDir;
        initConfigurationFiles(configDir);

        if (storageConfiguration != null) {
            this.defaultStorageEngine = storageConfiguration.getDefaultStorageEngineId();
        }
    }

    private void initDefaultConfigurationFiles() {
        try {
            if (System.getenv("OPENCGA_HOME") != null) {
                logger.info("Loading catalog and storage configuration from OPENCGA_HOME at '{}'",
                        System.getenv("OPENCGA_HOME") + "/conf/storage-configuration.yml");
                initConfigurationFiles(Paths.get(System.getenv("OPENCGA_HOME") + "/conf"));
//                catalogConfiguration = CatalogConfiguration
//                        .load(new FileInputStream(new File(System.getenv("OPENCGA_HOME") + "/conf/catalog-configuration.yml")));
//                storageConfiguration = StorageConfiguration
//                        .load(new FileInputStream(new File(System.getenv("OPENCGA_HOME") + "/conf/storage-configuration.yml")));
            } else {
                logger.info("Loading catalog and storage configuration from JAR at '{}'",
                        StorageConfiguration.class.getClassLoader().getResourceAsStream("catalog-configuration.yml").toString());
                catalogConfiguration = CatalogConfiguration
                        .load(CatalogConfiguration.class.getClassLoader().getResourceAsStream("catalog-configuration.yml"));
                storageConfiguration = StorageConfiguration
                        .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initConfigurationFiles(Path configDir) {
        try {
            if (configDir != null && Files.exists(configDir) && Files.isDirectory(configDir)) {
                catalogConfiguration = CatalogConfiguration
                        .load(new FileInputStream(new File(configDir.toFile().getAbsolutePath() + "/catalog-configuration.yml")));
                storageConfiguration = StorageConfiguration
                        .load(new FileInputStream(new File(configDir.toFile().getAbsolutePath() + "/storage-configuration.yml")));
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

//    public static StorageConfiguration getStorageConfiguration() {
//        return storageConfiguration;
//    }
//
//    public static void setStorageConfiguration(StorageConfiguration storageConfiguration) {
//        AbstractStorageServer.storageConfiguration = storageConfiguration;
//    }

}
