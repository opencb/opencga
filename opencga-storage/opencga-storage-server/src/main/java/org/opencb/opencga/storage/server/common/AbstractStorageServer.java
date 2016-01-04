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

package org.opencb.opencga.storage.server.common;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by imedina on 02/01/16.
 */
public abstract class AbstractStorageServer {

    protected int port;

    /**
     * This is the default StorageEngine to use when it is not provided by the client.
     */
    protected String defaultStorageEngine;
    protected static StorageConfiguration storageConfiguration;

    protected static Logger logger;

    static {
        logger = LoggerFactory.getLogger("org.opencb.opencga.storage.server.common.StorageServer");
        logger.info("Static block, loading StorageConfiguration");
        try {
            if (System.getenv("OPENCGA_HOME") != null) {
                logger.info("Loading storage configuration from OPENCGA_HOME at '{}'",
                        System.getenv("OPENCGA_HOME") + "/conf/storage-configuration.yml");
                storageConfiguration = StorageConfiguration
                        .load(new FileInputStream(new File(System.getenv("OPENCGA_HOME") + "/conf/storage-configuration.yml")));
            } else {
                logger.info("Loading storage configuration from JAR at '{}'",
                        StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml").toString());
                storageConfiguration = StorageConfiguration
                        .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public AbstractStorageServer() {
        this(storageConfiguration.getServer().getGrpc(), storageConfiguration.getDefaultStorageEngineId());
    }

    public AbstractStorageServer(int port, String defaultStorageEngine) {
        this.port = port;
        if (StringUtils.isNotEmpty(defaultStorageEngine)) {
            this.defaultStorageEngine = defaultStorageEngine;
        } else {
            this.defaultStorageEngine = storageConfiguration.getDefaultStorageEngineId();
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

    public static StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public static void setStorageConfiguration(StorageConfiguration storageConfiguration) {
        AbstractStorageServer.storageConfiguration = storageConfiguration;
    }

}
