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

package org.opencb.opencga.server;

import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by imedina on 02/01/16.
 */
public abstract class AbstractStorageServer {

    protected Path opencgaHome;
    protected int port;

    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;

    protected Logger logger;

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
                logger.info("Loading configuration files from '{}'", opencgaHome);
//                generalConfiguration = GeneralConfiguration.load(GeneralConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml"));
                configuration = Configuration
                        .load(Files.newInputStream(opencgaHome.resolve("conf").resolve("configuration.yml").toFile().toPath()));
                storageConfiguration = StorageConfiguration
                        .load(Files.newInputStream(opencgaHome.resolve("conf").resolve("storage-configuration.yml").toFile().toPath()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public abstract void start() throws Exception;

    public abstract void stop() throws Exception;

    public abstract void blockUntilShutdown() throws Exception;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StorageServer{");
        sb.append("port=").append(port);
        sb.append('}');
        return sb.toString();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
