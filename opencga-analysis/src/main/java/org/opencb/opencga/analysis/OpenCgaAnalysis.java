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

package org.opencb.opencga.analysis;

import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class OpenCgaAnalysis {

    protected CatalogManager catalogManager;
    protected Configuration configuration;
    protected StorageConfiguration storageConfiguration;

    protected String opencgaHome;

    protected Logger logger;

    public OpenCgaAnalysis(String opencgaHome) {
        this.opencgaHome = opencgaHome;

        init();
    }

    void init() {
        logger = LoggerFactory.getLogger(this.getClass().toString());

        try {
            loadConfiguration();
            loadStorageConfiguration();

            catalogManager = new CatalogManager(configuration);
        } catch (IOException | CatalogException e) {
            e.printStackTrace();
        }
    }
    /**
     * This method attempts to load general configuration from OpenCGA installation folder, if not exists then loads JAR configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadConfiguration() throws IOException {
        FileUtils.checkDirectory(Paths.get(this.opencgaHome));

        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(this.opencgaHome).resolve("conf").resolve("configuration.yml");
        if (Files.exists(path)) {
            logger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            this.configuration = Configuration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading configuration from JAR file");
            this.configuration = Configuration
                    .load(Configuration.class.getClassLoader().getResourceAsStream("configuration.yml"));
        }
    }

    /**
     * This method attempts to load storage configuration from OpenCGA installation folder, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     */
    public void loadStorageConfiguration() throws IOException {
        FileUtils.checkDirectory(Paths.get(this.opencgaHome));

        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(this.opencgaHome).resolve("conf").resolve("storage-configuration.yml");
        if (Files.exists(path)) {
            logger.debug("Loading storage configuration from '{}'", path.toAbsolutePath());
            this.storageConfiguration = StorageConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading storage configuration from JAR file");
            this.storageConfiguration = StorageConfiguration
                    .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
        }
    }

}
