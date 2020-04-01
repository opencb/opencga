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

package org.opencb.opencga.analysis;

import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ConfigurationUtils {
    private static Logger logger = LoggerFactory.getLogger(ConfigurationUtils.class);

    /**
     * This method attempts to load general configuration from OpenCGA installation folder, if not exists then loads JAR configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     * @param opencgaHome OpenCGA Home
     * @return
     */
    public static Configuration loadConfiguration(String opencgaHome) throws IOException {
        FileUtils.checkDirectory(Paths.get(opencgaHome));

        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(opencgaHome).resolve("conf").resolve("configuration.yml");
        if (Files.exists(path)) {
            logger.debug("Loading configuration from '{}'", path.toAbsolutePath());
            try (InputStream inputStream = FileUtils.newInputStream(path)) {
                return Configuration.load(inputStream);
            } catch (IOException e) {
                throw new IOException("Error loading configuration file " + path.toAbsolutePath(), e);
            }
        } else {
            logger.debug("Loading configuration from JAR file");
            return Configuration
                    .load(Configuration.class.getClassLoader().getResourceAsStream("configuration.yml"));
        }
    }

    /**
     * This method attempts to load storage configuration from OpenCGA installation folder, if not exists then loads JAR storage-configuration.yml.
     *
     * @throws IOException If any IO problem occurs
     * @param opencgaHome OpenCGA Home
     * @return
     */
    public static StorageConfiguration loadStorageConfiguration(String opencgaHome) throws IOException {
        FileUtils.checkDirectory(Paths.get(opencgaHome));

        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(opencgaHome).resolve("conf").resolve("storage-configuration.yml");
        if (Files.exists(path)) {
            logger.debug("Loading storage configuration from '{}'", path.toAbsolutePath());
            try (FileInputStream is = new FileInputStream(path.toFile())) {
                return StorageConfiguration.load(is);
            } catch (IOException e) {
                throw new IOException("Error loading storage configuration file " + path.toAbsolutePath());
            }
        } else {
            logger.debug("Loading storage configuration from JAR file");
            return StorageConfiguration
                    .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
        }
    }
}
