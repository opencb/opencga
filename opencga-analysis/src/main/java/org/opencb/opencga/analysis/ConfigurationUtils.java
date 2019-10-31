package org.opencb.opencga.analysis;

import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
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
            return Configuration.load(new FileInputStream(path.toFile()));
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
            return StorageConfiguration.load(new FileInputStream(path.toFile()));
        } else {
            logger.debug("Loading storage configuration from JAR file");
            return StorageConfiguration
                    .load(StorageConfiguration.class.getClassLoader().getResourceAsStream("storage-configuration.yml"));
        }
    }
}
